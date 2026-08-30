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

import hashlib
import logging
import uuid
from datetime import date, datetime, timedelta
from typing import List, Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from rosteriq.database import get_db
from rosteriq.services.clock import venue_today
from rosteriq.middleware.tenant import enforce_venue_access, enforce_venue_manager
from rosteriq.routes.menu_costing import GST_RATE, _cost_recipe, _qty_in_unit
from rosteriq.services.events import audit

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/sales", tags=["dish-sales"])


class SaleLine(BaseModel):
    recipe_id: str
    qty: float = Field(..., gt=0, le=10000)


class RecordSalesBody(BaseModel):
    venue_id: str
    sale_date: Optional[date] = None
    items: List[SaleLine] = Field(..., min_length=1)


class ImportRow(BaseModel):
    item_name: str = Field(..., min_length=1, max_length=200)
    qty: float = Field(..., gt=0, le=100000)


class ImportSalesBody(BaseModel):
    venue_id: str
    sale_date: Optional[date] = None
    rows: List[ImportRow] = Field(..., min_length=1, max_length=2000)


class MappingBody(BaseModel):
    venue_id: str
    item_name: str = Field(..., min_length=1, max_length=200)
    recipe_id: str = Field(default="", description="Empty string removes the mapping")


def _normalize(name: str) -> str:
    return " ".join(name.strip().lower().split())


def _record_lines(db, venue_id: str, sale_date: date, lines: list, source: str) -> dict:
    """Shared recording core: lines are (recipe_dict, qty) pairs. Persists
    dish_sales rows, depletes stock atomically, returns the money summary."""
    ingredients = {i["id"]: i for i in (db.list_ingredients(venue_id) or [])}
    recorded = []
    total_revenue = 0.0
    total_cogs = 0.0
    depleted: dict = {}

    # TWO PASSES. Everything that can fail — costing, unit conversion — happens
    # BEFORE the first row is written. Each save_dish_sale is its own committed
    # statement, so a mid-loop error used to leave rows 1..n persisted with no
    # batch fingerprint recorded; the user re-uploaded the file and those rows
    # were counted a second time, inflating revenue and COGS for the day.
    priced = []
    for recipe, qty in lines:
        costing = _cost_recipe(recipe, ingredients)
        revenue = round(float(costing["sell_price_inc_gst"]) * qty, 2)
        cogs = round(float(costing["cost_per_portion"]) * qty, 2)
        yield_portions = float(recipe.get("yield_portions", 1)) or 1

        # Deplete each recipe ingredient, converted to its stock unit
        for item in recipe.get("items", []):
            ing = ingredients.get(item.get("ingredient_id"))
            if not ing:
                continue  # costing already reported it as missing
            per_portion = _qty_in_unit(
                float(item.get("qty", 0)), item.get("unit"), ing.get("unit", "each"),
            ) / yield_portions
            depleted[ing["id"]] = depleted.get(ing["id"], 0.0) + per_portion * qty

        priced.append((recipe, qty, revenue, cogs))

    for recipe, qty, revenue, cogs in priced:
        db.save_dish_sale({
            "id": f"ds-{uuid.uuid4().hex[:12]}",
            "venue_id": venue_id,
            "sale_date": sale_date,
            "recipe_id": recipe["id"],
            "recipe_name": recipe.get("name"),
            "qty": qty,
            "revenue_inc_gst": revenue,
            "cogs": cogs,
            "source": source,
            "created_at": datetime.utcnow(),
        })
        recorded.append({"recipe": recipe.get("name"), "qty": qty,
                         "revenue_inc_gst": revenue, "cogs": cogs})
        total_revenue += revenue
        total_cogs += cogs

    negative = []
    for ing_id, qty in depleted.items():
        db.increment_ingredient_stock(ing_id, -qty)
        after = db.get_ingredient(ing_id)
        if after is not None and float(after.get("stock_qty") or 0) < 0:
            negative.append(after.get("name"))

    logger.info(f"Sales recorded at {venue_id} ({source}): {len(recorded)} lines, "
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


@router.post("/record")
async def record_sales(body: RecordSalesBody) -> dict:
    """Record what sold and deplete ingredient stock through the recipes."""
    enforce_venue_access(body.venue_id)
    db = get_db()
    # Venue-local: the host is on UTC, so an Australian morning is still
    # "yesterday" there — today's sales were stamped a day early, and picking
    # today in the date picker was rejected as a future date.
    _today = venue_today(body.venue_id, db)
    sale_date = body.sale_date or _today
    if sale_date > _today:
        raise HTTPException(status_code=422, detail="Can't record sales for a future date")

    recipes = {r["id"]: r for r in (db.list_recipes(body.venue_id) or [])}
    unknown = [l.recipe_id for l in body.items if l.recipe_id not in recipes]
    if unknown:
        raise HTTPException(status_code=422,
                            detail=f"Unknown recipes for this venue: {', '.join(unknown)}")

    result = _record_lines(db, body.venue_id, sale_date,
                           [(recipes[l.recipe_id], l.qty) for l in body.items], "manual")
    audit("sales.record", body.venue_id, "dish_sales", sale_date.isoformat(),
          sale_date=sale_date.isoformat(), lines=len(result["lines"]),
          total_revenue_inc_gst=result["total_revenue_inc_gst"],
          total_cogs=result["total_cogs"], source="manual")
    return result


@router.get("/mapping")
async def list_mappings(venue_id: str = Query(...)) -> dict:
    """POS item name -> recipe mappings for this venue."""
    enforce_venue_access(venue_id)
    db = get_db()
    recipes = {r["id"]: r.get("name") for r in (db.list_recipes(venue_id) or [])}
    rows = db.list_pos_item_maps(venue_id) or []
    return {"venue_id": venue_id, "count": len(rows), "mappings": [{
        "item_name": m.get("display_name") or m["normalized_name"],
        "normalized_name": m["normalized_name"],
        "recipe_id": m["recipe_id"],
        "recipe_name": recipes.get(m["recipe_id"], m["recipe_id"]),
    } for m in rows]}


@router.post("/mapping")
async def set_mapping(body: MappingBody) -> dict:
    """Map a POS item name to a recipe (empty recipe_id removes the mapping)."""
    enforce_venue_manager(body.venue_id)
    db = get_db()
    norm = _normalize(body.item_name)
    if not body.recipe_id:
        db.delete_pos_item_map(body.venue_id, norm)
        return {"status": "removed", "item_name": body.item_name}
    recipe = db.get_recipe(body.recipe_id)
    if not recipe or recipe.get("venue_id") != body.venue_id:
        raise HTTPException(status_code=404, detail="Recipe not found at this venue")
    db.save_pos_item_map({
        "venue_id": body.venue_id,
        "normalized_name": norm,
        "display_name": body.item_name.strip(),
        "recipe_id": body.recipe_id,
        "created_at": datetime.utcnow(),
    })
    return {"status": "mapped", "item_name": body.item_name,
            "recipe_name": recipe.get("name")}


@router.post("/import")
async def import_sales(body: ImportSalesBody) -> dict:
    """Import item-level sales from any POS product-mix export.

    - Names map to recipes via saved mappings; exact recipe-name matches
      auto-map (and the mapping is saved for next time).
    - Unmapped rows come back untouched with counts — map them and re-import
      the remainder. Nothing is silently dropped.
    - The same batch (same venue+date+rows) is refused on re-import so a
      double upload can never deplete stock twice.
    """
    enforce_venue_manager(body.venue_id)
    db = get_db()
    # Venue-local: the host is on UTC, so an Australian morning is still
    # "yesterday" there — today's sales were stamped a day early, and picking
    # today in the date picker was rejected as a future date.
    _today = venue_today(body.venue_id, db)
    sale_date = body.sale_date or _today
    if sale_date > _today:
        raise HTTPException(status_code=422, detail="Can't import sales for a future date")

    # Batch fingerprint: identical re-imports are refused, not double-counted
    fingerprint = hashlib.sha256(
        (body.venue_id + "|" + sale_date.isoformat() + "|" +
         "|".join(f"{_normalize(r.item_name)}={r.qty}"
                  for r in sorted(body.rows, key=lambda r: _normalize(r.item_name)))
         ).encode()).hexdigest()
    batch_id = f"ib-{fingerprint[:20]}"
    if db.get_import_batch(batch_id):
        raise HTTPException(
            status_code=409,
            detail="This exact batch was already imported for that date — "
                   "a re-upload would count every sale twice.")

    recipes = list(db.list_recipes(body.venue_id) or [])
    by_id = {r["id"]: r for r in recipes}
    by_name = {_normalize(r.get("name", "")): r for r in recipes}
    mappings = {m["normalized_name"]: m["recipe_id"]
                for m in (db.list_pos_item_maps(body.venue_id) or [])}

    lines = []       # (recipe, qty)
    unmapped = []
    auto_mapped = []
    # Aggregate duplicate item names in one upload before recording
    agg: dict = {}
    for row in body.rows:
        norm = _normalize(row.item_name)
        agg.setdefault(norm, {"display": row.item_name.strip(), "qty": 0.0})
        agg[norm]["qty"] += row.qty

    for norm, info in agg.items():
        recipe = None
        if norm in mappings and mappings[norm] in by_id:
            recipe = by_id[mappings[norm]]
        elif norm in by_name:
            recipe = by_name[norm]
            db.save_pos_item_map({
                "venue_id": body.venue_id, "normalized_name": norm,
                "display_name": info["display"], "recipe_id": recipe["id"],
                "created_at": datetime.utcnow(),
            })
            auto_mapped.append(info["display"])
        if recipe is None:
            unmapped.append({"item_name": info["display"], "qty": info["qty"]})
        else:
            lines.append((recipe, info["qty"]))

    if not lines:
        return {
            "status": "nothing_imported",
            "sale_date": sale_date.isoformat(),
            "recorded_lines": 0,
            "unmapped": unmapped,
            "message": "No rows matched a recipe — map the items below and re-import.",
        }

    # CLAIM the fingerprint before recording anything. Written afterwards, a
    # failure part-way through left rows persisted with no batch row, so the
    # obvious re-upload double-counted them — and two workers handling the same
    # upload both passed the get_import_batch check above.
    claimed = db.claim_import_batch({
        "id": batch_id, "venue_id": body.venue_id, "sale_date": sale_date,
        "row_count": len(lines), "revenue": 0.0,
        "imported_at": datetime.utcnow(),
    })
    if not claimed:
        raise HTTPException(
            status_code=409,
            detail="This exact batch was already imported for that date — "
                   "a re-upload would count every sale twice.")

    try:
        result = _record_lines(db, body.venue_id, sale_date, lines, "pos_import")
    except Exception:
        # Nothing was written (the pricing pass runs before any save), so
        # release the claim and let the caller fix the data and retry.
        try:
            db.delete_import_batch(batch_id)
        except Exception as e:  # noqa: BLE001
            logger.warning(f"Could not release import claim {batch_id}: {e}")
        raise

    db.save_import_batch({
        "id": batch_id, "venue_id": body.venue_id, "sale_date": sale_date,
        "row_count": len(lines), "revenue": result["total_revenue_inc_gst"],
        "imported_at": datetime.utcnow(),
    })
    result.update({
        "status": "imported",
        "recorded_lines": len(lines),
        "auto_mapped": auto_mapped,
        "unmapped": unmapped,
    })
    audit("sales.import", body.venue_id, "import_batch", batch_id,
          sale_date=sale_date.isoformat(), rows=len(body.rows), recorded_lines=len(lines),
          unmapped=len(unmapped), auto_mapped=len(auto_mapped),
          total_revenue_inc_gst=result["total_revenue_inc_gst"],
          total_cogs=result["total_cogs"], source="pos_import")
    return result


@router.get("/summary")
async def sales_summary(venue_id: str = Query(...),
                        start_date: Optional[date] = Query(None),
                        end_date: Optional[date] = Query(None)) -> dict:
    """Revenue / COGS / gross profit / food-cost % for a period (default 7 days)."""
    enforce_venue_access(venue_id)
    db = get_db()
    end = end_date or venue_today(venue_id, db)
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
