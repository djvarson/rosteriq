"""
Food & recipe costing — the margin engine (Restoke's crown jewel, native).

Ingredients carry a purchase price ("5 kg for $42.50" -> $8.50/kg); recipes
reference ingredient quantities; every read computes live per-portion cost,
GST-aware margin and food-cost %, so a supplier price change ripples through
the whole menu instantly.

AU specifics: sell prices are entered inc-GST (10%); margins are computed on
the ex-GST amount, because that's the money the venue actually keeps.
Food-cost % above the configurable threshold (default 35%) is flagged.

Routes (venue-scoped):
    GET/POST /api/menu/ingredients      -- list / upsert ingredients
    GET/POST /api/menu/recipes          -- list / upsert recipes (costed on read)
    GET      /api/menu/costing          -- whole-menu margin table + flags
    POST     /api/menu/seed             -- demo/starter data (idempotent)
"""

import logging
import uuid
from datetime import datetime
from typing import List, Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from rosteriq.database import get_db
from rosteriq.middleware.tenant import enforce_venue_access
from rosteriq.services.events import audit

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/menu", tags=["menu-costing"])

GST_RATE = 0.10
DEFAULT_FOOD_COST_TARGET_PCT = 35.0

# Base-unit conversions: quantities may be given in a smaller unit than the
# ingredient's costing unit (recipe uses 180 g of a $/kg ingredient).
_CONVERT = {
    ("g", "kg"): 0.001, ("kg", "g"): 1000.0,
    ("ml", "l"): 0.001, ("l", "ml"): 1000.0,
}
_VALID_UNITS = {"g", "kg", "ml", "l", "each"}


class IngredientRequest(BaseModel):
    venue_id: str
    id: Optional[str] = None
    name: str = Field(..., min_length=1, max_length=120)
    unit: str = Field(default="each", description="Costing unit: g|kg|ml|l|each")
    purchase_size: float = Field(..., gt=0, description="How much one purchase buys, in `unit`")
    purchase_cost: float = Field(..., ge=0, description="What that purchase costs ($)")
    supplier: str = Field(default="", max_length=120)
    active: bool = True

    def model_post_init(self, __context) -> None:
        u = self.unit.strip().lower()
        if u not in _VALID_UNITS:
            raise ValueError(f"unit must be one of {sorted(_VALID_UNITS)}")
        object.__setattr__(self, "unit", u)


class RecipeItem(BaseModel):
    ingredient_id: str
    qty: float = Field(..., gt=0)
    unit: Optional[str] = Field(default=None, description="Defaults to the ingredient's unit")


class RecipeRequest(BaseModel):
    venue_id: str
    id: Optional[str] = None
    name: str = Field(..., min_length=1, max_length=120)
    category: str = Field(default="", max_length=60)
    sell_price_inc_gst: float = Field(..., gt=0)
    yield_portions: float = Field(default=1, gt=0)
    items: List[RecipeItem] = Field(..., min_length=1, max_length=60)
    active: bool = True


class SeedRequest(BaseModel):
    venue_id: str


def _qty_in_unit(qty: float, from_unit: Optional[str], to_unit: str) -> float:
    """Convert a recipe quantity into the ingredient's costing unit."""
    f = (from_unit or to_unit).strip().lower()
    t = to_unit.strip().lower()
    if f == t:
        return qty
    factor = _CONVERT.get((f, t))
    if factor is None:
        raise HTTPException(
            status_code=422,
            detail=f"Can't convert {f} to {t} — use the same unit family (g/kg, ml/l) or the ingredient's unit",
        )
    return qty * factor


def _cost_recipe(recipe: dict, ingredients_by_id: dict) -> dict:
    """Live costing for one recipe. Missing ingredients are reported, not hidden."""
    total_cost = 0.0
    lines = []
    missing = []
    for item in recipe.get("items", []):
        ing = ingredients_by_id.get(item.get("ingredient_id"))
        if not ing:
            missing.append(item.get("ingredient_id"))
            continue
        qty = _qty_in_unit(float(item.get("qty", 0)), item.get("unit"), ing.get("unit", "each"))
        line_cost = qty * float(ing.get("cost_per_unit", 0))
        total_cost += line_cost
        lines.append({
            "ingredient_id": ing["id"],
            "name": ing.get("name"),
            "qty": item.get("qty"),
            "unit": item.get("unit") or ing.get("unit"),
            "line_cost": round(line_cost, 2),
        })

    yield_portions = float(recipe.get("yield_portions", 1)) or 1
    cost_per_portion = total_cost / yield_portions
    sell_inc = float(recipe.get("sell_price_inc_gst", 0))
    sell_ex = sell_inc / (1 + GST_RATE)
    margin = sell_ex - cost_per_portion
    food_cost_pct = (cost_per_portion / sell_ex * 100) if sell_ex > 0 else 0.0

    return {
        "id": recipe["id"],
        "name": recipe.get("name"),
        "category": recipe.get("category"),
        "active": recipe.get("active", True),
        "sell_price_inc_gst": round(sell_inc, 2),
        "sell_price_ex_gst": round(sell_ex, 2),
        "yield_portions": yield_portions,
        "cost_per_portion": round(cost_per_portion, 2),
        "margin_per_portion": round(margin, 2),
        "food_cost_pct": round(food_cost_pct, 1),
        "flagged": food_cost_pct > DEFAULT_FOOD_COST_TARGET_PCT,
        "missing_ingredients": missing,
        "lines": lines,
    }


# ---------------------------------------------------------------------------
# Ingredients
# ---------------------------------------------------------------------------

@router.get("/ingredients")
async def list_ingredients(venue_id: str = Query(...)) -> dict:
    enforce_venue_access(venue_id)
    db = get_db()
    ings = db.list_ingredients(venue_id) or []
    return {"venue_id": venue_id, "count": len(ings), "ingredients": ings}


@router.post("/ingredients")
async def upsert_ingredient(body: IngredientRequest) -> dict:
    enforce_venue_access(body.venue_id)
    db = get_db()
    if body.id:
        existing = db.get_ingredient(body.id)
        if not existing or existing.get("venue_id") != body.venue_id:
            raise HTTPException(status_code=404, detail="Ingredient not found")
    ing_id = body.id or f"ing-{uuid.uuid4().hex[:10]}"
    cost_per_unit = body.purchase_cost / body.purchase_size
    # A price/pack edit must not wipe the inventory levels set on this
    # ingredient — carry stock_qty/par_level over from the existing record.
    prior = db.get_ingredient(ing_id) if body.id else None
    db.save_ingredient({
        "id": ing_id,
        "venue_id": body.venue_id,
        "name": body.name,
        "unit": body.unit,
        "purchase_size": body.purchase_size,
        "purchase_cost": body.purchase_cost,
        "cost_per_unit": cost_per_unit,
        "supplier": body.supplier,
        "active": body.active,
        "stock_qty": float((prior or {}).get("stock_qty") or 0),
        "par_level": float((prior or {}).get("par_level") or 0),
        "created_at": datetime.utcnow(),
    })
    if prior is None:
        audit("ingredient.create", body.venue_id, "ingredient", ing_id, name=body.name,
              unit=body.unit, purchase_size=body.purchase_size,
              purchase_cost=body.purchase_cost, supplier=body.supplier)
    else:
        old_cost = float(prior.get("purchase_cost") or 0)
        old_size = float(prior.get("purchase_size") or 0)
        if round(old_cost, 4) != round(float(body.purchase_cost), 4) \
                or round(old_size, 4) != round(float(body.purchase_size), 4):
            audit("ingredient.price_change", body.venue_id, "ingredient", ing_id,
                  name=body.name, old_purchase_cost=old_cost, new_purchase_cost=body.purchase_cost,
                  old_purchase_size=old_size, new_purchase_size=body.purchase_size,
                  old_cost_per_unit=round(float(prior.get("cost_per_unit") or 0), 4),
                  new_cost_per_unit=round(cost_per_unit, 4), source="edit")
        else:
            audit("ingredient.update", body.venue_id, "ingredient", ing_id, name=body.name,
                  unit=body.unit, supplier=body.supplier, active=body.active)
    return {"status": "saved", "ingredient_id": ing_id,
            "cost_per_unit": round(cost_per_unit, 4), "unit": body.unit}


# ---------------------------------------------------------------------------
# Recipes
# ---------------------------------------------------------------------------

@router.get("/recipes")
async def list_recipes(venue_id: str = Query(...)) -> dict:
    enforce_venue_access(venue_id)
    db = get_db()
    ings = {i["id"]: i for i in (db.list_ingredients(venue_id) or [])}
    recipes = [_cost_recipe(r, ings) for r in (db.list_recipes(venue_id) or [])]
    return {"venue_id": venue_id, "count": len(recipes), "recipes": recipes}


@router.post("/recipes")
async def upsert_recipe(body: RecipeRequest) -> dict:
    enforce_venue_access(body.venue_id)
    db = get_db()
    if body.id:
        existing = db.get_recipe(body.id)
        if not existing or existing.get("venue_id") != body.venue_id:
            raise HTTPException(status_code=404, detail="Recipe not found")
    # Every referenced ingredient must exist at this venue — fail loud now,
    # not silently at costing time.
    ings = {i["id"]: i for i in (db.list_ingredients(body.venue_id) or [])}
    unknown = [i.ingredient_id for i in body.items if i.ingredient_id not in ings]
    if unknown:
        raise HTTPException(status_code=422, detail=f"Unknown ingredient(s): {', '.join(unknown)}")
    # Validate unit conversions up front
    for i in body.items:
        _qty_in_unit(i.qty, i.unit, ings[i.ingredient_id].get("unit", "each"))

    recipe_id = body.id or f"rcp-{uuid.uuid4().hex[:10]}"
    recipe = {
        "id": recipe_id,
        "venue_id": body.venue_id,
        "name": body.name,
        "category": body.category,
        "sell_price_inc_gst": body.sell_price_inc_gst,
        "yield_portions": body.yield_portions,
        "items": [i.model_dump() for i in body.items],
        "active": body.active,
        "created_at": datetime.utcnow(),
    }
    db.save_recipe(recipe)
    costing = _cost_recipe(recipe, ings)
    audit("recipe.update" if body.id else "recipe.create", body.venue_id, "recipe", recipe_id,
          name=body.name, category=body.category, sell_price_inc_gst=body.sell_price_inc_gst,
          items=len(body.items), cost_per_portion=costing.get("cost_per_portion"),
          food_cost_pct=costing.get("food_cost_pct"), active=body.active)
    return {"status": "saved", "recipe_id": recipe_id, "costing": costing}


# ---------------------------------------------------------------------------
# Menu-wide costing
# ---------------------------------------------------------------------------

@router.get("/costing")
async def menu_costing(venue_id: str = Query(...)) -> dict:
    """The whole menu, costed live: sorted by food-cost %, worst first, with
    flags above the target band. This is the screen that pays for the product."""
    enforce_venue_access(venue_id)
    db = get_db()
    ings = {i["id"]: i for i in (db.list_ingredients(venue_id) or [])}
    costed = [_cost_recipe(r, ings) for r in (db.list_recipes(venue_id) or []) if r.get("active", True)]
    costed.sort(key=lambda c: -c["food_cost_pct"])
    flagged = [c for c in costed if c["flagged"]]
    return {
        "venue_id": venue_id,
        "target_food_cost_pct": DEFAULT_FOOD_COST_TARGET_PCT,
        "recipe_count": len(costed),
        "flagged_count": len(flagged),
        "avg_food_cost_pct": round(sum(c["food_cost_pct"] for c in costed) / len(costed), 1) if costed else 0,
        "recipes": costed,
    }


# ---------------------------------------------------------------------------
# Starter data
# ---------------------------------------------------------------------------

_SEED_INGREDIENTS = [
    {"name": "Chicken breast", "unit": "kg", "purchase_size": 5, "purchase_cost": 62.50},
    {"name": "Panko crumbs", "unit": "kg", "purchase_size": 2, "purchase_cost": 11.00},
    {"name": "Napoli sauce", "unit": "l", "purchase_size": 3, "purchase_cost": 13.50},
    {"name": "Mozzarella", "unit": "kg", "purchase_size": 2, "purchase_cost": 24.00},
    {"name": "Chips (frozen)", "unit": "kg", "purchase_size": 10, "purchase_cost": 32.00},
    {"name": "Coffee beans", "unit": "kg", "purchase_size": 1, "purchase_cost": 38.00},
    {"name": "Milk", "unit": "l", "purchase_size": 10, "purchase_cost": 15.50},
]

_SEED_RECIPES = [
    {"name": "Chicken Parmigiana", "category": "Mains", "sell_price_inc_gst": 28.00,
     "items": [("Chicken breast", 250, "g"), ("Panko crumbs", 60, "g"),
               ("Napoli sauce", 80, "ml"), ("Mozzarella", 70, "g"), ("Chips (frozen)", 180, "g")]},
    {"name": "Bowl of Chips", "category": "Sides", "sell_price_inc_gst": 9.50,
     "items": [("Chips (frozen)", 350, "g")]},
    {"name": "Flat White", "category": "Coffee", "sell_price_inc_gst": 5.00,
     "items": [("Coffee beans", 18, "g"), ("Milk", 150, "ml")]},
]


def seed_starter_menu(db, venue_id: str) -> dict:
    """Idempotent starter ingredients + recipes (skips names that exist).
    Shared by the /seed route and the demo seeder so the demo's Menu & Sales,
    inventory and snapshot are never empty on a fresh deploy."""
    existing_ing = {i.get("name"): i for i in (db.list_ingredients(venue_id) or [])}
    created_i = []
    for d in _SEED_INGREDIENTS:
        if d["name"] in existing_ing:
            continue
        ing = {
            "id": f"ing-{uuid.uuid4().hex[:10]}",
            "venue_id": venue_id,
            "name": d["name"], "unit": d["unit"],
            "purchase_size": d["purchase_size"], "purchase_cost": d["purchase_cost"],
            "cost_per_unit": d["purchase_cost"] / d["purchase_size"],
            "supplier": "", "active": True, "created_at": datetime.utcnow(),
        }
        db.save_ingredient(ing)
        existing_ing[d["name"]] = ing
        created_i.append(d["name"])

    existing_rcp = {r.get("name") for r in (db.list_recipes(venue_id) or [])}
    created_r = []
    for d in _SEED_RECIPES:
        if d["name"] in existing_rcp:
            continue
        db.save_recipe({
            "id": f"rcp-{uuid.uuid4().hex[:10]}",
            "venue_id": venue_id,
            "name": d["name"], "category": d["category"],
            "sell_price_inc_gst": d["sell_price_inc_gst"], "yield_portions": 1,
            "items": [{"ingredient_id": existing_ing[n]["id"], "qty": q, "unit": u}
                      for (n, q, u) in d["items"]],
            "active": True, "created_at": datetime.utcnow(),
        })
        created_r.append(d["name"])
    return {"status": "seeded", "ingredients_created": created_i, "recipes_created": created_r}


@router.post("/seed")
async def seed_menu(body: SeedRequest) -> dict:
    """Idempotent starter ingredients + recipes (skips names that exist)."""
    enforce_venue_access(body.venue_id)
    return seed_starter_menu(get_db(), body.venue_id)
