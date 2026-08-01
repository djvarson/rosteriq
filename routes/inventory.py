"""
Inventory — stock on hand, stocktakes, and supplier ordering (the Restoke
replacement). Builds directly on the menu-costing ingredients so counting
stock and costing dishes share one source of truth.

The flow a venue actually runs:
    1. Set a par level on each ingredient (what you need on the shelf).
    2. Stocktake: count what's really there -> variance report valued in
       dollars (shrinkage/waste made visible), stock levels corrected.
    3. Order draft: everything below par, grouped by supplier, quantities
       rounded up to purchase pack sizes with real costs.
    4. Receive the order -> stock goes back up.

Routes (all venue-scoped):
    GET  /api/inventory                    -- stock list + value + low flags
    POST /api/inventory/levels             -- set stock/par for an ingredient
    POST /api/inventory/stocktake/start    -- open a stocktake (snapshot)
    POST /api/inventory/stocktake/count    -- record a count
    POST /api/inventory/stocktake/complete -- close -> variance + stock update
    GET  /api/inventory/stocktakes         -- history
    POST /api/inventory/order/draft        -- draft orders for below-par stock
    POST /api/inventory/order/{id}/status  -- draft -> ordered -> received
    GET  /api/inventory/orders             -- order history
"""

import logging
import math
import uuid
from datetime import date as dt_date, datetime
from typing import List, Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from rosteriq.database import get_db
from rosteriq.middleware.tenant import enforce_venue_access, get_tenant_context_optional

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/inventory", tags=["inventory"])


class LevelsBody(BaseModel):
    venue_id: str
    ingredient_id: str
    stock_qty: Optional[float] = Field(default=None, ge=0)
    par_level: Optional[float] = Field(default=None, ge=0)


class StocktakeStartBody(BaseModel):
    venue_id: str


class StocktakeCountBody(BaseModel):
    venue_id: str
    stocktake_id: str
    ingredient_id: str
    counted: float = Field(..., ge=0)


class StocktakeCompleteBody(BaseModel):
    venue_id: str
    stocktake_id: str


class OrderDraftBody(BaseModel):
    venue_id: str


class OrderStatusBody(BaseModel):
    venue_id: str
    status: str = Field(..., pattern=r"^(ordered|received|cancelled)$")


class InvoiceLine(BaseModel):
    ingredient_id: str
    packs: float = Field(..., gt=0, le=10000, description="Packs actually delivered")
    pack_cost: Optional[float] = Field(default=None, ge=0,
                                       description="Price per pack on the invoice; omit to keep the stored price")


class InvoiceBody(BaseModel):
    venue_id: str
    supplier: str = Field(default="", max_length=120)
    invoice_number: str = Field(..., min_length=1, max_length=80)
    invoice_date: Optional[dt_date] = None
    order_id: str = Field(default="", description="Optional: the ordered order this delivery fulfils")
    lines: List[InvoiceLine] = Field(..., min_length=1, max_length=200)


def _ingredient_or_404(db, venue_id: str, ingredient_id: str) -> dict:
    ing = db.get_ingredient(ingredient_id)
    if not ing or ing.get("venue_id") != venue_id:
        raise HTTPException(status_code=404, detail="Ingredient not found at this venue")
    return ing


# ---------------------------------------------------------------------------
# Stock levels
# ---------------------------------------------------------------------------

@router.get("")
async def stock_list(venue_id: str = Query(...)) -> dict:
    """Every ingredient with stock on hand, par, value, and a low flag."""
    enforce_venue_access(venue_id)
    db = get_db()
    rows = []
    total_value = 0.0
    low_count = 0
    for ing in db.list_ingredients(venue_id) or []:
        if not ing.get("active", True):
            continue
        # round(3): repeated float increments (sales depletion) accumulate
        # artifacts like -0.8400000000000001 that a venue owner would see
        stock = round(float(ing.get("stock_qty") or 0), 3)
        par = float(ing.get("par_level") or 0)
        cpu = float(ing.get("cost_per_unit") or 0)
        value = round(stock * cpu, 2)
        low = par > 0 and stock < par
        if low:
            low_count += 1
        total_value += value
        rows.append({
            "ingredient_id": ing["id"],
            "name": ing["name"],
            "unit": ing.get("unit"),
            "supplier": ing.get("supplier"),
            "stock_qty": stock,
            "par_level": par,
            "cost_per_unit": cpu,
            "stock_value": value,
            "low": low,
        })
    rows.sort(key=lambda r: (not r["low"], r["name"]))
    return {"venue_id": venue_id, "count": len(rows), "low_count": low_count,
            "total_stock_value": round(total_value, 2), "items": rows}


@router.post("/levels")
async def set_levels(body: LevelsBody) -> dict:
    """Set stock on hand and/or par level for one ingredient."""
    enforce_venue_access(body.venue_id)
    if body.stock_qty is None and body.par_level is None:
        raise HTTPException(status_code=422, detail="Provide stock_qty and/or par_level")
    db = get_db()
    ing = _ingredient_or_404(db, body.venue_id, body.ingredient_id)
    if body.stock_qty is not None:
        ing["stock_qty"] = float(body.stock_qty)
    if body.par_level is not None:
        ing["par_level"] = float(body.par_level)
    db.save_ingredient(ing)
    return {"status": "saved", "ingredient_id": ing["id"],
            "stock_qty": float(ing.get("stock_qty") or 0),
            "par_level": float(ing.get("par_level") or 0)}


# ---------------------------------------------------------------------------
# Stocktakes
# ---------------------------------------------------------------------------

@router.post("/stocktake/start")
async def start_stocktake(body: StocktakeStartBody) -> dict:
    """Open a stocktake: snapshots every ingredient's expected level."""
    enforce_venue_access(body.venue_id)
    db = get_db()
    for st in db.list_stocktakes(body.venue_id) or []:
        if st.get("status") == "open":
            raise HTTPException(status_code=409,
                                detail="A stocktake is already open — complete it first")
    ingredients = [i for i in (db.list_ingredients(body.venue_id) or [])
                   if i.get("active", True)]
    if not ingredients:
        raise HTTPException(status_code=422,
                            detail="No ingredients to count — add them in Menu & Costing first")
    tenant = get_tenant_context_optional()
    st = {
        "id": f"st-{uuid.uuid4().hex[:10]}",
        "venue_id": body.venue_id,
        "status": "open",
        "started_by": tenant.user_id if tenant else None,
        "items": [{
            "ingredient_id": i["id"],
            "name": i["name"],
            "unit": i.get("unit"),
            "expected": float(i.get("stock_qty") or 0),
            "counted": None,
            "unit_cost": float(i.get("cost_per_unit") or 0),
        } for i in ingredients],
        "started_at": datetime.utcnow(),
        "completed_at": None,
        "total_variance_value": None,
        "created_at": datetime.utcnow(),
    }
    db.save_stocktake(st)
    return {"status": "open", "stocktake_id": st["id"], "item_count": len(st["items"])}


@router.post("/stocktake/count")
async def record_count(body: StocktakeCountBody) -> dict:
    enforce_venue_access(body.venue_id)
    db = get_db()
    st = db.get_stocktake(body.stocktake_id)
    if not st or st.get("venue_id") != body.venue_id:
        raise HTTPException(status_code=404, detail="Stocktake not found")
    if st.get("status") != "open":
        raise HTTPException(status_code=409, detail="Stocktake is already completed")
    if not any(i.get("ingredient_id") == body.ingredient_id for i in st.get("items", [])):
        raise HTTPException(status_code=404, detail="Ingredient not in this stocktake")
    # Atomic single-item update: two staff counting different items at once
    # must never clobber each other's counts.
    if not db.update_stocktake_count(body.stocktake_id, body.ingredient_id,
                                     float(body.counted)):
        raise HTTPException(status_code=409, detail="Stocktake is already completed")
    return {"status": "counted", "ingredient_id": body.ingredient_id,
            "counted": float(body.counted)}


@router.post("/stocktake/complete")
async def complete_stocktake(body: StocktakeCompleteBody) -> dict:
    """Close the stocktake: value the variance and correct stock levels.
    Uncounted items are flagged and keep their expected level — we never
    invent a count that nobody took."""
    enforce_venue_access(body.venue_id)
    db = get_db()
    st = db.get_stocktake(body.stocktake_id)
    if not st or st.get("venue_id") != body.venue_id:
        raise HTTPException(status_code=404, detail="Stocktake not found")
    if st.get("status") != "open":
        raise HTTPException(status_code=409, detail="Stocktake is already completed")

    counted_items = [i for i in st["items"] if i.get("counted") is not None]
    if not counted_items:
        raise HTTPException(status_code=422,
                            detail="Nothing has been counted yet — count at least one item")

    total_variance = 0.0
    uncounted = []
    for item in st["items"]:
        if item.get("counted") is None:
            item["variance"] = None
            item["variance_value"] = None
            uncounted.append(item["name"])
            continue
        variance = float(item["counted"]) - float(item["expected"])
        item["variance"] = round(variance, 3)
        item["variance_value"] = round(variance * float(item.get("unit_cost") or 0), 2)
        total_variance += item["variance_value"]
        ing = db.get_ingredient(item["ingredient_id"])
        if ing and ing.get("venue_id") == body.venue_id:
            ing["stock_qty"] = float(item["counted"])
            db.save_ingredient(ing)

    st["status"] = "completed"
    st["completed_at"] = datetime.utcnow()
    st["total_variance_value"] = round(total_variance, 2)
    db.save_stocktake(st)
    logger.info(f"Stocktake {st['id']} completed at {body.venue_id}: "
                f"variance ${st['total_variance_value']}, {len(uncounted)} uncounted")
    return {
        "status": "completed",
        "stocktake_id": st["id"],
        "counted": len(counted_items),
        "uncounted": uncounted,
        "total_variance_value": st["total_variance_value"],
    }


@router.get("/stocktakes")
async def stocktake_history(venue_id: str = Query(...)) -> dict:
    enforce_venue_access(venue_id)
    db = get_db()
    rows = db.list_stocktakes(venue_id) or []
    return {"venue_id": venue_id, "count": len(rows), "stocktakes": [{
        "id": s["id"], "status": s.get("status"),
        "started_at": str(s.get("started_at")),
        "completed_at": str(s.get("completed_at")) if s.get("completed_at") else None,
        "total_variance_value": s.get("total_variance_value"),
        "items": s.get("items", []),
    } for s in rows]}


# ---------------------------------------------------------------------------
# Supplier orders
# ---------------------------------------------------------------------------

@router.post("/order/draft")
async def draft_orders(body: OrderDraftBody) -> dict:
    """Draft one order per supplier for everything below par, quantities
    rounded UP to whole purchase packs with real pack costs."""
    enforce_venue_access(body.venue_id)
    db = get_db()
    # Ingredients already on an open (draft/ordered) order are covered — never
    # draft the same shortfall twice, or a double-click double-orders it.
    already_covered = set()
    for existing in db.list_supplier_orders(body.venue_id) or []:
        if existing.get("status") in ("draft", "ordered"):
            for item in existing.get("items", []):
                already_covered.add(item.get("ingredient_id"))

    by_supplier: dict = {}
    for ing in db.list_ingredients(body.venue_id) or []:
        if not ing.get("active", True) or ing["id"] in already_covered:
            continue
        stock = float(ing.get("stock_qty") or 0)
        par = float(ing.get("par_level") or 0)
        if par <= 0 or stock >= par:
            continue
        pack_size = float(ing.get("purchase_size") or 1) or 1
        pack_cost = float(ing.get("purchase_cost") or 0)
        needed = par - stock
        # round() before ceil(): binary-float artifacts (0.8-0.2=0.6000...01)
        # would otherwise over-order a whole extra pack of real money.
        packs = max(1, math.ceil(round(needed / pack_size, 9)))
        supplier = (ing.get("supplier") or "Unassigned supplier").strip() or "Unassigned supplier"
        by_supplier.setdefault(supplier, []).append({
            "ingredient_id": ing["id"],
            "name": ing["name"],
            "unit": ing.get("unit"),
            "stock_qty": stock,
            "par_level": par,
            "needed": round(needed, 3),
            "pack_size": pack_size,
            "packs": packs,
            "pack_cost": pack_cost,
            "line_cost": round(packs * pack_cost, 2),
        })
    if not by_supplier:
        raise HTTPException(status_code=409,
                            detail="Nothing to order — everything below par is "
                                   "already covered by an open order, or nothing is below par")
    orders = []
    for supplier, items in sorted(by_supplier.items()):
        order = {
            "id": f"so-{uuid.uuid4().hex[:10]}",
            "venue_id": body.venue_id,
            "supplier": supplier,
            "status": "draft",
            "items": items,
            "total_cost": round(sum(i["line_cost"] for i in items), 2),
            "created_at": datetime.utcnow(),
            "ordered_at": None,
            "received_at": None,
        }
        db.save_supplier_order(order)
        orders.append({"order_id": order["id"], "supplier": supplier,
                       "lines": len(items), "total_cost": order["total_cost"]})
    return {"status": "drafted", "orders": orders}


@router.post("/order/{order_id}/status")
async def set_order_status(order_id: str, body: OrderStatusBody) -> dict:
    """draft -> ordered -> received (receiving books the stock in);
    draft/ordered -> cancelled. Anything else is refused."""
    enforce_venue_access(body.venue_id)
    db = get_db()
    order = db.get_supplier_order(order_id)
    if not order or order.get("venue_id") != body.venue_id:
        raise HTTPException(status_code=404, detail="Order not found")
    current = order.get("status")
    allowed = {
        ("draft", "ordered"), ("draft", "cancelled"),
        ("ordered", "received"), ("ordered", "cancelled"),
    }
    if (current, body.status) not in allowed:
        raise HTTPException(status_code=409,
                            detail=f"Can't go from {current} to {body.status}")

    if body.status == "received":
        # A delivery landing mid-stocktake would be overwritten (or double
        # counted) when the stocktake completes — refuse, fail loud.
        for st in db.list_stocktakes(body.venue_id) or []:
            if st.get("status") == "open":
                raise HTTPException(
                    status_code=409,
                    detail="A stocktake is open — complete it before receiving "
                           "stock, or the counts and the delivery will fight.")

    stamp = {"ordered": "ordered_at", "received": "received_at"}.get(body.status)
    # Atomic conditional transition: on a double-click only one request wins,
    # so stock can never be booked in twice.
    if not db.transition_supplier_order(order_id, current, body.status, stamp):
        raise HTTPException(status_code=409,
                            detail=f"Order is no longer {current} — refresh and retry")
    if body.status == "received":
        for item in order.get("items", []):
            db.increment_ingredient_stock(
                item["ingredient_id"],
                float(item.get("packs", 0)) * float(item.get("pack_size", 0)))
    logger.info(f"Supplier order {order_id} -> {body.status} at {body.venue_id}")
    return {"status": body.status, "order_id": order_id}


@router.post("/invoice")
async def enter_invoice(body: InvoiceBody) -> dict:
    """Receive stock against the supplier's ACTUAL invoice.

    - Books in the packs actually delivered (not what the order hoped for).
    - A changed pack price updates the ingredient and recosts every menu
      margin instantly; each change is reported old -> new.
    - Optionally closes the linked ordered order (without double-booking —
      the invoice's quantities are the truth, the order's are ignored).
    - Same invoice number from the same supplier twice is refused.
    """
    enforce_venue_access(body.venue_id)
    db = get_db()

    # A delivery landing mid-stocktake corrupts the count — same guard as receive
    for st in db.list_stocktakes(body.venue_id) or []:
        if st.get("status") == "open":
            raise HTTPException(
                status_code=409,
                detail="A stocktake is open — complete it before entering a delivery.")

    norm_num = body.invoice_number.strip().lower()
    for prev in db.list_supplier_invoices(body.venue_id) or []:
        if (str(prev.get("invoice_number", "")).strip().lower() == norm_num
                and (prev.get("supplier") or "") == body.supplier):
            raise HTTPException(status_code=409,
                                detail=f"Invoice {body.invoice_number} from this supplier "
                                       "is already entered")

    ingredients = {i["id"]: i for i in (db.list_ingredients(body.venue_id) or [])}
    unknown = [l.ingredient_id for l in body.lines if l.ingredient_id not in ingredients]
    if unknown:
        raise HTTPException(status_code=422,
                            detail=f"Unknown ingredient(s): {', '.join(unknown)}")

    order = None
    if body.order_id:
        order = db.get_supplier_order(body.order_id)
        if not order or order.get("venue_id") != body.venue_id:
            raise HTTPException(status_code=404, detail="Linked order not found")
        if order.get("status") != "ordered":
            raise HTTPException(status_code=409,
                                detail=f"Linked order is {order.get('status')}, not ordered")

    items = []
    price_changes = []
    total = 0.0
    for line in body.lines:
        ing = ingredients[line.ingredient_id]
        pack_size = float(ing.get("purchase_size") or 1) or 1
        stored_cost = float(ing.get("purchase_cost") or 0)
        pack_cost = stored_cost if line.pack_cost is None else float(line.pack_cost)
        line_total = round(line.packs * pack_cost, 2)
        total += line_total
        items.append({
            "ingredient_id": ing["id"], "name": ing["name"], "unit": ing.get("unit"),
            "packs": line.packs, "pack_size": pack_size,
            "pack_cost": pack_cost, "line_total": line_total,
        })
        if line.pack_cost is not None and round(pack_cost, 4) != round(stored_cost, 4):
            price_changes.append({
                "ingredient_id": ing["id"], "name": ing["name"],
                "old_pack_cost": stored_cost, "new_pack_cost": pack_cost,
            })
            ing["purchase_cost"] = pack_cost
            ing["cost_per_unit"] = pack_cost / pack_size
            db.save_ingredient(ing)

    # Close the linked order FIRST (atomic transition) so a double-submit
    # can't book the same delivery twice via order + invoice paths.
    if order is not None:
        if not db.transition_supplier_order(body.order_id, "ordered", "received",
                                            "received_at"):
            raise HTTPException(status_code=409,
                                detail="Order was already received — refresh and retry")

    for item in items:
        db.increment_ingredient_stock(item["ingredient_id"],
                                      float(item["packs"]) * float(item["pack_size"]))

    inv = {
        "id": f"si-{uuid.uuid4().hex[:10]}",
        "venue_id": body.venue_id,
        "supplier": body.supplier,
        "invoice_number": body.invoice_number.strip(),
        "invoice_date": body.invoice_date,
        "order_id": body.order_id or None,
        "items": items,
        "total": round(total, 2),
        "price_changes": price_changes,
        "created_at": datetime.utcnow(),
    }
    db.save_supplier_invoice(inv)
    logger.info(f"Invoice {inv['invoice_number']} entered at {body.venue_id}: "
                f"${inv['total']}, {len(price_changes)} price changes")
    return {
        "status": "entered",
        "invoice_id": inv["id"],
        "total": inv["total"],
        "stock_booked": {i["ingredient_id"]: round(i["packs"] * i["pack_size"], 3)
                         for i in items},
        "price_changes": price_changes,
        "order_closed": bool(order),
    }


@router.get("/invoices")
async def invoice_history(venue_id: str = Query(...)) -> dict:
    enforce_venue_access(venue_id)
    db = get_db()
    rows = db.list_supplier_invoices(venue_id) or []
    return {"venue_id": venue_id, "count": len(rows), "invoices": [{
        "id": i["id"], "supplier": i.get("supplier"),
        "invoice_number": i.get("invoice_number"),
        "invoice_date": str(i.get("invoice_date")) if i.get("invoice_date") else None,
        "order_id": i.get("order_id"), "total": i.get("total"),
        "price_changes": i.get("price_changes", []),
        "items": i.get("items", []),
        "created_at": str(i.get("created_at")),
    } for i in rows]}


@router.get("/usage")
async def usage_report(venue_id: str = Query(...)) -> dict:
    """Theoretical vs actual usage between the two most recent completed
    stocktakes — where the money actually leaks.

        actual      = opening count + deliveries received - closing count
        theoretical = what the recorded sales SHOULD have used (via recipes)
        variance    = actual - theoretical  (positive = unexplained loss:
                      waste, over-portioning, shrinkage)

    Theoretical usage is computed against CURRENT recipes; if a recipe
    changed mid-period the attribution shifts slightly — the report says so
    rather than pretending otherwise.
    """
    enforce_venue_access(venue_id)
    from rosteriq.routes.menu_costing import _qty_in_unit  # shared unit conversion
    db = get_db()

    completed = [s for s in (db.list_stocktakes(venue_id) or [])
                 if s.get("status") == "completed" and s.get("completed_at")]
    if len(completed) < 2:
        raise HTTPException(
            status_code=422,
            detail="Usage needs two completed stocktakes to compare — run your "
                   "next stocktake and this report unlocks.")
    completed.sort(key=lambda s: str(s.get("completed_at")))
    opening_st, closing_st = completed[-2], completed[-1]

    def _counts(st):
        return {i["ingredient_id"]: float(i["counted"])
                for i in st.get("items", []) if i.get("counted") is not None}

    opening = _counts(opening_st)
    closing = _counts(closing_st)

    def _ts(v):
        if isinstance(v, datetime):
            return v.replace(tzinfo=None)
        return datetime.fromisoformat(str(v).replace("Z", "+00:00")).replace(tzinfo=None)

    start_ts, end_ts = _ts(opening_st["completed_at"]), _ts(closing_st["completed_at"])

    # Deliveries received in the window
    received: dict = {}
    for order in db.list_supplier_orders(venue_id) or []:
        if order.get("status") != "received" or not order.get("received_at"):
            continue
        rts = _ts(order["received_at"])
        if not (start_ts < rts <= end_ts):
            continue
        for item in order.get("items", []):
            received[item["ingredient_id"]] = received.get(item["ingredient_id"], 0.0) + \
                float(item.get("packs", 0)) * float(item.get("pack_size", 0))

    # Theoretical usage from recorded sales (dates within the window's days)
    ingredients = {i["id"]: i for i in (db.list_ingredients(venue_id) or [])}
    recipes = {r["id"]: r for r in (db.list_recipes(venue_id) or [])}
    theoretical: dict = {}
    for sale in db.list_dish_sales(venue_id, start_ts.date(), end_ts.date()) or []:
        recipe = recipes.get(sale.get("recipe_id"))
        if not recipe:
            continue
        yield_portions = float(recipe.get("yield_portions", 1)) or 1
        for item in recipe.get("items", []):
            ing = ingredients.get(item.get("ingredient_id"))
            if not ing:
                continue
            try:
                per_portion = _qty_in_unit(
                    float(item.get("qty", 0)), item.get("unit"), ing.get("unit", "each"),
                ) / yield_portions
            except HTTPException:
                continue
            theoretical[ing["id"]] = theoretical.get(ing["id"], 0.0) + \
                per_portion * float(sale.get("qty") or 0)

    rows = []
    total_variance_value = 0.0
    for ing_id in sorted(set(opening) & set(closing)):
        ing = ingredients.get(ing_id)
        if not ing:
            continue
        actual = opening[ing_id] + received.get(ing_id, 0.0) - closing[ing_id]
        theo = theoretical.get(ing_id, 0.0)
        variance = actual - theo
        cpu = float(ing.get("cost_per_unit") or 0)
        variance_value = round(variance * cpu, 2)
        total_variance_value += variance_value
        rows.append({
            "ingredient_id": ing_id,
            "name": ing.get("name"),
            "unit": ing.get("unit"),
            "opening": round(opening[ing_id], 3),
            "received": round(received.get(ing_id, 0.0), 3),
            "closing": round(closing[ing_id], 3),
            "actual_usage": round(actual, 3),
            "theoretical_usage": round(theo, 3),
            "variance": round(variance, 3),
            "variance_value": variance_value,
        })
    rows.sort(key=lambda r: -r["variance_value"])
    return {
        "venue_id": venue_id,
        "from": start_ts.isoformat(),
        "to": end_ts.isoformat(),
        "basis": "current recipes",
        "total_variance_value": round(total_variance_value, 2),
        "items": rows,
    }


@router.get("/orders")
async def order_history(venue_id: str = Query(...)) -> dict:
    enforce_venue_access(venue_id)
    db = get_db()
    rows = db.list_supplier_orders(venue_id) or []
    return {"venue_id": venue_id, "count": len(rows), "orders": [{
        "id": o["id"], "supplier": o.get("supplier"), "status": o.get("status"),
        "total_cost": o.get("total_cost"), "created_at": str(o.get("created_at")),
        "ordered_at": str(o.get("ordered_at")) if o.get("ordered_at") else None,
        "received_at": str(o.get("received_at")) if o.get("received_at") else None,
        "items": o.get("items", []),
    } for o in rows]}
