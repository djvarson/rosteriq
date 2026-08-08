"""
Ingredient import from any program's export — populate the kitchen's
foundation (the ingredients costing, inventory and waste all build on)
without a live connection. A venue leaving Restoke or a supplier price list
exports name / unit / pack size / pack cost / supplier; paste it here.

Forgiving of real files: delimiter sniffed, header mapped by aliases, units
normalised (kg/kilogram, l/litre, ea/each...), costs stripped of $, cost per
unit derived. Every row reported created / skipped-with-reason.

Route:
    POST /api/setup/import-ingredients  { venue_id, content }
"""

import csv
import io
import logging
import re
import uuid
from datetime import datetime

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from rosteriq.database import get_db
from rosteriq.middleware.tenant import enforce_venue_access

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/setup", tags=["setup"])


class IngredientImportBody(BaseModel):
    venue_id: str
    content: str = Field(..., min_length=1, max_length=1_000_000)


_NAME_KEYS = {"name", "ingredient", "item", "product", "product name", "description"}
_UNIT_KEYS = {"unit", "uom", "unit of measure", "measure"}
_SIZE_KEYS = {"pack size", "size", "purchase size", "pack qty", "quantity",
              "qty", "pack", "unit size"}
_COST_KEYS = {"cost", "price", "pack cost", "purchase cost", "unit cost",
              "buy price", "cost price"}
_SUPPLIER_KEYS = {"supplier", "vendor", "brand"}

# Normalise messy unit strings onto the 5 costing units.
_UNIT_ALIASES = {
    "g": "g", "gram": "g", "grams": "g", "gm": "g",
    "kg": "kg", "kilo": "kg", "kilogram": "kg", "kilograms": "kg", "kgs": "kg",
    "ml": "ml", "millilitre": "ml", "milliliter": "ml",
    "l": "l", "litre": "l", "liter": "l", "litres": "l", "liters": "l", "lt": "l",
    "each": "each", "ea": "each", "unit": "each", "units": "each",
    "pc": "each", "pcs": "each", "piece": "each", "count": "each",
}


def _norm_unit(v: str) -> str:
    return _UNIT_ALIASES.get(str(v or "").strip().lower(), "each")


def _num(v: str):
    s = re.sub(r"[^\d.]", "", str(v or ""))
    if not s:
        return None
    try:
        return float(s)
    except ValueError:
        return None


@router.post("/import-ingredients")
async def import_ingredients(body: IngredientImportBody) -> dict:
    enforce_venue_access(body.venue_id)
    db = get_db()
    if not db.get_venue(body.venue_id):
        raise HTTPException(status_code=404, detail="Venue not found")

    text = body.content.replace("\r\n", "\n").replace("\r", "\n").strip()
    sample = text.split("\n", 1)[0]
    delim = "\t" if "\t" in sample else (
        ";" if sample.count(";") > sample.count(",") else (
            "|" if "|" in sample and "," not in sample else ","))
    rows = [r for r in csv.reader(io.StringIO(text), delimiter=delim) if any(c.strip() for c in r)]
    if not rows:
        raise HTTPException(status_code=422, detail="No rows found")

    def _norm(h):
        return str(h).strip().lower()
    header = [_norm(h) for h in rows[0]]
    has_header = any(h in (_NAME_KEYS | _COST_KEYS | _SIZE_KEYS) for h in header)
    col = {"name": None, "unit": None, "size": None, "cost": None, "supplier": None}
    if has_header:
        keymap = [("name", _NAME_KEYS), ("unit", _UNIT_KEYS), ("size", _SIZE_KEYS),
                  ("cost", _COST_KEYS), ("supplier", _SUPPLIER_KEYS)]
        for idx, h in enumerate(header):
            for field, keys in keymap:
                if col[field] is None and h in keys:
                    col[field] = idx
                    break
        data_rows = rows[1:]
    else:
        data_rows = rows  # infer: col0 name, then a numeric cost, etc.

    existing_names = {(i.get("name") or "").strip().lower()
                      for i in (db.list_ingredients(body.venue_id) or [])}
    created, skipped = [], []
    now = datetime.utcnow()
    for i, raw in enumerate(data_rows, start=(2 if has_header else 1)):
        cells = [c.strip() for c in raw]

        def cell(field):
            j = col[field]
            return cells[j] if j is not None and j < len(cells) else ""

        name = cell("name") if col["name"] is not None else (cells[0] if cells else "")
        name = name.strip()
        if not name or name.lower() in _NAME_KEYS:
            skipped.append({"row": i, "reason": "no name"})
            continue
        if name.lower() in existing_names:
            skipped.append({"row": i, "name": name, "reason": "already exists"})
            continue

        unit = _norm_unit(cell("unit"))
        size = _num(cell("size"))
        if size is None or size <= 0:
            size = 1.0  # default: priced per single unit
        cost = _num(cell("cost"))
        if cost is None:
            cost = 0.0  # unknown cost -> 0, owner fills it in later
        supplier = cell("supplier")

        try:
            db.save_ingredient({
                "id": f"ing-{uuid.uuid4().hex[:10]}",
                "venue_id": body.venue_id,
                "name": name, "unit": unit,
                "purchase_size": size, "purchase_cost": cost,
                "cost_per_unit": (cost / size) if size else 0.0,
                "supplier": supplier, "active": True,
                "stock_qty": 0, "par_level": 0,
                "created_at": now,
            })
            existing_names.add(name.lower())
            created.append({"name": name, "unit": unit, "pack_size": size,
                            "pack_cost": cost, "supplier": supplier or None})
        except Exception as e:
            skipped.append({"row": i, "name": name, "reason": f"invalid: {e}"})

    logger.info(f"Ingredient import at {body.venue_id}: {len(created)} created, "
                f"{len(skipped)} skipped")
    return {
        "status": "imported",
        "created_count": len(created),
        "skipped_count": len(skipped),
        "created": created[:100],
        "skipped": skipped[:100],
        "note": "Rows without a pack cost import at $0 — set the real cost in "
                "Menu & Sales so costing and margins are right.",
    }
