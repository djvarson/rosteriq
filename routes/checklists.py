"""
Compliance checklists — opening/closing task lists and food-safety temperature
logs. The daily-habit pillar of the Venue OS: managers open RosterIQ every
morning because the venue literally can't open without it.

Item types:
    check -- tick it off
    temp  -- record a °C reading; out-of-range readings are FLAGGED, not hidden
    note  -- free-text observation

Runs are the compliance evidence trail: who completed what, when, with every
flagged reading preserved for food-safety audits.

Routes (venue-scoped):
    GET  /api/checklists/templates            -- list templates
    POST /api/checklists/templates            -- create/update a template
    POST /api/checklists/templates/seed       -- one-click AU hospo defaults
    GET  /api/checklists/today                -- today's board (per-template status)
    POST /api/checklists/runs/start           -- start a run from a template
    POST /api/checklists/runs/{run_id}/item   -- tick / record an item
    POST /api/checklists/runs/{run_id}/complete
    GET  /api/checklists/runs                 -- history for a date range
"""

import logging
import uuid
from datetime import date, datetime
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field

from rosteriq.database import get_db
from rosteriq.services.clock import venue_today
from rosteriq.middleware.auth import get_current_user, UserContext
from rosteriq.middleware.tenant import enforce_venue_access, enforce_venue_manager

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/checklists", tags=["checklists"])


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------

class TemplateItem(BaseModel):
    id: str = Field(default_factory=lambda: f"i-{uuid.uuid4().hex[:8]}")
    label: str = Field(..., min_length=1, max_length=200)
    type: str = Field(default="check", pattern="^(check|temp|note)$")
    target_min: Optional[float] = Field(default=None, description="°C lower bound (temp items)")
    target_max: Optional[float] = Field(default=None, description="°C upper bound (temp items)")


class TemplateRequest(BaseModel):
    venue_id: str
    id: Optional[str] = Field(default=None, description="Omit to create")
    name: str = Field(..., min_length=1, max_length=100)
    schedule: str = Field(default="daily", pattern="^(opening|closing|daily|weekly)$")
    items: List[TemplateItem] = Field(..., min_length=1, max_length=50)
    active: bool = True


class SeedRequest(BaseModel):
    venue_id: str


class StartRunRequest(BaseModel):
    venue_id: str
    template_id: str


class ItemUpdateRequest(BaseModel):
    venue_id: str
    item_id: str
    done: bool = True
    value: Optional[float] = Field(default=None, description="°C reading for temp items")
    note: str = Field(default="", max_length=300)


class CompleteRunRequest(BaseModel):
    venue_id: str


# AU hospitality defaults — food-safety temps per FSANZ guidance (cold ≤5°C,
# frozen ≤-15°C, hot-hold ≥60°C).
_DEFAULT_TEMPLATES = [
    {
        "name": "Opening",
        "schedule": "opening",
        "items": [
            {"label": "Unlock, disarm alarm, lights on", "type": "check"},
            {"label": "Fridge 1 temperature (°C)", "type": "temp", "target_min": -1, "target_max": 5},
            {"label": "Fridge 2 temperature (°C)", "type": "temp", "target_min": -1, "target_max": 5},
            {"label": "Freezer temperature (°C)", "type": "temp", "target_min": -30, "target_max": -15},
            {"label": "Sanitiser buckets made up", "type": "check"},
            {"label": "Floors clear, no trip hazards", "type": "check"},
            {"label": "Till float counted", "type": "check"},
        ],
    },
    {
        "name": "Closing",
        "schedule": "closing",
        "items": [
            {"label": "Fridge temperatures checked (°C)", "type": "temp", "target_min": -1, "target_max": 5},
            {"label": "Hot equipment off (fryers, grills, coffee machine)", "type": "check"},
            {"label": "Benches and boards sanitised", "type": "check"},
            {"label": "Bins out, bin area hosed", "type": "check"},
            {"label": "Till reconciled and float secured", "type": "check"},
            {"label": "Doors locked, alarm armed", "type": "check"},
        ],
    },
    {
        "name": "Food Safety (daily)",
        "schedule": "daily",
        "items": [
            {"label": "Deliveries checked and put away promptly", "type": "check"},
            {"label": "Date labels on all prepped items", "type": "check"},
            {"label": "Hot-hold temperature (°C)", "type": "temp", "target_min": 60, "target_max": 100},
            {"label": "Allergen matrix up to date", "type": "check"},
            {"label": "Anything unusual to note?", "type": "note"},
        ],
    },
]


def _flag_for(item: dict, value: Optional[float]) -> bool:
    """A temp reading outside its target band is flagged — visible, never hidden."""
    if item.get("type") != "temp" or value is None:
        return False
    lo, hi = item.get("target_min"), item.get("target_max")
    if lo is not None and value < lo:
        return True
    if hi is not None and value > hi:
        return True
    return False


# ---------------------------------------------------------------------------
# Templates
# ---------------------------------------------------------------------------

@router.get("/templates")
async def list_templates(venue_id: str = Query(...)) -> dict:
    enforce_venue_access(venue_id)
    db = get_db()
    tpls = db.list_checklist_templates(venue_id) or []
    return {"venue_id": venue_id, "count": len(tpls), "templates": tpls}


@router.post("/templates")
async def upsert_template(body: TemplateRequest) -> dict:
    enforce_venue_manager(body.venue_id)
    db = get_db()
    tpl_id = body.id or f"ckt-{uuid.uuid4().hex[:10]}"
    if body.id:
        existing = db.get_checklist_template(body.id)
        if not existing or existing.get("venue_id") != body.venue_id:
            raise HTTPException(status_code=404, detail="Template not found")
    tpl = {
        "id": tpl_id,
        "venue_id": body.venue_id,
        "name": body.name,
        "schedule": body.schedule,
        "items": [i.model_dump() for i in body.items],
        "active": body.active,
        "created_at": datetime.utcnow(),
    }
    db.save_checklist_template(tpl)
    return {"status": "saved", "template_id": tpl_id}


@router.post("/templates/seed")
async def seed_default_templates(body: SeedRequest) -> dict:
    """One click: give the venue the standard AU hospo checklists (idempotent —
    skips any template name that already exists)."""
    enforce_venue_manager(body.venue_id)
    db = get_db()
    existing_names = {t.get("name") for t in (db.list_checklist_templates(body.venue_id) or [])}
    created = []
    for d in _DEFAULT_TEMPLATES:
        if d["name"] in existing_names:
            continue
        tpl = {
            "id": f"ckt-{uuid.uuid4().hex[:10]}",
            "venue_id": body.venue_id,
            "name": d["name"],
            "schedule": d["schedule"],
            "items": [TemplateItem(**i).model_dump() for i in d["items"]],
            "active": True,
            "created_at": datetime.utcnow(),
        }
        db.save_checklist_template(tpl)
        created.append(d["name"])
    return {"status": "seeded", "created": created,
            "skipped": sorted(existing_names & {d["name"] for d in _DEFAULT_TEMPLATES})}


# ---------------------------------------------------------------------------
# Runs
# ---------------------------------------------------------------------------

@router.get("/today")
async def today_board(venue_id: str = Query(...)) -> dict:
    """Today's checklist board: each active template with its run status."""
    enforce_venue_access(venue_id)
    db = get_db()
    # The venue's own day. On the host clock (UTC) the opening checklist a
    # Perth venue ticked off at 08:00 stops matching "today" at 08:00 local
    # and the board resets to "not started" in front of them.
    today = venue_today(venue_id, db)
    runs = {r["template_id"]: r for r in (db.list_checklist_runs(venue_id, today, today) or [])}
    board = []
    for tpl in db.list_checklist_templates(venue_id) or []:
        if not tpl.get("active", True):
            continue
        run = runs.get(tpl["id"])
        done_items = sum(1 for i in (run or {}).get("items", []) if i.get("done"))
        board.append({
            "template_id": tpl["id"],
            "name": tpl["name"],
            "schedule": tpl.get("schedule", "daily"),
            "item_count": len(tpl.get("items", [])),
            "run_id": (run or {}).get("id"),
            "status": (run or {}).get("status", "not_started"),
            "done_items": done_items,
            "flags_count": (run or {}).get("flags_count", 0),
        })
    return {"venue_id": venue_id, "date": today.isoformat(), "checklists": board}


@router.post("/runs/start")
async def start_run(body: StartRunRequest, user: UserContext = Depends(get_current_user)) -> dict:
    enforce_venue_access(body.venue_id)
    db = get_db()
    tpl = db.get_checklist_template(body.template_id)
    if not tpl or tpl.get("venue_id") != body.venue_id:
        raise HTTPException(status_code=404, detail="Template not found")

    today = venue_today(body.venue_id, db)
    for r in db.list_checklist_runs(body.venue_id, today, today) or []:
        if r.get("template_id") == body.template_id and r.get("status") != "complete":
            return {"status": "already_started", "run_id": r["id"], "run": r}

    run = {
        "id": f"ckr-{uuid.uuid4().hex[:10]}",
        "venue_id": body.venue_id,
        "template_id": tpl["id"],
        "template_name": tpl["name"],
        "run_date": today,
        "started_at": datetime.utcnow(),
        "completed_at": None,
        "completed_by": None,
        "status": "in_progress",
        "items": [
            {**i, "done": False, "value": None, "flagged": False, "note": ""}
            for i in tpl.get("items", [])
        ],
        "flags_count": 0,
        "created_at": datetime.utcnow(),
    }
    db.save_checklist_run(run)
    logger.info(f"Checklist run started: {tpl['name']} at {body.venue_id} by {user.user_id}")
    return {"status": "started", "run_id": run["id"], "run": run}


@router.post("/runs/{run_id}/item")
async def update_item(run_id: str, body: ItemUpdateRequest) -> dict:
    enforce_venue_access(body.venue_id)
    db = get_db()
    run = db.get_checklist_run(run_id)
    if not run or run.get("venue_id") != body.venue_id:
        raise HTTPException(status_code=404, detail="Run not found")
    if run.get("status") == "complete":
        raise HTTPException(status_code=409, detail="Run is already complete")

    target = None
    for i in run.get("items", []):
        if i.get("id") == body.item_id:
            target = i
            break
    if target is None:
        raise HTTPException(status_code=404, detail="Item not found in this run")
    if target.get("type") == "temp" and body.done and body.value is None:
        raise HTTPException(status_code=422, detail="A temperature reading is required for this item")

    target["done"] = body.done
    target["value"] = body.value
    target["note"] = body.note
    target["flagged"] = _flag_for(target, body.value)
    run["flags_count"] = sum(1 for i in run["items"] if i.get("flagged"))
    db.save_checklist_run(run)
    return {
        "status": "updated",
        "item_id": body.item_id,
        "flagged": target["flagged"],
        "flags_count": run["flags_count"],
        "done_items": sum(1 for i in run["items"] if i.get("done")),
        "item_count": len(run["items"]),
    }


@router.post("/runs/{run_id}/complete")
async def complete_run(run_id: str, body: CompleteRunRequest,
                       user: UserContext = Depends(get_current_user)) -> dict:
    enforce_venue_access(body.venue_id)
    db = get_db()
    run = db.get_checklist_run(run_id)
    if not run or run.get("venue_id") != body.venue_id:
        raise HTTPException(status_code=404, detail="Run not found")
    if run.get("status") == "complete":
        raise HTTPException(status_code=409, detail="Run is already complete")

    undone = [i["label"] for i in run.get("items", []) if not i.get("done") and i.get("type") != "note"]
    if undone:
        raise HTTPException(
            status_code=422,
            detail=f"{len(undone)} item(s) not done: " + "; ".join(undone[:5]),
        )

    run["status"] = "complete"
    run["completed_at"] = datetime.utcnow()
    run["completed_by"] = user.user_id
    db.save_checklist_run(run)
    logger.info(f"Checklist complete: {run.get('template_name')} at {body.venue_id} "
                f"by {user.user_id} ({run.get('flags_count', 0)} flags)")
    return {
        "status": "complete",
        "run_id": run_id,
        "flags_count": run.get("flags_count", 0),
        "completed_by": user.user_id,
    }


@router.get("/runs")
async def run_history(
    venue_id: str = Query(...),
    start_date: date = Query(...),
    end_date: date = Query(...),
) -> dict:
    """The compliance evidence trail — every run with its flags preserved."""
    enforce_venue_access(venue_id)
    db = get_db()
    runs = db.list_checklist_runs(venue_id, start_date, end_date) or []
    return {
        "venue_id": venue_id,
        "count": len(runs),
        "total_flags": sum(r.get("flags_count", 0) for r in runs),
        "runs": [{
            "id": r["id"],
            "template_name": r.get("template_name"),
            "run_date": str(r.get("run_date")),
            "status": r.get("status"),
            "completed_by": r.get("completed_by"),
            "completed_at": str(r.get("completed_at")) if r.get("completed_at") else None,
            "flags_count": r.get("flags_count", 0),
            "flagged_items": [
                {"label": i.get("label"), "value": i.get("value"), "note": i.get("note")}
                for i in r.get("items", []) if i.get("flagged")
            ],
        } for r in runs],
    }
