"""
SOP / JSP document library — the venue's procedures, safe-work procedures and
policies in one place, targeted at roles, read and acknowledged by staff.

Managers write or paste a procedure (text-first; no file upload yet), pick a
category, optionally target it at roles, and publish it. Staff see the
procedures that apply to them in /my and tap "I've read this". Every edit to
the title or body bumps the version, which resets acknowledgements — so a
manager can always answer "who has read the CURRENT version?".

Manager routes (owner / manager / admin, venue-scoped):
    POST /api/sops/documents          -- publish a document (version 1)
    PUT  /api/sops/documents/{id}     -- edit; title/body change bumps version
    GET  /api/sops/documents?venue_id -- library + ack_stats (who is outstanding)
    POST /api/sops/seed               -- idempotent AU-hospo starter set (4 docs)

Staff routes (email-linked identity, same as the rest of the portal):
    GET  /api/me/sops                     -- active docs that apply to me + acked?
    POST /api/me/sops/{id}/acknowledge    -- acknowledge the current version

Applicability: a document with an empty ``applies_to`` applies to everyone;
otherwise it applies to any employee whose ``skills`` intersect the roles
listed (case-insensitive).
"""

import logging
import re
import uuid
from datetime import datetime
from typing import List, Literal, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field, field_validator

from rosteriq.database import get_db
from rosteriq.middleware.auth import get_current_user, UserContext
from rosteriq.middleware.tenant import enforce_venue_access
from rosteriq.routes.staff_portal import _linked_employee, _no_link_response

logger = logging.getLogger(__name__)

router = APIRouter(tags=["sops"])

CATEGORIES = ("sop", "jsp", "policy", "other")
_MANAGER_ROLES = ("owner", "manager", "admin")
MAX_BODY_CHARS = 20000


def _strip_nul(v):
    """Postgres TEXT/JSONB reject NUL bytes (\\x00) — drop them before
    validation so a pasted procedure can never 500 the write."""
    if isinstance(v, str):
        return v.replace("\x00", "")
    if isinstance(v, list):
        return [x.replace("\x00", "") if isinstance(x, str) else x for x in v]
    return v


class SopCreateBody(BaseModel):
    venue_id: str
    title: str = Field(..., min_length=1, max_length=160)
    category: Literal["sop", "jsp", "policy", "other"] = "sop"
    body: str = Field(..., min_length=1, max_length=MAX_BODY_CHARS)
    applies_to: List[str] = Field(default_factory=list, max_length=30,
                                  description="Role/skill names; empty = everyone")
    requires_ack: bool = True

    @field_validator("title", "body", "applies_to", mode="before")
    @classmethod
    def _no_nul(cls, v):
        return _strip_nul(v)


class SopUpdateBody(BaseModel):
    title: Optional[str] = Field(default=None, min_length=1, max_length=160)
    category: Optional[Literal["sop", "jsp", "policy", "other"]] = None
    body: Optional[str] = Field(default=None, min_length=1, max_length=MAX_BODY_CHARS)
    applies_to: Optional[List[str]] = Field(default=None, max_length=30)
    requires_ack: Optional[bool] = None
    active: Optional[bool] = None

    @field_validator("title", "body", "applies_to", mode="before")
    @classmethod
    def _no_nul(cls, v):
        return _strip_nul(v)


class SeedBody(BaseModel):
    venue_id: str


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _clean_roles(roles) -> list:
    out = []
    for r in roles or []:
        r = str(r or "").strip()
        if r and r.lower() not in [o.lower() for o in out]:
            out.append(r)
    return out


def _require_manager(user: UserContext, venue_id: str) -> None:
    enforce_venue_access(venue_id)
    if user.role not in _MANAGER_ROLES:
        raise HTTPException(
            status_code=403,
            detail="Only managers can publish or edit procedures. Read and "
                   "acknowledge yours in the SOPs tab of /my instead.",
        )


def _require_manager_for_doc(user: UserContext, doc: dict) -> None:
    """By-id routes: a caller WITHOUT access to the document's venue gets the
    same 404 as an unknown id (the id must not be an oracle for another
    venue's library). Same-venue non-managers still get the 403 that names
    the fix."""
    try:
        enforce_venue_access(doc.get("venue_id"))
    except HTTPException:
        raise HTTPException(status_code=404, detail="Procedure not found")
    _require_manager(user, doc.get("venue_id"))


def _applies_to_employee(doc: dict, emp) -> bool:
    roles = [str(r).strip().lower() for r in (doc.get("applies_to") or []) if str(r).strip()]
    if not roles:
        return True
    skills = {str(s).strip().lower() for s in (getattr(emp, "skills", None) or [])}
    return any(r in skills for r in roles)


def _doc_payload(d: dict) -> dict:
    return {
        "id": d["id"],
        "venue_id": d.get("venue_id"),
        "title": d.get("title"),
        "category": d.get("category") or "sop",
        "body": d.get("body") or "",
        "applies_to": list(d.get("applies_to") or []),
        "version": int(d.get("version") or 1),
        "requires_ack": bool(d.get("requires_ack", True)),
        "active": bool(d.get("active", True)),
        "author_name": d.get("author_name"),
        "created_at": str(d.get("created_at")) if d.get("created_at") else None,
        "updated_at": str(d.get("updated_at")) if d.get("updated_at") else None,
    }


def _ack_stats(doc: dict, staff: list, acks: list) -> dict:
    """Who still needs to read the CURRENT version of ``doc``."""
    version = int(doc.get("version") or 1)
    current = {
        a.get("employee_id") for a in acks
        if a.get("doc_id") == doc["id"] and int(a.get("doc_version") or 0) == version
    }
    applicable = [e for e in staff if _applies_to_employee(doc, e)]
    acked = [e for e in applicable if e.id in current]
    outstanding = sorted(e.name for e in applicable if e.id not in current)
    if not doc.get("requires_ack", True):
        # Reference-only document: nobody is "outstanding".
        return {"required": 0, "applicable": len(applicable),
                "acknowledged_current_version": len(acked),
                "outstanding_names": []}
    return {
        "required": len(applicable),
        "applicable": len(applicable),
        "acknowledged_current_version": len(acked),
        "outstanding_names": outstanding,
    }


def _find_ack(acks: list, doc_id: str, version: int, employee_id: str):
    for a in acks:
        if (a.get("doc_id") == doc_id and int(a.get("doc_version") or 0) == int(version)
                and a.get("employee_id") == employee_id):
            return a
    return None


# ---------------------------------------------------------------------------
# Starter set — sensible AU hospitality defaults a manager can edit in place
# ---------------------------------------------------------------------------

STARTER_SOPS = [
    {
        "title": "Opening & closing procedure",
        "category": "sop",
        "applies_to": [],
        "body": (
            "OPENING (first person on site)\n"
            "1. Disarm the alarm and unlock the front door only when you are ready to trade. "
            "Turn on lights, ventilation and the coffee machine; let the machine reach temperature "
            "before pulling the first shot.\n"
            "2. Check the fridge and freezer temperature log. Fridges must read 5°C or below and "
            "freezers -18°C or below. Record readings on the temp log; report anything out of range "
            "to the manager on duty before stock is used.\n"
            "3. Count the till float and sign the float sheet. Do not start trading with an "
            "unverified float.\n"
            "4. Walk the floor: chairs down, tables wiped and set, menus clean, toilets checked and "
            "stocked, bins emptied, specials board updated.\n"
            "5. Check the day's roster and any handover notes in RosterIQ. Confirm deliveries "
            "expected today.\n\n"
            "CLOSING (last person on site)\n"
            "1. Last orders called as per licence conditions. Do not serve after close.\n"
            "2. Cash up: count each till, complete the end-of-day report, note any variance and "
            "secure cash in the safe. Two people present where practicable.\n"
            "3. Clean and sanitise all food-contact surfaces, empty and clean bins, sweep and mop. "
            "Run the dishwasher's final cycle and leave the door open to dry.\n"
            "4. Cover, date and refrigerate all open food. Log closing fridge temperatures.\n"
            "5. Turn off gas, fryers, hot plates and the coffee machine (leave fridges running). "
            "Check toilets and the back area for anyone remaining.\n"
            "6. Lock all doors and windows, set the alarm and leave together — never close alone "
            "if a second staff member is rostered.\n"
            "Report any incident, breakage or near-miss to the manager before leaving."
        ),
    },
    {
        "title": "Safe manual handling",
        "category": "jsp",
        "applies_to": [],
        "body": (
            "This job safety procedure covers lifting, carrying and moving stock, kegs, furniture "
            "and deliveries. Manual handling injuries are the most common hospitality injury and "
            "almost all are preventable.\n\n"
            "BEFORE YOU LIFT\n"
            "- Size up the load. If it is awkward, heavier than about 15-20 kg or you are unsure, "
            "get help or use the trolley. Kegs (approx. 60 kg full) are always a two-person lift "
            "or a keg trolley job — never lift a full keg alone.\n"
            "- Clear the path. Look for wet floors, steps, cords and stacked boxes.\n"
            "- Wear closed, non-slip footwear at all times on shift.\n\n"
            "LIFTING TECHNIQUE\n"
            "- Stand close to the load, feet shoulder-width apart, one foot slightly forward.\n"
            "- Bend your knees and hips, keep your back straight and head up.\n"
            "- Grip firmly and lift smoothly with your legs. Keep the load close to your body.\n"
            "- Never twist while lifting — turn with your feet.\n"
            "- To put the load down, reverse the lift; do not drop it.\n\n"
            "REPETITIVE TASKS\n"
            "- Rotate heavy or repetitive tasks (unloading a delivery, restocking the cool room, "
            "glass racks) between team members.\n"
            "- Store heavy items between knee and shoulder height; light items up high or low.\n"
            "- Take micro-breaks; stretch shoulders, wrists and back.\n\n"
            "IF SOMETHING GOES WRONG\n"
            "Stop, report any pain, strain or near-miss to the manager on duty straight away and "
            "record it in the incident register. Early reporting means faster recovery. Under WHS "
            "law you have the right to refuse a lift you believe is unsafe — ask for help."
        ),
    },
    {
        "title": "Cash handling & till variance",
        "category": "policy",
        "applies_to": [],
        "body": (
            "PURPOSE\n"
            "To protect staff and the business by handling cash consistently and being able to "
            "explain every dollar of variance.\n\n"
            "FLOATS AND TILLS\n"
            "- Each till starts the day with a counted float, signed for by the operator and a "
            "manager. Only staff logged in to that till operate it.\n"
            "- Keep the drawer closed between transactions. Count change back to the customer.\n"
            "- Notes of $50 and above are checked before acceptance. If unsure, ask a manager.\n"
            "- Do not accept personal cheques or IOUs. Staff purchases go through the till like any "
            "other sale with the staff discount applied by a manager.\n\n"
            "SKIMS AND END OF DAY\n"
            "- When a till exceeds the skim limit set by the manager, excess notes are removed, "
            "counted by two people, recorded and secured in the safe.\n"
            "- At close, count each till against the end-of-day report. Record cash, EFTPOS, "
            "vouchers and any refunds or voids separately.\n\n"
            "VARIANCE\n"
            "- A variance up to $10 is recorded and monitored. Anything above $10, or a pattern "
            "of small variances, is reported to the venue manager the same night and investigated.\n"
            "- Never adjust a till or 'top up' a shortfall from your own money — the record must "
            "show what actually happened.\n"
            "- Repeated unexplained variances may result in disciplinary action; honest mistakes "
            "reported promptly are treated as training opportunities.\n\n"
            "SECURITY\n"
            "- Never count cash in view of customers. Bank runs vary in time and route.\n"
            "- In a robbery, comply, stay calm, do not resist and call 000 once safe."
        ),
    },
    {
        "title": "Food safety & allergen declaration",
        "category": "sop",
        "applies_to": [],
        "body": (
            "Every staff member who prepares, handles or serves food or drink is responsible for "
            "food safety under the Food Standards Code (Standard 3.2.2A). This applies to the floor "
            "and bar as much as the kitchen.\n\n"
            "PERSONAL HYGIENE\n"
            "- Wash hands with soap and warm water for at least 20 seconds before starting work, "
            "after breaks, after handling raw food, rubbish or money, and after using the toilet.\n"
            "- Do not work with food while ill with vomiting or diarrhoea; report symptoms and stay "
            "away for 48 hours after they stop.\n"
            "- Cover cuts with a blue waterproof dressing and a glove. Tie hair back; no jewellery on "
            "hands or wrists other than a plain band.\n\n"
            "TEMPERATURE CONTROL\n"
            "- Keep cold food at 5°C or below and hot food at 60°C or above. Use the 2-hour/4-hour "
            "rule for anything in between: under 2 hours refrigerate or use; 2-4 hours use "
            "immediately; over 4 hours discard.\n"
            "- Log fridge and cool room temperatures at open and close. Probe-check reheated food "
            "reaches 75°C.\n"
            "- Label all prepared and decanted food with the item, date made and use-by.\n\n"
            "CROSS-CONTAMINATION\n"
            "- Raw meat below ready-to-eat food in fridges. Colour-coded boards and separate "
            "utensils. Sanitise benches between tasks.\n\n"
            "ALLERGEN DECLARATION\n"
            "- When a customer declares an allergy or intolerance, stop and tell the chef in "
            "charge before the order is placed. Never guess or say 'it should be fine'.\n"
            "- Check the recipe/allergen matrix for the 10 priority allergens: peanuts, tree nuts, "
            "milk, egg, wheat/gluten, soy, fish, crustacea, sesame and lupin (plus sulphites).\n"
            "- Allergen orders are flagged on the docket, prepared with clean equipment and gloves, "
            "and delivered to the table by the person who took the order.\n"
            "- If in doubt, the answer is: we cannot guarantee that dish is safe."
        ),
    },
]


def _starter_doc_id(venue_id: str, title: str) -> str:
    """Stable id for a starter document at a venue (see seed_starter_sops)."""
    import hashlib
    slug = re.sub(r"[^a-z0-9]+", "-", title.lower()).strip("-")[:32]
    h = hashlib.sha1(f"{venue_id}|{title.lower()}".encode()).hexdigest()[:8]
    return f"sop-{slug}-{h}"


def seed_starter_sops(db, venue_id: str, author_name: str = "RosterIQ",
                      now: Optional[datetime] = None) -> dict:
    """Idempotently seed the starter procedures for a venue.

    Titles that already exist (any case, active or retired) are skipped, so a
    manager who edited "Cash handling & till variance" never gets a duplicate.
    Returns {"created": [ids], "skipped": [titles]}.

    Race-proof: ids are DETERMINISTIC per venue + starter (the app boots with
    two uvicorn workers that both seed the demo at once — random ids let both
    slip past the "nothing here yet" check and produced two copies of every
    starter on production). Same id -> the store's upsert makes concurrent
    seeds converge on one row. An id-exists check also protects a doc a
    manager RENAMED from being overwritten back to starter content.
    """
    now = now or datetime.utcnow()
    existing_titles = {
        str(d.get("title") or "").strip().lower()
        for d in (db.list_sop_documents(venue_id, include_inactive=True) or [])
    }
    created, skipped = [], []
    for spec in STARTER_SOPS:
        doc_id = _starter_doc_id(venue_id, spec["title"])
        if spec["title"].strip().lower() in existing_titles or db.get_sop_document(doc_id):
            skipped.append(spec["title"])
            continue
        doc = {
            "id": doc_id,
            "venue_id": venue_id,
            "title": spec["title"],
            "category": spec["category"],
            "body": spec["body"],
            "applies_to": list(spec.get("applies_to") or []),
            "version": 1,
            "requires_ack": True,
            "active": True,
            "author_name": author_name,
            "created_at": now,
            "updated_at": now,
        }
        db.save_sop_document(doc)
        created.append(doc["id"])
        existing_titles.add(spec["title"].strip().lower())
    return {"created": created, "skipped": skipped}


# ---------------------------------------------------------------------------
# Manager-facing
# ---------------------------------------------------------------------------

@router.post("/api/sops/documents")
async def create_sop_document(body: SopCreateBody,
                              user: UserContext = Depends(get_current_user)) -> dict:
    _require_manager(user, body.venue_id)
    if not body.body.strip():
        raise HTTPException(status_code=422, detail="Procedure body cannot be blank")
    db = get_db()
    now = datetime.utcnow()
    doc = {
        "id": f"sop-{uuid.uuid4().hex[:10]}",
        "venue_id": body.venue_id,
        "title": body.title.strip(),
        "category": body.category,
        "body": body.body.strip(),
        "applies_to": _clean_roles(body.applies_to),
        "version": 1,
        "requires_ack": bool(body.requires_ack),
        "active": True,
        "author_name": user.name or user.email,
        "created_at": now,
        "updated_at": now,
    }
    db.save_sop_document(doc)
    logger.info(f"SOP published at {body.venue_id}: {doc['title']!r} ({doc['category']})")
    return {"status": "published", "document_id": doc["id"], "document": _doc_payload(doc)}


@router.delete("/api/sops/documents/{doc_id}")
async def delete_sop_document(doc_id: str,
                              user: UserContext = Depends(get_current_user)) -> dict:
    """Hard-delete a procedure that nobody has ever acknowledged (a draft, a
    duplicate, a mistake). Once staff have signed off on any version it is
    part of the compliance record — retire it (PUT active=false) instead."""
    db = get_db()
    doc = db.get_sop_document(doc_id)
    if not doc:
        raise HTTPException(status_code=404, detail="Procedure not found")
    _require_manager_for_doc(user, doc)
    if db.list_sop_acks(doc.get("venue_id"), doc_id=doc_id):
        raise HTTPException(
            status_code=409,
            detail="Staff have acknowledged this procedure, so it is part of the "
                   "compliance record — retire it instead of deleting it.")
    db.delete_sop_document(doc_id)
    logger.info(f"SOP {doc_id} deleted at {doc.get('venue_id')} (no acknowledgements)")
    return {"status": "deleted", "document_id": doc_id}


@router.put("/api/sops/documents/{doc_id}")
async def update_sop_document(doc_id: str, body: SopUpdateBody,
                              user: UserContext = Depends(get_current_user)) -> dict:
    db = get_db()
    doc = db.get_sop_document(doc_id)
    if not doc:
        raise HTTPException(status_code=404, detail="Procedure not found")
    _require_manager_for_doc(user, doc)

    bump = False
    if body.title is not None and body.title.strip() != (doc.get("title") or ""):
        doc["title"] = body.title.strip()
        bump = True
    if body.body is not None:
        # Compare stripped-to-stripped: the editor round-trips the body with
        # trailing whitespace, so an applies_to-only save must never look
        # like a body edit (which would bump the version and reset acks).
        new_body = body.body.strip()
        if not new_body:
            raise HTTPException(status_code=422, detail="Procedure body cannot be blank")
        if new_body != (doc.get("body") or "").strip():
            doc["body"] = new_body
            bump = True
    if body.category is not None:
        doc["category"] = body.category
    if body.applies_to is not None:
        doc["applies_to"] = _clean_roles(body.applies_to)
    if body.requires_ack is not None:
        doc["requires_ack"] = bool(body.requires_ack)
    if body.active is not None:
        doc["active"] = bool(body.active)
    if bump:
        # A changed procedure must be re-read: new version, acks start again.
        doc["version"] = int(doc.get("version") or 1) + 1
    doc["updated_at"] = datetime.utcnow()
    db.save_sop_document(doc)
    payload = _doc_payload(doc)
    payload["status"] = "updated"
    payload["version_bumped"] = bump
    return payload


@router.get("/api/sops/documents")
async def list_sop_documents(venue_id: str = Query(...),
                             include_inactive: bool = Query(False),
                             user: UserContext = Depends(get_current_user)) -> dict:
    _require_manager(user, venue_id)
    db = get_db()
    staff = db.get_employees(venue_id) or []
    docs = db.list_sop_documents(venue_id, include_inactive=include_inactive) or []
    acks = db.list_sop_acks(venue_id) or []
    acked_doc_ids = {a.get("doc_id") for a in acks}
    out = []
    for d in docs:
        p = _doc_payload(d)
        p["ack_stats"] = _ack_stats(d, staff, acks)
        # Any ack on ANY version makes the doc part of the compliance record:
        # it can be retired but not deleted (mirrors DELETE's 409 rule), so
        # the UI can show Delete vs Retire without a round trip.
        has_any_ack = d["id"] in acked_doc_ids
        p["has_any_ack"] = has_any_ack
        p["deletable"] = not has_any_ack
        out.append(p)
    return {"venue_id": venue_id, "count": len(out), "staff_count": len(staff),
            "documents": out}


@router.post("/api/sops/seed")
async def seed_sops(body: SeedBody, user: UserContext = Depends(get_current_user)) -> dict:
    _require_manager(user, body.venue_id)
    db = get_db()
    result = seed_starter_sops(db, body.venue_id, author_name=user.name or user.email)
    return {
        "status": "seeded",
        "created": len(result["created"]),
        "skipped": len(result["skipped"]),
        "document_ids": result["created"],
        "skipped_titles": result["skipped"],
    }


# ---------------------------------------------------------------------------
# Staff-facing
# ---------------------------------------------------------------------------

@router.get("/api/me/sops")
async def my_sops(user: UserContext = Depends(get_current_user)) -> dict:
    db = get_db()
    emp, vid = _linked_employee(db, user)
    if not emp:
        return _no_link_response(user)
    docs = db.list_sop_documents(vid) or []
    acks = db.list_sop_acks(vid) or []
    out = []
    outstanding = 0
    for d in docs:
        if not d.get("active", True) or not _applies_to_employee(d, emp):
            continue
        ack = _find_ack(acks, d["id"], int(d.get("version") or 1), emp.id)
        p = _doc_payload(d)
        p["acknowledged"] = ack is not None
        p["acknowledged_at"] = str(ack.get("acknowledged_at")) if ack else None
        if p["requires_ack"] and not p["acknowledged"]:
            outstanding += 1
        out.append(p)
    return {"linked": True, "count": len(out), "outstanding": outstanding, "documents": out}


@router.post("/api/me/sops/{doc_id}/acknowledge")
async def acknowledge_sop(doc_id: str, user: UserContext = Depends(get_current_user)) -> dict:
    db = get_db()
    emp, vid = _linked_employee(db, user)
    if not emp:
        raise HTTPException(status_code=409, detail=_no_link_response(user)["message"])
    doc = db.get_sop_document(doc_id)
    if not doc or doc.get("venue_id") != vid:
        raise HTTPException(status_code=404, detail="Procedure not found")
    if not doc.get("active", True):
        raise HTTPException(
            status_code=404,
            detail="This procedure has been retired and no longer needs acknowledging.",
        )
    if not _applies_to_employee(doc, emp):
        raise HTTPException(
            status_code=403,
            detail="This procedure does not apply to your role, so there is nothing to "
                   "acknowledge. Ask a manager if you think it should.",
        )
    version = int(doc.get("version") or 1)
    existing = _find_ack(db.list_sop_acks(vid, doc_id) or [], doc_id, version, emp.id)
    if existing:
        return {"status": "acknowledged", "already": True, "document_id": doc_id,
                "version": version, "acknowledged_at": str(existing.get("acknowledged_at"))}
    ack = {
        "id": f"ack-{uuid.uuid4().hex[:10]}",
        "venue_id": vid,
        "doc_id": doc_id,
        "doc_version": version,
        "employee_id": emp.id,
        "employee_name": emp.name,
        "acknowledged_at": datetime.utcnow(),
    }
    db.save_sop_ack(ack)
    return {"status": "acknowledged", "already": False, "document_id": doc_id,
            "version": version, "acknowledged_at": str(ack["acknowledged_at"])}
