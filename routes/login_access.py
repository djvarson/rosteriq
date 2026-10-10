"""
Dashboard access per venue, for logins that reach more than one of an
owner's venues (services/venue_access.py holds the storage rules).

    GET  /api/me/access                    -- how the dashboard names me
    GET  /api/employees/{id}/access        -- their access at each venue
    POST /api/employees/{id}/access        -- set it at one venue, or
                                              give access to another venue

Changing a login is account-wide (a password reset reaches every venue it
holds), so every change goes through the same guards as reset links and
access-role: the caller must fully manage every venue the login reaches.
Adding a venue also needs the caller to fully manage that venue. Lowering
the login's access at one venue changes nothing elsewhere, so a full
manager of that venue may do it alone (never to the venue's creator).
"""

import logging
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from rosteriq.database import get_db
from rosteriq.middleware.auth import UserContext, get_current_user
from rosteriq.middleware.tenant import enforce_venue_manager, load_employee_in_scope
from rosteriq.services.events import audit
from rosteriq.services.venue_access import (
    DEPARTMENT, FULL, STAFF, access_map, apply_access_map, fully_managed_group,
    held_venue_ids, role_label, section_options,
)

logger = logging.getLogger(__name__)

router = APIRouter(tags=["login-access"])

_LEVEL_WORDS = {FULL: "a manager", DEPARTMENT: "a department manager", STAFF: "staff"}


def _venue_name(db, venue_id: str) -> str:
    venue = db.get_venue(venue_id)
    return getattr(venue, "name", None) or venue_id


def _has_staff_record(db, venue_id: str, email: str) -> bool:
    email = (email or "").strip().lower()
    return bool(email) and any((getattr(e, "email", "") or "").strip().lower() == email
                               for e in (db.get_employees(venue_id) or []))


def _venues_payload(db, account) -> list:
    out = []
    for vid, a in access_map(account).items():
        out.append({"venue_id": vid, "venue_name": _venue_name(db, vid), "level": a["level"],
                    "sections": a["sections"],
                    "section_options": section_options(db.get_venue(vid))})
    return out


@router.get("/api/me/access")
async def my_access(user: UserContext = Depends(get_current_user)) -> dict:
    """What the dashboard calls this login: 'Owner (The Wembley Hotel,
    Apothecary)' for the owner of the venues, else Manager / Department
    manager / Staff with the venues."""
    from rosteriq.services.demo import is_demo_identity
    db = get_db()
    if is_demo_identity(user.user_id, user.email):
        return {"label": "Demo", "role": user.role, "venues": []}
    account = db.get_user_by_id(user.user_id) or {}
    return {"label": role_label(db, account), "role": account.get("role"),
            "venues": [{k: v for k, v in row.items() if k != "section_options"}
                       for row in _venues_payload(db, account)]}


def _target(db, user: UserContext, employee_id: str, lower_to=None):
    from rosteriq.routes.staff_portal import _account_management_guards
    emp = load_employee_in_scope(db, employee_id)
    enforce_venue_manager(getattr(emp, "venue_id", None))
    account = _account_management_guards(user, emp, db, lower_to=lower_to)
    return emp, account


@router.get("/api/employees/{employee_id}/access")
async def employee_access(employee_id: str, user: UserContext = Depends(get_current_user)) -> dict:
    """The linked login's access at each venue it reaches, and the other
    venues the caller runs that it could be given."""
    db = get_db()
    emp, account = _target(db, user, employee_id)
    held = set(held_venue_ids(account))
    email = account.get("email") or ""
    addable = []
    for vid in fully_managed_group(db, user, emp.venue_id):
        if vid in held:
            continue
        addable.append({"venue_id": vid, "venue_name": _venue_name(db, vid),
                        "section_options": section_options(db.get_venue(vid)),
                        "has_staff_record": _has_staff_record(db, vid, email)})
    return {"employee_id": emp.id, "name": emp.name, "email": email,
            "label": role_label(db, account), "self": account.get("id") == user.user_id,
            "venues": _venues_payload(db, account), "can_add": addable}


class VenueAccessBody(BaseModel):
    venue_id: str = Field(..., min_length=1, max_length=120)
    level: str = Field(..., pattern="^(full|department|staff)$")
    sections: Optional[List[str]] = Field(default=None, max_length=20)


@router.post("/api/employees/{employee_id}/access")
async def set_employee_access(employee_id: str, body: VenueAccessBody,
                              user: UserContext = Depends(get_current_user)) -> dict:
    """Set the login's access at ``venue_id``: one of the venues it already
    reaches, or another venue the caller fully manages ('Also give access
    to Apothecary'). Staff access somewhere new needs a staff record there
    with their email, or there would be nothing for them to see."""
    db = get_db()
    emp, account = _target(db, user, employee_id, lower_to=(body.venue_id, body.level))
    vid = body.venue_id
    enforce_venue_manager(vid)
    if account.get("id") == user.user_id:
        raise HTTPException(status_code=400, detail="You can't change your own access level")
    held = held_venue_ids(account)
    adding = vid not in held
    name = _venue_name(db, vid)
    if adding:
        if vid not in fully_managed_group(db, user, emp.venue_id):
            raise HTTPException(status_code=403, detail="You can only give access to venues you run")
        if body.level == STAFF and not _has_staff_record(db, vid, account.get("email")):
            raise HTTPException(status_code=409, detail=(
                f"{emp.name} isn't on {name}'s staff list with this email yet — add them "
                f"there first, or give manager access instead"))
    sections: List[str] = []
    for s in body.sections or []:
        n = str(s or "").strip().lower()
        if n and n not in sections:
            sections.append(n)
    if body.level == DEPARTMENT and not sections:
        raise HTTPException(status_code=422, detail="Tick at least one department")
    if body.level != DEPARTMENT:
        sections = []
    old_role = account.get("role")
    try:
        apply_access_map(account, {vid: {"level": body.level, "sections": sections}})
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e))
    db.save_user(account)
    after = access_map(account)
    if adding:
        audit("user.venue_grant", vid, "user", account["id"], email=account.get("email"),
              employee_id=emp.id, reason="manager_added", changed_by=user.user_id,
              level=body.level)
    audit("user.role_change", vid, "user", account["id"], email=account.get("email"),
          old_role=old_role, new_role=account.get("role"), changed_by=user.user_id,
          sections=sections or None, reason="venue_access_set",
          venue_access={v: a["level"] for v, a in after.items()})
    what = _LEVEL_WORDS[body.level]
    if body.level == DEPARTMENT:
        what += f" for {', '.join(sections)}"
    msg = (f"{emp.name} {'can now open' if adding else 'is now'} "
           f"{name + ' as ' + what if adding else what + ' at ' + name}"
           f" — takes effect on their next sign-in.")
    return {"employee_id": emp.id, "name": emp.name, "message": msg,
            "label": role_label(db, account), "venues": _venues_payload(db, account)}
