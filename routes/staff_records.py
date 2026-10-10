"""
Staff records after import — the manager's side of keeping the team list true.

    PUT  /employees/{id}                     edit name, contact, rate (with the
                                             date it takes effect), type, level,
                                             areas, hours cap, work rights
    POST /employees/{id}/deactivate          they've left: off every roster,
                                             offer and kiosk; history kept
    POST /employees/{id}/reactivate          they're back
    GET  /api/venues/{id}/join-codes         every unlinked active person's join
                                             code (?format=csv to download)
    GET  /api/venues/{id}/audience-options   the pills for notices and procedures

Every write needs a full manager of the employee's own venue
(enforce_venue_manager); another tenant's id is a 404. The demo identities are
refused by the tenant middleware's /employees/ write deny-list.
"""

import logging
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import Response
from pydantic import BaseModel, Field, ValidationError

from rosteriq.database import get_db
from rosteriq.middleware.auth import get_current_user, UserContext
from rosteriq.middleware.tenant import (
    enforce_venue_manager, enforce_venue_manager_any, load_employee_in_scope,
)
from rosteriq.models import AwardLevel, EmploymentType
from rosteriq.services.events import audit
from rosteriq.services.staff_records import (
    active_only, email_holder, email_taken_message, hourly_rate_problem, is_active,
    is_anonymised, join_code_rows, join_codes_csv, rate_change_entry,
)

logger = logging.getLogger(__name__)

router = APIRouter(tags=["staff-records"])


class StaffEdit(BaseModel):
    """Only the fields sent are changed; send null to clear an optional one."""
    name: Optional[str] = Field(default=None, max_length=120)
    email: Optional[str] = Field(default=None, max_length=254)
    phone: Optional[str] = Field(default=None, max_length=40)
    hourly_base_rate: Optional[Decimal] = None
    rate_effective_from: Optional[date] = None
    employment_type: Optional[EmploymentType] = None
    award_level: Optional[AwardLevel] = None
    skills: Optional[List[str]] = Field(default=None, max_length=30)
    max_hours_per_week: Optional[float] = None
    visa_status: Optional[str] = Field(default=None, max_length=40)
    visa_expiry: Optional[date] = None
    visa_work_limit_fortnight: Optional[float] = None


_FIELD_WORDS = {
    "max_hours_per_week": "Max hours a week must be between 1 and 60",
    "visa_work_limit_fortnight": "The visa hour cap must be between 1 and 152 hours a fortnight",
    "skills": "Areas must be plain names, e.g. Front Bar",
}
_MAX_AREA_NAME = 60


def _load_for_manager(db, employee_id: str):
    emp = load_employee_in_scope(db, employee_id)
    enforce_venue_manager(emp.venue_id)
    return emp


def _refuse_anonymised(emp) -> None:
    if is_anonymised(emp):
        raise HTTPException(
            status_code=409,
            detail="This person's details were removed. If they come back, add them as a new "
                   "staff member.")


async def _forget_cached_lists() -> None:
    # GET /employees is cached for two minutes; the Staff page reloads right
    # after a save and must show the change.
    try:
        from rosteriq.middleware.cache import get_cache_manager
        await get_cache_manager().invalidate_all("employee_lists")
    except Exception:
        pass


def _record_saved(updated, previous) -> None:
    try:
        from rosteriq.api import _audit_employee_saved
        _audit_employee_saved(updated, previous, via="edit")
    except Exception as e:  # noqa: BLE001 — recording never breaks the save
        logger.warning(f"employee edit not audited: {e}")


def _today(db, venue_id):
    from rosteriq.services.clock import venue_today
    return venue_today(venue_id, db)


@router.put("/employees/{employee_id}")
async def edit_employee(employee_id: str, body: StaffEdit,
                        user: UserContext = Depends(get_current_user)) -> dict:
    db = get_db()
    emp = _load_for_manager(db, employee_id)
    _refuse_anonymised(emp)
    sent = body.model_fields_set
    changes, notes = {}, []

    if "name" in sent:
        name = " ".join((body.name or "").split())
        if not name:
            raise HTTPException(status_code=422, detail="Name can't be blank")
        changes["name"] = name
    if "email" in sent:
        email = (body.email or "").strip() or None
        if email and ("@" not in email or " " in email or email.startswith("@") or email.endswith("@")):
            raise HTTPException(status_code=422, detail=f"'{email}' isn't an email address")
        if email:
            other = email_holder(db, emp.venue_id, email, except_id=emp.id)
            if other is not None:
                raise HTTPException(status_code=409, detail=email_taken_message(other, email))
        changes["email"] = email
    if "phone" in sent:
        changes["phone"] = (body.phone or "").strip() or None
    for field in ("employment_type", "award_level"):
        if field in sent and getattr(body, field) is not None:
            changes[field] = getattr(body, field)
    if "max_hours_per_week" in sent and body.max_hours_per_week is not None:
        if not 1 <= body.max_hours_per_week <= 60:
            raise HTTPException(status_code=422, detail=_FIELD_WORDS["max_hours_per_week"])
        changes["max_hours_per_week"] = body.max_hours_per_week
    if "skills" in sent:
        if any(len(str(s or "").strip()) > _MAX_AREA_NAME for s in body.skills or []):
            raise HTTPException(status_code=422, detail=_FIELD_WORDS["skills"])
        changes["skills"] = body.skills or []
    if "visa_work_limit_fortnight" in sent and body.visa_work_limit_fortnight is not None:
        if not 1 <= body.visa_work_limit_fortnight <= 152:
            raise HTTPException(status_code=422, detail=_FIELD_WORDS["visa_work_limit_fortnight"])
    for field in ("visa_status", "visa_expiry", "visa_work_limit_fortnight"):
        if field in sent:
            changes[field] = getattr(body, field) or None

    if "hourly_base_rate" in sent and body.hourly_base_rate is not None:
        problem = hourly_rate_problem(body.hourly_base_rate, getattr(emp, "pay_basis", "hourly"))
        if problem:
            raise HTTPException(status_code=422, detail=problem)
        new_rate = Decimal(str(body.hourly_base_rate)).quantize(Decimal("0.01"))
        old_rate = Decimal(str(emp.hourly_base_rate))
        if new_rate != old_rate:
            today = _today(db, emp.venue_id)
            effective = body.rate_effective_from or today
            if effective > today:
                raise HTTPException(
                    status_code=422,
                    detail="A new rate can't start in the future — RosterIQ prices every shift "
                           "at the current rate. Save the change on the day it starts.")
            changes["hourly_base_rate"] = new_rate
            changes["rate_history"] = list(emp.rate_history or []) + [
                rate_change_entry(old_rate, new_rate, effective, user.email)]
            if effective < today:
                notes.append(
                    f"Backdated to {effective.isoformat()}: RosterIQ prices every shift at the "
                    f"current rate, so check that shifts before then are paid at the old "
                    f"${old_rate:,.2f} (advisor to confirm).")

    if not changes:
        return {"status": "unchanged", "employee": emp, "notes": notes}
    updated = emp.model_copy(deep=True)
    try:
        for field, value in changes.items():
            setattr(updated, field, value)
    except (ValidationError, ValueError) as e:
        errs = e.errors() if isinstance(e, ValidationError) else []
        loc = str(errs[0]["loc"][0]) if errs and errs[0].get("loc") else ""
        raise HTTPException(status_code=422, detail=_FIELD_WORDS.get(loc) or f"Can't save that: {e}")
    updated.updated_at = datetime.utcnow()
    db.save_employee(updated)
    _record_saved(updated, emp)
    await _forget_cached_lists()
    return {"status": "updated", "employee": updated, "notes": notes}


def _upcoming_shifts(db, emp) -> list:
    from rosteriq.services.current_roster import live_shifts
    today = _today(db, emp.venue_id)
    out = []
    try:
        for s in live_shifts(db, emp.venue_id, today, today + timedelta(days=28)):
            if str(getattr(s, "employee_id", "")) != str(emp.id):
                continue
            if str(getattr(s.status, "value", s.status)) in ("cancelled", "completed", "no_show"):
                continue
            out.append({"shift_id": s.id, "date": s.date.isoformat(),
                        "start": s.start_time.strftime("%H:%M"),
                        "end": s.end_time.strftime("%H:%M"), "role": s.role})
    except Exception as e:  # noqa: BLE001
        logger.warning(f"upcoming shifts lookup failed for {emp.id}: {e}")
    out.sort(key=lambda x: (x["date"], x["start"]))
    return out


async def _set_active(employee_id: str, active: bool) -> dict:
    db = get_db()
    emp = _load_for_manager(db, employee_id)
    if active:
        _refuse_anonymised(emp)
    changed = is_active(emp) != active
    if changed:
        updated = emp.model_copy(deep=True)
        updated.active = active
        updated.updated_at = datetime.utcnow()
        db.save_employee(updated)
        await _forget_cached_lists()
    upcoming = _upcoming_shifts(db, emp) if not active else []
    if changed:
        audit("employee.deactivate" if not active else "employee.reactivate",
              emp.venue_id, "employee", emp.id, name=emp.name,
              upcoming_shifts=len(upcoming) if not active else None)
    if active:
        message = f"{emp.name} is active again and can be rostered."
    elif upcoming:
        message = (f"{emp.name} is inactive. They're still on {len(upcoming)} upcoming "
                   f"shift{'s' if len(upcoming) != 1 else ''} — give those to someone else.")
    else:
        message = f"{emp.name} is inactive: off rosters, cover and the kiosk. Their history is kept."
    return {"status": "active" if active else "inactive", "changed": changed,
            "employee_id": emp.id, "name": emp.name, "upcoming_shifts": upcoming,
            "message": message}


@router.post("/employees/{employee_id}/deactivate")
async def deactivate_employee(employee_id: str) -> dict:
    return await _set_active(employee_id, False)


@router.post("/employees/{employee_id}/reactivate")
async def reactivate_employee(employee_id: str) -> dict:
    return await _set_active(employee_id, True)


@router.get("/api/venues/{venue_id}/join-codes")
async def venue_join_codes(venue_id: str,
                           format: str = Query("json", pattern="^(json|csv)$")):
    """Everyone active here with no linked login yet, with their join code,
    for handing out in bulk (download or print)."""
    enforce_venue_manager(venue_id)
    from rosteriq.routes.auth import public_origin
    db = get_db()
    rows = join_code_rows(db, venue_id, public_origin())
    audit("staff.join_codes_export", venue_id, "venue", venue_id, people=len(rows), format=format)
    if format == "csv":
        return Response(content=join_codes_csv(rows), media_type="text/csv; charset=utf-8",
                        headers={"Content-Disposition": 'attachment; filename="join-codes.csv"'})
    return {"venue_id": venue_id, "count": len(rows), "people": rows,
            "register_url": rows[0]["link"] if rows else f"{public_origin()}/register?join=1"}


@router.get("/api/venues/{venue_id}/audience-options")
async def audience_options(venue_id: str) -> dict:
    """Pills for 'Send to' and 'Applies to': the venue's areas, the skills its
    current staff hold, and 'All <word>' groups."""
    enforce_venue_manager_any(venue_id)
    from rosteriq.services.audience import options
    db = get_db()
    staff = active_only(db.get_employees(venue_id))
    return {"venue_id": venue_id, "options": options(db.get_venue(venue_id), staff)}
