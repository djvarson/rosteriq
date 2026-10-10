"""
Manager tools around leave and shift cover (the request/decide flow itself is
in routes/staff_portal.py):

    GET  /api/leave/impact        -- the person's rostered shifts in a leave
                                     window, in words, for the approve dialog
    POST /api/leave               -- record leave for someone (approved at
                                     once; past dates allowed, e.g. yesterday's
                                     sick day), with an audit trail
    GET  /api/cover/{id}/checks   -- what approving a claimed cover would mean:
                                     trained, hours, visa cap, rest, clashes
"""

import uuid
from datetime import date, datetime
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field

from rosteriq.database import get_db
from rosteriq.middleware.auth import UserContext, get_current_user
from rosteriq.middleware.tenant import enforce_venue_manager, enforce_venue_manager_any
from rosteriq.services.clock import venue_today
from rosteriq.services.events import audit
from rosteriq.services.leave_shifts import (
    LEAVE_TYPE_PATTERN, LEAVE_TYPES, PAID_LEAVE_TYPES, apply_approved_leave,
    default_paid_hours, leave_impact, leave_type_label, opened_summary,
)
from rosteriq.services.roster_edit import RosterBusy

router = APIRouter(tags=["leave-cover"])

_MAX_LEAVE_DAYS = 366


class RecordLeaveBody(BaseModel):
    venue_id: str
    employee_id: str
    start_date: date
    end_date: date
    leave_type: str = Field(..., pattern=LEAVE_TYPE_PATTERN)
    reason: str = Field(default="", max_length=300)
    paid_hours: Optional[float] = Field(default=None, ge=0, le=400)


def _venue_employee(db, venue_id: str, employee_id: str):
    emp = db.get_employee(employee_id)
    if emp is None or getattr(emp, "venue_id", None) != venue_id:
        raise HTTPException(status_code=404, detail="Employee not found")
    return emp


def _check_window(start: date, end: date) -> None:
    if end < start:
        raise HTTPException(status_code=422, detail="end_date must be on or after start_date")
    if (end - start).days >= _MAX_LEAVE_DAYS:
        raise HTTPException(status_code=422, detail="Record leave a year at a time at most")


@router.get("/api/leave/impact")
async def leave_impact_preview(venue_id: str = Query(...), employee_id: str = Query(...),
                               start_date: date = Query(...), end_date: date = Query(...),
                               leave_type: Optional[str] = Query(None, pattern=LEAVE_TYPE_PATTERN)) -> dict:
    enforce_venue_manager_any(venue_id)
    _check_window(start_date, end_date)
    db = get_db()
    emp = _venue_employee(db, venue_id, employee_id)
    impact = leave_impact(db, venue_id, emp, start_date, end_date)
    impact["paid_hours_default"] = default_paid_hours(impact)
    impact["paid_leave"] = (leave_type in PAID_LEAVE_TYPES) if leave_type else None
    impact["leave_types"] = LEAVE_TYPES
    return impact


@router.post("/api/leave")
async def record_leave(body: RecordLeaveBody,
                       user: UserContext = Depends(get_current_user)) -> dict:
    enforce_venue_manager(body.venue_id)
    _check_window(body.start_date, body.end_date)
    db = get_db()
    emp = _venue_employee(db, body.venue_id, body.employee_id)
    for r in db.list_leave_requests(body.venue_id) or []:
        if r.get("employee_id") != emp.id or r.get("status") != "pending":
            continue
        rs = date.fromisoformat(str(r["start_date"])[:10])
        re_ = date.fromisoformat(str(r["end_date"])[:10])
        if rs <= body.end_date and re_ >= body.start_date:
            raise HTTPException(status_code=409, detail=(
                f"{emp.name} already has a pending request for {rs.isoformat()} to "
                f"{re_.isoformat()} — approve or decline that one instead"))
    now = datetime.utcnow()
    req = {
        "id": f"lv-{uuid.uuid4().hex[:10]}",
        "venue_id": body.venue_id,
        "employee_id": emp.id,
        "start_date": body.start_date,
        "end_date": body.end_date,
        "reason": body.reason,
        "leave_type": body.leave_type,
        "paid_hours": body.paid_hours,
        "entered_by": user.user_id,
        "status": "approved",
        "decided_by": user.user_id,
        "decided_at": now,
        "decision_note": "",
        "created_at": now,
    }
    try:
        opened = apply_approved_leave(db, body.venue_id, req, emp)
    except RosterBusy:
        raise HTTPException(status_code=409, detail="Someone else is saving the roster right "
                                                    "now \u2014 try again in a moment.")
    db.save_leave_request(req)
    audit("leave.record", body.venue_id, "leave_request", req["id"],
          employee_id=emp.id, employee=emp.name,
          start_date=body.start_date.isoformat(), end_date=body.end_date.isoformat(),
          leave_type=body.leave_type, paid_hours=req.get("paid_hours"),
          past_dated=body.start_date < venue_today(body.venue_id, db),
          entered_by=user.user_id, opened_shifts=[o["label"] for o in opened],
          reason=body.reason)
    return {
        "status": "approved",
        "request_id": req["id"],
        "leave_type": body.leave_type,
        "leave_type_label": leave_type_label(body.leave_type),
        "paid_hours": req.get("paid_hours"),
        "opened_shifts": opened,
        "message": (f"{leave_type_label(body.leave_type)} recorded for {emp.name}."
                    + (f" {opened_summary(opened)}" if opened else "")),
    }


@router.get("/api/cover/{cover_id}/checks")
async def cover_approval_checks(cover_id: str, venue_id: str = Query(...)) -> dict:
    enforce_venue_manager(venue_id)
    db = get_db()
    cover = db.get_shift_cover(cover_id)
    if not cover or cover.get("venue_id") != venue_id:
        raise HTTPException(status_code=404, detail="Cover request not found")
    if cover.get("status") != "claimed":
        raise HTTPException(status_code=409, detail="Nobody has claimed this shift yet"
                            if cover.get("status") == "open"
                            else f"Request is already {cover.get('status')}")
    from rosteriq.routes.staff_portal import _live_shift, _run_cover_checks
    if _live_shift(db, venue_id, cover) is None:
        raise HTTPException(status_code=409, detail="That shift is no longer on the roster "
                                                    "— decline this request instead.")
    return _run_cover_checks(db, venue_id, cover)
