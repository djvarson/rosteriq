"""
Manager tools around leave and shift cover (the request/decide flow itself is
in routes/staff_portal.py):

    GET  /api/leave/impact        -- the person's rostered shifts in a leave
                                     window, in words, for the approve dialog
    POST /api/leave               -- record leave for someone (approved at
                                     once; past dates allowed, e.g. yesterday's
                                     sick day), with an audit trail
    POST /api/leave/{id}/amend    -- cancel approved leave, shorten it to dates
                                     inside it, change its paid hours, or mark
                                     the days of it the person also works part of
    GET  /api/cover/{id}/checks   -- what approving a claimed cover would mean:
                                     trained, hours, visa cap, rest, clashes
"""

import uuid
from datetime import date, datetime, timedelta
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field

from rosteriq.database import get_db
from rosteriq.middleware.auth import UserContext, get_current_user
from rosteriq.middleware.tenant import enforce_venue_manager, enforce_venue_manager_any
from rosteriq.services.clock import venue_today
from rosteriq.services.current_roster import live_shifts
from rosteriq.services.events import audit
from rosteriq.models import normalise_area
from rosteriq.services.leave_shifts import (
    LEAVE_TYPE_PATTERN, LEAVE_TYPES, PAID_LEAVE_TYPES, apply_approved_leave, cover_day,
    default_paid_hours, is_open, leave_hours_in_period, leave_impact, leave_type_label,
    opened_summary, shift_label,
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
    # Two records for the same days can both be right (half annual, half personal).
    allow_overlap: bool = False


class AmendLeaveBody(BaseModel):
    venue_id: str
    cancel: bool = False
    start_date: Optional[date] = None
    end_date: Optional[date] = None
    paid_hours: Optional[float] = Field(default=None, ge=0, le=400)
    # Every day of the leave they also work part of ([] for none): they can be
    # rostered then, and the leave keeps its paid hours.
    working_days: Optional[List[date]] = Field(default=None, max_length=_MAX_LEAVE_DAYS)
    note: str = Field(default="", max_length=300)


def _day(v) -> date:
    if isinstance(v, datetime):
        return v.date()
    return v if isinstance(v, date) else date.fromisoformat(str(v)[:10])


def _hours_text(h) -> str:
    return "none" if h is None else f"{float(h):g}h"


def approved_overlap(db, venue_id: str, employee_id: str, start: date, end: date,
                     exclude_id: Optional[str] = None) -> Optional[dict]:
    """The person's approved leave at the venue that overlaps start..end."""
    for r in db.list_leave_requests(venue_id) or []:
        if (r.get("employee_id") != employee_id or r.get("status") != "approved"
                or r.get("id") == exclude_id):
            continue
        if _day(r["start_date"]) <= end and _day(r["end_date"]) >= start:
            return r
    return None


def overlap_refusal(name: str, r: dict) -> HTTPException:
    what = leave_type_label(r.get("leave_type"))
    if r.get("paid_hours") is not None:
        what += f", {_hours_text(r['paid_hours'])}"
    first, last = _day(r["start_date"]).isoformat(), _day(r["end_date"]).isoformat()
    return HTTPException(status_code=409, detail={
        "code": "LEAVE_OVERLAP",
        "message": (f"{name} already has approved leave for {first} to {last} ({what}) \u2014 "
                    "it's on the Leave page under Approved. Change or cancel that one, or "
                    "record both if both are right (say, half annual and half personal) \u2014 "
                    "each record then needs its own share of the hours."),
    })


def overlap_paid_hours(db, venue_id: str, employee, req: dict) -> str:
    """Paid leave recorded or approved over the person's other approved leave
    with no hours given: the rostered hours its days keep them from working,
    less the hours the other records already have for those days, so the
    absence isn't counted twice. Sets req["paid_hours"]; returns a note."""
    if req.get("leave_type") not in PAID_LEAVE_TYPES or req.get("paid_hours") is not None:
        return ""
    first, last = _day(req["start_date"]), _day(req["end_date"])
    others = [r for r in db.list_leave_requests(venue_id) or []
              if r.get("employee_id") == employee.id and r.get("status") == "approved"
              and r.get("id") != req.get("id")
              and _day(r["start_date"]) <= last and _day(r["end_date"]) >= first]
    if not others:
        return ""
    impact = leave_impact(db, venue_id, employee, first, last)
    seen = {_slot_key(s["date"], s["start"], s["end"], s["role"]) for s in impact["shifts"]}
    rostered = float(impact["leave_hours"] or 0)
    for r in others:
        for o in r.get("opened_shifts") or []:
            key = _slot_key(o.get("date"), o.get("start"), o.get("end"), o.get("role"))
            if first.isoformat() <= key[0] <= last.isoformat() and key not in seen:
                seen.add(key)
                rostered += float(o.get("hours") or 0)
    if not rostered:
        return ""
    had = sum(float(leave_hours_in_period(r, first, last) or 0) for r in others
              if r.get("leave_type") in PAID_LEAVE_TYPES)
    req["paid_hours"] = round(max(0.0, rostered - had), 2)
    return (f" Paid hours: {_hours_text(req['paid_hours'])} \u2014 the {_hours_text(rostered)} "
            f"rostered those days less the {_hours_text(had)} already on their other leave; "
            "change either record's hours if they're split differently.")


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
    if not body.allow_overlap:
        clash = approved_overlap(db, body.venue_id, emp.id, body.start_date, body.end_date)
        if clash is not None:
            raise overlap_refusal(emp.name, clash)
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
    hours_note = overlap_paid_hours(db, body.venue_id, emp, req) if body.allow_overlap else ""
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
                    + (f" {opened_summary(opened)}" if opened else "") + hours_note),
    }


def _slot_key(date_iso, start, end, role) -> tuple:
    return (str(date_iso)[:10], str(start)[:5], str(end)[:5], normalise_area(role))


def _hours_off(db, venue_id: str, employee, req: dict, first: date, last: date) -> Optional[float]:
    """What default_paid_hours gives for first..last now that the leave has
    opened the person's shifts: the shifts it opened on those days plus the
    unworked part of the ones that had started (still theirs)."""
    impact = leave_impact(db, venue_id, employee, first, last)
    held = {_slot_key(s["date"], s["start"], s["end"], s["role"]) for s in impact["shifts"]}
    hours = float(impact["leave_hours"] or 0)
    for o in req.get("opened_shifts") or []:
        key = _slot_key(o.get("date"), o.get("start"), o.get("end"), o.get("role"))
        if first.isoformat() <= key[0] <= last.isoformat() and key not in held:
            hours += float(o.get("hours") or 0)
    return default_paid_hours({"leave_hours": hours})


def _same_hours(a, b) -> bool:
    if a is None or b is None:
        return a is None and b is None
    return abs(float(a) - float(b)) < 0.005


def _day_text(d: date) -> str:
    return f"{d:%a} {d.day} {d:%b}"


@router.post("/api/leave/{req_id}/amend")
async def amend_leave(req_id: str, body: AmendLeaveBody,
                      user: UserContext = Depends(get_current_user)) -> dict:
    """Cancel approved leave, shorten it to dates inside it, change its paid
    hours, or set the days of it the person also works part of. The shifts it
    left open on the days released stay open (the manager puts the person
    back on); the covers still open for those days are withdrawn. A day they
    work part of keeps its leave, its paid hours and its open shifts."""
    enforce_venue_manager(body.venue_id)
    db = get_db()
    stored = db.get_leave_request(req_id)
    if not stored or stored.get("venue_id") != body.venue_id:
        raise HTTPException(status_code=404, detail="Leave request not found")
    if stored.get("status") != "approved":
        raise HTTPException(status_code=409, detail=f"Only approved leave can be changed \u2014 "
                                                    f"this request is {stored.get('status')}")
    req = dict(stored)
    old_start, old_end = _day(req["start_date"]), _day(req["end_date"])
    old_working = sorted({_day(d) for d in req.get("working_days") or []})
    emp_id = req.get("employee_id")
    emp = db.get_employee(emp_id)
    if emp is not None and getattr(emp, "venue_id", None) != body.venue_id:
        emp = None
    name = getattr(emp, "name", None) or "This person"
    label = leave_type_label(req.get("leave_type"))
    paid = req.get("leave_type") in PAID_LEAVE_TYPES
    old_hours = req.get("paid_hours")
    new_start = new_end = None
    working = old_working
    if not body.cancel:
        new_start, new_end = body.start_date or old_start, body.end_date or old_end
        _check_window(new_start, new_end)
        if new_start < old_start or new_end > old_end:
            raise HTTPException(status_code=422, detail=(
                f"The new dates must be inside the leave ({old_start.isoformat()} to "
                f"{old_end.isoformat()}) \u2014 record any extra days as new leave"))
        if body.working_days is None:
            working = [d for d in old_working if new_start <= d <= new_end]
        else:
            outside = sorted(d for d in body.working_days if not new_start <= d <= new_end)
            if outside:
                raise HTTPException(status_code=422, detail=(
                    f"{outside[0].isoformat()} isn't a day of the leave ({new_start.isoformat()} "
                    f"to {new_end.isoformat()})"))
            working = sorted(set(body.working_days))
        if ((new_start, new_end) == (old_start, old_end) and working == old_working
                and (not paid or body.paid_hours is None or _same_hours(old_hours, body.paid_hours))):
            raise HTTPException(status_code=422, detail=(
                "Nothing to change \u2014 pick fewer days, mark a day they're working part of, "
                "give new paid hours, or cancel it"))
    dates_changed = not body.cancel and (new_start, new_end) != (old_start, old_end)

    hours_note = ""
    if body.cancel or not paid:
        new_hours = None
    elif body.paid_hours is not None:
        new_hours = body.paid_hours
    elif not dates_changed:
        new_hours = old_hours
    elif (emp is not None and not (old_working or working)
          and _same_hours(old_hours, _hours_off(db, body.venue_id, emp, req, old_start, old_end))):
        new_hours = _hours_off(db, body.venue_id, emp, req, new_start, new_end)
    else:
        new_hours = old_hours
        if old_hours is not None:
            hours_note = (f" Paid hours stay at {_hours_text(old_hours)} \u2014 check them for "
                          "the new dates.")
    if not body.cancel and not hours_note and not _same_hours(old_hours, new_hours):
        hours_note = f" Paid hours: {_hours_text(old_hours)} \u2192 {_hours_text(new_hours)}."

    # A day another approved record of theirs still covers isn't released.
    others = [(_day(r["start_date"]), _day(r["end_date"]))
              for r in db.list_leave_requests(body.venue_id) or []
              if r.get("employee_id") == emp_id and r.get("status") == "approved"
              and r.get("id") != req_id]
    released = set()
    for n in range((old_end - old_start).days + 1):
        d = old_start + timedelta(days=n)
        kept = not body.cancel and new_start <= d <= new_end
        if not kept and not any(a <= d <= b for a, b in others):
            released.add(d)

    if body.cancel:
        req.update(status="cancelled", decided_by=user.user_id, decided_at=datetime.utcnow(),
                   decision_note=body.note)
    else:
        req.update(start_date=new_start, end_date=new_end,
                   working_days=[d.isoformat() for d in working])
    req["paid_hours"] = new_hours
    db.save_leave_request(req)

    note = f"{name}'s leave no longer covers this day."
    withdrawn, claimed = [], []
    for cv in db.list_shift_covers(body.venue_id) or []:
        if (cv.get("origin") != "leave" or str(cv.get("requested_by") or "") != str(emp_id)
                or cover_day(cv) not in released):
            continue
        if cv.get("status") == "open":
            row = db.withdraw_shift_cover(cv["id"], user.user_id, note)
            if row is not None:
                withdrawn.append(row)
        elif cv.get("status") == "claimed":
            claimed.append(cv)
    # The shifts left open on the released days: the ones this leave opened,
    # and the ones its covers were posted for (a paste or a copied week leaves
    # a shift open without recording it on the leave).
    still_open = {}
    if released:
        open_now = {_slot_key(s.date.isoformat(), s.start_time.strftime("%H:%M"),
                              s.end_time.strftime("%H:%M"), s.role): s
                    for s in live_shifts(db, body.venue_id, min(released), max(released))
                    if is_open(s)}
        keys = [_slot_key(o.get("date"), o.get("start"), o.get("end"), o.get("role"))
                for o in req.get("opened_shifts") or []]
        keys += [_slot_key(cv.get("shift_date"), cv.get("shift_start"), cv.get("shift_end"),
                           cv.get("role")) for cv in withdrawn + claimed]
        for key in keys:
            if key in open_now and _day(key[0]) in released:
                still_open[key] = shift_label(open_now[key])
    still_open = [still_open[k] for k in sorted(still_open)]

    if body.cancel:
        change = "cancel"
    elif dates_changed:
        change = "shorten"
    else:
        change = "working_days" if working != old_working else "paid_hours"
    audit("leave.amend", body.venue_id, "leave_request", req_id,
          change=change, employee_id=emp_id,
          employee=getattr(emp, "name", None), leave_type=req.get("leave_type") or "other",
          old_start_date=old_start.isoformat(), old_end_date=old_end.isoformat(),
          start_date=new_start.isoformat() if new_start else None,
          end_date=new_end.isoformat() if new_end else None,
          old_paid_hours=old_hours, paid_hours=new_hours,
          old_working_days=[d.isoformat() for d in old_working],
          working_days=[d.isoformat() for d in working],
          released_days=[d.isoformat() for d in sorted(released)],
          withdrawn_covers=[c.get("id") for c in withdrawn], note=body.note)

    if body.cancel:
        message = (f"{label} for {name} ({old_start.isoformat()} to {old_end.isoformat()}) "
                   "cancelled.")
    elif dates_changed:
        message = (f"{name}'s {label.lower()} now runs {new_start.isoformat()} to "
                   f"{new_end.isoformat()}.")
    else:
        message = (f"{name}'s {label.lower()} ({old_start.isoformat()} to "
                   f"{old_end.isoformat()}) changed.")
    message += hours_note
    if not body.cancel and working != old_working:
        if working:
            message += (f" {name} can be rostered on {', '.join(_day_text(d) for d in working)} "
                        f"too \u2014 the leave {'keeps its paid hours' if paid else 'stands'}, "
                        "and the shifts it left open those days stay open for cover.")
        else:
            message += " None of its days is marked as worked any more."
    if still_open:
        message += (f" Still open on the days released: {', '.join(still_open)} \u2014 put "
                    f"{name} back on the roster if they're working.")
    if withdrawn:
        message += (" The cover request still open for those days was withdrawn."
                    if len(withdrawn) == 1 else
                    f" The {len(withdrawn)} cover requests still open for those days were "
                    "withdrawn.")
    if claimed:
        message += (f" Someone has claimed {len(claimed)} of those days' shifts \u2014 approve or "
                    "decline on the Cover page.")
    return {
        "status": req["status"],
        "request_id": req_id,
        "start_date": (new_start or old_start).isoformat(),
        "end_date": (new_end or old_end).isoformat(),
        "paid_hours": new_hours,
        "working_days": [d.isoformat() for d in working],
        "released_shifts": still_open,
        "withdrawn_covers": len(withdrawn),
        "message": message,
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
