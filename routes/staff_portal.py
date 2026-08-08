"""
Staff portal — the staff-facing side of the Venue OS, answering the #1
industry complaint (schedule visibility: 41% of operators name it their staff's
top gripe) and the self-service basics staff expect: my roster, my hours, my
leave.

Identity model: a portal user is linked to an Employee record by EMAIL — the
manager puts the staff member's email on their employee record, the staff
member registers/logs in with that same email, and /api/me/* resolves the
match within the venues the user can access. No match = a clear explanation,
not an empty screen.

Staff routes:
    GET  /api/me/profile      -- who am I linked to (employee + venue)
    GET  /api/me/shifts       -- my upcoming shifts (next 14 days)
    GET  /api/me/timesheets   -- my worked hours (last 14 days)
    GET  /api/me/leave        -- my leave requests
    POST /api/me/leave        -- request leave / unavailability

Manager routes:
    GET  /api/leave           -- venue's leave requests (filter by status)
    POST /api/leave/{id}/decide -- approve / decline with optional note

Shift cover (phase 2 — "I can't work this shift"):
    POST /api/me/shifts/{shift_id}/cover -- put my shift up for cover
    GET  /api/me/cover                   -- cover board (up for grabs + mine)
    POST /api/me/cover/{id}/claim        -- claim a co-worker's shift
    POST /api/me/cover/{id}/cancel       -- requester withdraws an open/claimed cover
    GET  /api/cover                      -- manager: venue's cover requests
    POST /api/cover/{id}/decide          -- manager approve/decline; approval
                                            REASSIGNS THE SHIFT on the roster —
                                            the roster is the source of truth,
                                            never just a status flag.
"""

import logging
import uuid
from datetime import date, datetime, timedelta

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field

from rosteriq.database import get_db
from rosteriq.middleware.auth import get_current_user, UserContext
from rosteriq.middleware.tenant import enforce_venue_access

logger = logging.getLogger(__name__)

router = APIRouter(tags=["staff-portal"])

_WEEKDAYS = ["monday", "tuesday", "wednesday", "thursday",
             "friday", "saturday", "sunday"]


class LeaveRequestBody(BaseModel):
    start_date: date
    end_date: date
    reason: str = Field(default="", max_length=300)


class LeaveDecision(BaseModel):
    venue_id: str
    approve: bool
    note: str = Field(default="", max_length=300)


class DayAvailability(BaseModel):
    status: str = Field(..., pattern=r"^(available|unavailable|partial)$")
    ranges: list = Field(default_factory=list,
                         description="For 'partial': [{start:'HH:MM', end:'HH:MM'}]")


class AvailabilityBody(BaseModel):
    # weekday name -> DayAvailability; only supplied days are changed
    days: dict = Field(..., description="e.g. {'monday': {'status':'unavailable'}}")


def _parse_hhmm(v: str) -> int:
    """'HH:MM' -> hour int, validating range; raises 422 on garbage."""
    try:
        h, m = str(v).split(":")[:2] if ":" in str(v) else (v, "0")
        hi = int(h)
        if not (0 <= hi <= 23):
            raise ValueError
        return hi
    except Exception:
        raise HTTPException(status_code=422, detail=f"Bad time '{v}' — use HH:MM (00:00–23:00)")


class CoverRequestBody(BaseModel):
    reason: str = Field(default="", max_length=300)


class CoverDecision(BaseModel):
    venue_id: str
    approve: bool
    note: str = Field(default="", max_length=300)


def _linked_employee(db, user: UserContext):
    """Find the employee record matching this user's email.

    Searches the venues the user can access first (owners search all). If a
    self-registered staff member has NO venue access yet, the manager putting
    their email on an employee record IS the authorisation — so we search all
    venues by email and durably grant that venue on first match. Without this,
    staff who sign themselves up are stuck at linked:false forever."""
    email = (user.email or "").strip().lower()
    if not email:
        return None, None
    if user.is_owner:
        venue_ids = [v.id for v in (db.list_venues() or [])]
    else:
        venue_ids = list(user.venue_ids or [])
    for vid in venue_ids:
        for emp in db.get_employees(vid) or []:
            if (getattr(emp, "email", "") or "").strip().lower() == email:
                return emp, vid

    if not user.is_owner:
        for venue in db.list_venues() or []:
            vid = getattr(venue, "id", None)
            if not vid or vid in venue_ids:
                continue
            for emp in db.get_employees(vid) or []:
                if (getattr(emp, "email", "") or "").strip().lower() == email:
                    # Auto-link: grant this venue to the user record so every
                    # later request (and the tenant middleware) sees it.
                    try:
                        rec = db.get_user_by_id(user.user_id)
                        if rec is not None:
                            vids = list(rec.get("venue_ids") or [])
                            if vid not in vids:
                                vids.append(vid)
                                rec["venue_ids"] = vids
                                db.save_user(rec)
                                logger.info(
                                    f"Staff auto-link: {email} -> employee "
                                    f"{emp.id} at {vid}")
                    except Exception as e:
                        logger.warning(f"Staff auto-link grant failed for {email}: {e}")
                    return emp, vid
    return None, None


def _no_link_response(user: UserContext) -> dict:
    return {
        "linked": False,
        "message": (
            "No staff record matches your login email "
            f"({user.email}). Ask your manager to add this email to your "
            "profile in Staff, then reload."
        ),
    }


# ---------------------------------------------------------------------------
# Staff-facing
# ---------------------------------------------------------------------------

@router.get("/api/me/profile")
async def my_profile(user: UserContext = Depends(get_current_user)) -> dict:
    db = get_db()
    emp, vid = _linked_employee(db, user)
    if not emp:
        return _no_link_response(user)
    venue = db.get_venue(vid)
    return {
        "linked": True,
        "employee_id": emp.id,
        "name": emp.name,
        "venue_id": vid,
        "venue_name": getattr(venue, "name", vid) if venue else vid,
        "employment_type": str(emp.employment_type.value if hasattr(emp.employment_type, "value") else emp.employment_type),
        "skills": list(emp.skills or []),
    }


@router.get("/api/me/shifts")
async def my_shifts(user: UserContext = Depends(get_current_user)) -> dict:
    """My upcoming shifts — the schedule-visibility fix."""
    db = get_db()
    emp, vid = _linked_employee(db, user)
    if not emp:
        return _no_link_response(user)
    today = date.today()
    horizon = today + timedelta(days=14)
    mine = []
    try:
        for s in db.get_shifts(vid, today, horizon) or []:
            if str(getattr(s, "employee_id", "")) != str(emp.id):
                continue
            mine.append({
                "id": s.id,
                "date": s.date.isoformat(),
                "day": s.date.strftime("%A"),
                "start": s.start_time.strftime("%H:%M"),
                "end": s.end_time.strftime("%H:%M"),
                "role": s.role,
                "break_minutes": getattr(s, "break_minutes", 0),
            })
    except Exception as e:
        logger.warning(f"my_shifts read failed for {emp.id}: {e}")
    mine.sort(key=lambda x: (x["date"], x["start"]))
    return {"linked": True, "employee_id": emp.id, "name": emp.name,
            "from": today.isoformat(), "to": horizon.isoformat(),
            "count": len(mine), "shifts": mine}


@router.get("/api/me/timesheets")
async def my_timesheets(user: UserContext = Depends(get_current_user)) -> dict:
    """My worked hours, last 14 days — pay transparency."""
    db = get_db()
    emp, vid = _linked_employee(db, user)
    if not emp:
        return _no_link_response(user)
    end = date.today()
    start = end - timedelta(days=14)
    rows = []
    total = 0
    for t in db.get_timesheets(vid, start, end) or []:
        if t.get("employee_id") != emp.id:
            continue
        ci, co = t.get("clock_in"), t.get("clock_out")
        worked = None
        if ci and co:
            a = datetime.fromisoformat(ci) if isinstance(ci, str) else ci
            b = datetime.fromisoformat(co) if isinstance(co, str) else co
            if getattr(a, "tzinfo", None) is not None:
                a = a.replace(tzinfo=None)
            if getattr(b, "tzinfo", None) is not None:
                b = b.replace(tzinfo=None)
            worked = max(0, int((b - a).total_seconds() // 60) - int(t.get("break_minutes") or 0))
            total += worked
        rows.append({
            "work_date": str(t.get("work_date")),
            "worked_minutes": worked,
            "status": t.get("status"),
        })
    return {"linked": True, "count": len(rows), "total_worked_minutes": total, "timesheets": rows}


@router.get("/api/me/leave")
async def my_leave(user: UserContext = Depends(get_current_user)) -> dict:
    db = get_db()
    emp, vid = _linked_employee(db, user)
    if not emp:
        return _no_link_response(user)
    mine = [r for r in (db.list_leave_requests(vid) or []) if r.get("employee_id") == emp.id]
    return {"linked": True, "count": len(mine), "requests": [{
        "id": r["id"], "start_date": str(r["start_date"]), "end_date": str(r["end_date"]),
        "reason": r.get("reason"), "status": r.get("status"),
        "decision_note": r.get("decision_note"),
    } for r in mine]}


@router.post("/api/me/leave")
async def request_leave(body: LeaveRequestBody,
                        user: UserContext = Depends(get_current_user)) -> dict:
    db = get_db()
    emp, vid = _linked_employee(db, user)
    if not emp:
        raise HTTPException(status_code=409, detail=_no_link_response(user)["message"])
    if body.end_date < body.start_date:
        raise HTTPException(status_code=422, detail="end_date must be on or after start_date")
    if body.start_date < date.today():
        raise HTTPException(status_code=422, detail="Leave requests must be for future dates")
    # One pending request per overlapping period keeps things sane
    for r in db.list_leave_requests(vid) or []:
        if (r.get("employee_id") == emp.id and r.get("status") == "pending"):
            rs = r["start_date"] if isinstance(r["start_date"], date) else date.fromisoformat(str(r["start_date"])[:10])
            re_ = r["end_date"] if isinstance(r["end_date"], date) else date.fromisoformat(str(r["end_date"])[:10])
            if rs <= body.end_date and re_ >= body.start_date:
                raise HTTPException(status_code=409, detail="You already have a pending request overlapping those dates")
    req = {
        "id": f"lv-{uuid.uuid4().hex[:10]}",
        "venue_id": vid,
        "employee_id": emp.id,
        "start_date": body.start_date,
        "end_date": body.end_date,
        "reason": body.reason,
        "status": "pending",
        "created_at": datetime.utcnow(),
    }
    db.save_leave_request(req)
    logger.info(f"Leave requested: {emp.name} {body.start_date}..{body.end_date} at {vid}")
    return {"status": "requested", "request_id": req["id"]}


# ---------------------------------------------------------------------------
# Availability — staff set when they can/can't work; the roster generator
# already honours employee.availability, so this closes that loop from /my.
#   day absent from dict  -> available all day
#   day present, ranges [] -> unavailable all day
#   day present, ranges    -> available only in those windows
# ---------------------------------------------------------------------------

@router.get("/api/me/availability")
async def my_availability(user: UserContext = Depends(get_current_user)) -> dict:
    db = get_db()
    emp, vid = _linked_employee(db, user)
    if not emp:
        return _no_link_response(user)
    avail = getattr(emp, "availability", {}) or {}
    days = []
    for d in _WEEKDAYS:
        if d not in avail:
            days.append({"day": d, "status": "available", "ranges": []})
        elif not avail[d]:
            days.append({"day": d, "status": "unavailable", "ranges": []})
        else:
            days.append({"day": d, "status": "partial", "ranges": avail[d]})
    return {"linked": True, "days": days}


@router.post("/api/me/availability")
async def set_my_availability(body: AvailabilityBody,
                              user: UserContext = Depends(get_current_user)) -> dict:
    db = get_db()
    emp, vid = _linked_employee(db, user)
    if not emp:
        raise HTTPException(status_code=409, detail=_no_link_response(user)["message"])

    avail = dict(getattr(emp, "availability", {}) or {})
    for day, spec in (body.days or {}).items():
        d = str(day).strip().lower()
        if d not in _WEEKDAYS:
            raise HTTPException(status_code=422, detail=f"Unknown day '{day}'")
        status = str((spec or {}).get("status", "")).lower()
        if status == "available":
            avail.pop(d, None)
        elif status == "unavailable":
            avail[d] = []
        elif status == "partial":
            ranges = (spec or {}).get("ranges") or []
            if not ranges:
                raise HTTPException(status_code=422,
                                    detail=f"{d}: 'partial' needs at least one time range")
            clean = []
            for r in ranges:
                start = str(r.get("start", "")).strip()
                end = str(r.get("end", "")).strip()
                if _parse_hhmm(start) >= _parse_hhmm(end):
                    raise HTTPException(status_code=422,
                                        detail=f"{d}: range start {start} must be before end {end}")
                clean.append({"start": start, "end": end})
            avail[d] = clean
        else:
            raise HTTPException(status_code=422,
                                detail=f"{d}: status must be available / unavailable / partial")

    emp.availability = avail
    emp.updated_at = datetime.utcnow()
    db.save_employee(emp)
    logger.info(f"Availability updated: {emp.name} at {vid}")
    return {"status": "saved", "days_set": list((body.days or {}).keys())}


# ---------------------------------------------------------------------------
# Shift cover — staff side
# ---------------------------------------------------------------------------

def _find_my_shift(db, venue_id: str, employee_id: str, shift_id: str):
    """The named future shift, only if it belongs to this employee."""
    today = date.today()
    try:
        shifts = db.get_shifts(venue_id, today, today + timedelta(days=60)) or []
    except Exception:
        shifts = []
    for s in shifts:
        if getattr(s, "id", None) == shift_id:
            if str(getattr(s, "employee_id", "")) != str(employee_id):
                raise HTTPException(status_code=403, detail="You can only offer your own shifts for cover")
            return s
    raise HTTPException(status_code=404, detail="Shift not found in your upcoming roster")


def _cover_payload(c, emp_names: dict) -> dict:
    return {
        "id": c["id"],
        "shift_id": c["shift_id"],
        "shift_date": str(c["shift_date"]),
        "shift_start": c.get("shift_start"),
        "shift_end": c.get("shift_end"),
        "role": c.get("role"),
        "requested_by": c["requested_by"],
        "requested_by_name": emp_names.get(c["requested_by"], c["requested_by"]),
        "reason": c.get("reason"),
        "claimed_by": c.get("claimed_by"),
        "claimed_by_name": emp_names.get(c.get("claimed_by"), c.get("claimed_by")) if c.get("claimed_by") else None,
        "status": c.get("status"),
        "decision_note": c.get("decision_note"),
    }


@router.post("/api/me/shifts/{shift_id}/cover")
async def offer_shift_for_cover(shift_id: str, body: CoverRequestBody,
                                user: UserContext = Depends(get_current_user)) -> dict:
    """Put one of my upcoming shifts up for a co-worker to cover."""
    db = get_db()
    emp, vid = _linked_employee(db, user)
    if not emp:
        raise HTTPException(status_code=409, detail=_no_link_response(user)["message"])
    shift = _find_my_shift(db, vid, emp.id, shift_id)
    for c in db.list_shift_covers(vid) or []:
        if c.get("shift_id") == shift_id and c.get("status") in ("open", "claimed"):
            raise HTTPException(status_code=409, detail="That shift is already up for cover")
    cover = {
        "id": f"cv-{uuid.uuid4().hex[:10]}",
        "venue_id": vid,
        "shift_id": shift_id,
        "shift_date": shift.date,
        "shift_start": shift.start_time.strftime("%H:%M"),
        "shift_end": shift.end_time.strftime("%H:%M"),
        "role": shift.role,
        "requested_by": emp.id,
        "reason": body.reason,
        "claimed_by": None,
        "status": "open",
        "created_at": datetime.utcnow(),
    }
    db.save_shift_cover(cover)
    logger.info(f"Cover requested: {emp.name} for shift {shift_id} at {vid}")
    return {"status": "open", "cover_id": cover["id"]}


@router.get("/api/me/cover")
async def cover_board(user: UserContext = Depends(get_current_user)) -> dict:
    """The cover board: shifts up for grabs at my venue, plus my own requests
    and anything I've claimed."""
    db = get_db()
    emp, vid = _linked_employee(db, user)
    if not emp:
        return _no_link_response(user)
    emp_names = {e.id: e.name for e in (db.get_employees(vid) or [])}
    covers = db.list_shift_covers(vid) or []
    up_for_grabs, mine, claimed_by_me = [], [], []
    for c in covers:
        p = _cover_payload(c, emp_names)
        if c.get("requested_by") == emp.id:
            mine.append(p)
        elif c.get("claimed_by") == emp.id and c.get("status") in ("claimed", "approved"):
            claimed_by_me.append(p)
        elif c.get("status") == "open":
            up_for_grabs.append(p)
    return {"linked": True, "up_for_grabs": up_for_grabs, "mine": mine,
            "claimed_by_me": claimed_by_me}


@router.post("/api/me/cover/{cover_id}/claim")
async def claim_cover(cover_id: str, user: UserContext = Depends(get_current_user)) -> dict:
    """Claim a co-worker's open shift — goes to the manager for approval."""
    db = get_db()
    emp, vid = _linked_employee(db, user)
    if not emp:
        raise HTTPException(status_code=409, detail=_no_link_response(user)["message"])
    cover = db.get_shift_cover(cover_id)
    if not cover or cover.get("venue_id") != vid:
        raise HTTPException(status_code=404, detail="Cover request not found")
    if cover.get("requested_by") == emp.id:
        raise HTTPException(status_code=409, detail="You can't claim your own shift")
    if cover.get("status") != "open":
        raise HTTPException(status_code=409, detail=f"This shift is already {cover.get('status')}")
    cover["claimed_by"] = emp.id
    cover["status"] = "claimed"
    db.save_shift_cover(cover)
    logger.info(f"Cover claimed: {emp.name} claims {cover_id} at {vid}")
    return {"status": "claimed", "cover_id": cover_id,
            "message": "Claimed — your manager still needs to approve the change"}


@router.post("/api/me/cover/{cover_id}/cancel")
async def cancel_cover(cover_id: str, user: UserContext = Depends(get_current_user)) -> dict:
    """Requester withdraws their cover request before it's decided."""
    db = get_db()
    emp, vid = _linked_employee(db, user)
    if not emp:
        raise HTTPException(status_code=409, detail=_no_link_response(user)["message"])
    cover = db.get_shift_cover(cover_id)
    if not cover or cover.get("venue_id") != vid:
        raise HTTPException(status_code=404, detail="Cover request not found")
    if cover.get("requested_by") != emp.id:
        raise HTTPException(status_code=403, detail="Only the requester can cancel this")
    if cover.get("status") not in ("open", "claimed"):
        raise HTTPException(status_code=409, detail=f"Already {cover.get('status')} — ask your manager")
    cover["status"] = "cancelled"
    db.save_shift_cover(cover)
    return {"status": "cancelled", "cover_id": cover_id}


# ---------------------------------------------------------------------------
# Manager-facing
# ---------------------------------------------------------------------------

@router.get("/api/leave")
async def venue_leave(venue_id: str = Query(...), status: str = Query("")) -> dict:
    enforce_venue_access(venue_id)
    db = get_db()
    emp_names = {e.id: e.name for e in (db.get_employees(venue_id) or [])}
    rows = db.list_leave_requests(venue_id) or []
    if status:
        rows = [r for r in rows if r.get("status") == status]
    return {"venue_id": venue_id, "count": len(rows), "requests": [{
        "id": r["id"],
        "employee_id": r["employee_id"],
        "employee_name": emp_names.get(r["employee_id"], r["employee_id"]),
        "start_date": str(r["start_date"]), "end_date": str(r["end_date"]),
        "reason": r.get("reason"), "status": r.get("status"),
        "decided_by": r.get("decided_by"), "decision_note": r.get("decision_note"),
    } for r in rows]}


@router.get("/api/cover")
async def venue_covers(venue_id: str = Query(...), status: str = Query("")) -> dict:
    """Manager view of the venue's shift cover requests."""
    enforce_venue_access(venue_id)
    db = get_db()
    emp_names = {e.id: e.name for e in (db.get_employees(venue_id) or [])}
    rows = db.list_shift_covers(venue_id) or []
    if status:
        rows = [c for c in rows if c.get("status") == status]
    return {"venue_id": venue_id, "count": len(rows),
            "covers": [_cover_payload(c, emp_names) for c in rows]}


def _reassign_shift(db, venue_id: str, shift_id: str, new_employee_id: str) -> bool:
    """Move the shift to its new owner on the roster itself. Returns False if
    the shift no longer exists (roster edited since the cover was requested)."""
    for roster in db.list_rosters() or []:
        if getattr(roster, "venue_id", None) != venue_id:
            continue
        for s in roster.shifts:
            if s.id == shift_id:
                s.employee_id = new_employee_id
                db.save_roster(roster)
                return True
    return False


@router.post("/api/cover/{cover_id}/decide")
async def decide_cover(cover_id: str, body: CoverDecision,
                       user: UserContext = Depends(get_current_user)) -> dict:
    """Approve (reassigns the shift on the roster) or decline a claimed cover."""
    enforce_venue_access(body.venue_id)
    db = get_db()
    cover = db.get_shift_cover(cover_id)
    if not cover or cover.get("venue_id") != body.venue_id:
        raise HTTPException(status_code=404, detail="Cover request not found")
    if cover.get("status") == "open":
        raise HTTPException(status_code=409, detail="Nobody has claimed this shift yet")
    if cover.get("status") != "claimed":
        raise HTTPException(status_code=409, detail=f"Request is already {cover.get('status')}")

    if body.approve:
        # The roster is the source of truth — a cover is only approved if the
        # shift genuinely moves. Fail loud if the roster changed underneath us.
        if not _reassign_shift(db, body.venue_id, cover["shift_id"], cover["claimed_by"]):
            raise HTTPException(
                status_code=409,
                detail="That shift no longer exists on the roster — it may have "
                       "been edited or republished. Decline this request instead.")
        cover["status"] = "approved"
    else:
        # Declined: shift stays with the requester; reopen for other claimants
        cover["status"] = "open"
        cover["claimed_by"] = None
    cover["decided_by"] = user.user_id
    cover["decided_at"] = datetime.utcnow()
    cover["decision_note"] = body.note
    if not body.approve:
        db.save_shift_cover(cover)
        return {"status": "reopened", "cover_id": cover_id}
    db.save_shift_cover(cover)
    logger.info(f"Cover approved: shift {cover['shift_id']} -> {cover['claimed_by']} at {body.venue_id}")
    return {"status": "approved", "cover_id": cover_id}


@router.post("/api/leave/{req_id}/decide")
async def decide_leave(req_id: str, body: LeaveDecision,
                       user: UserContext = Depends(get_current_user)) -> dict:
    enforce_venue_access(body.venue_id)
    db = get_db()
    req = db.get_leave_request(req_id)
    if not req or req.get("venue_id") != body.venue_id:
        raise HTTPException(status_code=404, detail="Leave request not found")
    if req.get("status") != "pending":
        raise HTTPException(status_code=409, detail=f"Request is already {req.get('status')}")
    req["status"] = "approved" if body.approve else "declined"
    req["decided_by"] = user.user_id
    req["decided_at"] = datetime.utcnow()
    req["decision_note"] = body.note
    db.save_leave_request(req)
    return {"status": req["status"], "request_id": req_id}
