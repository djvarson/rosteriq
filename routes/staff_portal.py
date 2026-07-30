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


class LeaveRequestBody(BaseModel):
    start_date: date
    end_date: date
    reason: str = Field(default="", max_length=300)


class LeaveDecision(BaseModel):
    venue_id: str
    approve: bool
    note: str = Field(default="", max_length=300)


def _linked_employee(db, user: UserContext):
    """Find the employee record matching this user's email, searching the
    venues the user can access (owners search all venues)."""
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
