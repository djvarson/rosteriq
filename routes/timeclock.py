"""
Native time clock — RosterIQ's own clock in/out, the first pillar of making
external attendance software (Deputy et al) unnecessary.

Staff clock in/out on a venue device (tablet by the till, or a phone) running
under the venue's logged-in session. Each punch is a timesheet row; clock-out
reconciles the worked time against the rostered shift (variance in minutes) so
the award engine and payroll exports price actual worked time, not just the
plan.

Routes (all venue-scoped via enforce_venue_access):
    GET  /api/clock/board        -- today's roster + live clock state per employee
    POST /api/clock/in           -- clock an employee in (optional PIN)
    POST /api/clock/out          -- clock an employee out (optional PIN)
    POST /api/clock/pin          -- set/replace an employee's 4-6 digit PIN
    GET  /api/clock/timesheets   -- timesheets for a date range (with variance)
"""

import hashlib
import logging
import uuid
from datetime import date, datetime, timedelta
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field

from rosteriq.database import get_db
from rosteriq.middleware.auth import get_current_user, UserContext
from rosteriq.middleware.tenant import enforce_venue_access
from rosteriq.services.events import audit

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/clock", tags=["timeclock"])


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------

class PunchRequest(BaseModel):
    venue_id: str = Field(..., description="Venue the device belongs to")
    employee_id: str = Field(..., description="Employee clocking in/out")
    pin: str = Field(default="", description="Employee PIN, if one is set")
    break_minutes: int = Field(default=0, ge=0, le=480,
                               description="Unpaid break taken (clock-out only)")


class SetPinRequest(BaseModel):
    venue_id: str
    employee_id: str
    pin: str = Field(..., min_length=4, max_length=6, pattern=r"^\d{4,6}$",
                     description="4-6 digit PIN")


class ReviewRequest(BaseModel):
    """Manager review of a closed timesheet — optional corrections + approval."""
    venue_id: str
    approve: bool = Field(default=True, description="Approve after (optional) corrections")
    clock_in: Optional[datetime] = Field(default=None, description="Corrected clock-in (UTC)")
    clock_out: Optional[datetime] = Field(default=None, description="Corrected clock-out (UTC)")
    break_minutes: Optional[int] = Field(default=None, ge=0, le=480)
    note: str = Field(default="", max_length=500, description="Why the correction was made")


def _hash_pin(venue_id: str, employee_id: str, pin: str) -> str:
    """Deterministic salted hash — PINs are low-entropy gate codes, not passwords."""
    return hashlib.sha256(f"{venue_id}:{employee_id}:{pin}".encode()).hexdigest()


def _verify_pin(db, venue_id: str, employee_id: str, pin: str) -> bool:
    """True when the PIN matches, or when no PIN is set (punch gets flagged)."""
    stored = db.get_timeclock_pin(venue_id, employee_id)
    if not stored:
        return True  # no PIN configured — allow, pin_verified stays False
    return stored == _hash_pin(venue_id, employee_id, pin)


def _employee_or_404(db, venue_id: str, employee_id: str):
    for emp in db.get_employees(venue_id) or []:
        if getattr(emp, "id", None) == employee_id:
            return emp
    raise HTTPException(status_code=404, detail="Employee not found at this venue")


def _todays_shift_for(db, venue_id: str, employee_id: str, on_date: date):
    """The employee's rostered shift today, if any (for variance reconciliation)."""
    try:
        shifts = db.get_shifts(venue_id, on_date, on_date) or []
    except Exception:
        return None
    for s in shifts:
        if str(getattr(s, "employee_id", "")) == str(employee_id):
            return s
    return None


def _shift_minutes(shift) -> int:
    """Rostered paid minutes for a shift (handles overnight, minus breaks)."""
    start = datetime.combine(shift.date, shift.start_time)
    end = datetime.combine(shift.date, shift.end_time)
    if end <= start:
        end += timedelta(days=1)  # overnight shift
    return int((end - start).total_seconds() // 60) - int(getattr(shift, "break_minutes", 0) or 0)


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@router.get("/board")
async def clock_board(venue_id: str = Query(...)) -> dict:
    """The kiosk's home screen: every staff member with today's rostered times
    and their live clock state (off / on / done)."""
    enforce_venue_access(venue_id)
    db = get_db()
    today = date.today()

    shifts_today = {}
    try:
        for s in db.get_shifts(venue_id, today, today) or []:
            shifts_today[str(s.employee_id)] = {
                "start": s.start_time.strftime("%H:%M"),
                "end": s.end_time.strftime("%H:%M"),
                "role": s.role,
            }
    except Exception:
        pass

    board = []
    for emp in db.get_employees(venue_id) or []:
        open_ts = db.get_open_timesheet(venue_id, emp.id)
        done = False
        if not open_ts:
            for t in db.get_timesheets(venue_id, today, today) or []:
                if t.get("employee_id") == emp.id and t.get("clock_out"):
                    done = True
                    break
        board.append({
            "employee_id": emp.id,
            "name": emp.name,
            "rostered": shifts_today.get(str(emp.id)),
            "state": "on" if open_ts else ("done" if done else "off"),
            "clock_in": str(open_ts["clock_in"]) if open_ts else None,
            "has_pin": bool(db.get_timeclock_pin(venue_id, emp.id)),
        })
    board.sort(key=lambda b: (b["rostered"] is None, b["name"]))
    return {"venue_id": venue_id, "date": today.isoformat(), "staff": board}


@router.post("/in")
async def clock_in(body: PunchRequest) -> dict:
    enforce_venue_access(body.venue_id)
    db = get_db()
    emp = _employee_or_404(db, body.venue_id, body.employee_id)

    if not _verify_pin(db, body.venue_id, body.employee_id, body.pin):
        raise HTTPException(status_code=403, detail="Incorrect PIN")

    if db.get_open_timesheet(body.venue_id, body.employee_id):
        raise HTTPException(status_code=409, detail=f"{emp.name} is already clocked in")

    now = datetime.utcnow()
    shift = _todays_shift_for(db, body.venue_id, body.employee_id, date.today())
    ts = {
        "id": f"ts-{uuid.uuid4().hex[:12]}",
        "venue_id": body.venue_id,
        "employee_id": body.employee_id,
        "work_date": date.today(),
        "clock_in": now,
        "clock_out": None,
        "break_minutes": 0,
        "status": "open",
        "pin_verified": bool(db.get_timeclock_pin(body.venue_id, body.employee_id)),
        "rostered_shift_id": getattr(shift, "id", None) if shift else None,
        "variance_minutes": None,
        "created_at": now,
    }
    db.save_timesheet(ts)
    logger.info(f"Clock IN: {emp.name} at {body.venue_id} ({ts['id']})")
    return {
        "status": "clocked_in",
        "timesheet_id": ts["id"],
        "employee": emp.name,
        "at": now.isoformat(),
        "rostered": bool(shift),
        "pin_verified": ts["pin_verified"],
    }


@router.post("/out")
async def clock_out(body: PunchRequest) -> dict:
    enforce_venue_access(body.venue_id)
    db = get_db()
    emp = _employee_or_404(db, body.venue_id, body.employee_id)

    if not _verify_pin(db, body.venue_id, body.employee_id, body.pin):
        raise HTTPException(status_code=403, detail="Incorrect PIN")

    ts = db.get_open_timesheet(body.venue_id, body.employee_id)
    if not ts:
        raise HTTPException(status_code=409, detail=f"{emp.name} is not clocked in")

    now = datetime.utcnow()
    clock_in_at = ts["clock_in"]
    if isinstance(clock_in_at, str):
        clock_in_at = datetime.fromisoformat(clock_in_at)
    # Postgres returns tz-aware datetimes; normalise both sides to naive UTC
    if getattr(clock_in_at, "tzinfo", None) is not None:
        clock_in_at = clock_in_at.replace(tzinfo=None)
    worked_minutes = max(0, int((now - clock_in_at).total_seconds() // 60) - body.break_minutes)

    variance = None
    shift = None
    if ts.get("rostered_shift_id"):
        shift = _todays_shift_for(db, body.venue_id, body.employee_id, ts["work_date"]
                                  if isinstance(ts["work_date"], date) else date.today())
    if shift is not None:
        variance = worked_minutes - _shift_minutes(shift)

    ts.update({
        "clock_out": now,
        "break_minutes": body.break_minutes,
        "status": "closed",
        "variance_minutes": variance,
    })
    db.save_timesheet(ts)
    logger.info(f"Clock OUT: {emp.name} at {body.venue_id} — {worked_minutes}m worked, variance={variance}")
    out = {
        "status": "clocked_out",
        "timesheet_id": ts["id"],
        "employee": emp.name,
        "at": now.isoformat(),
        "worked_minutes": worked_minutes,
        "break_minutes": body.break_minutes,
        "variance_minutes": variance,
    }
    # An immediate in/out is almost always a mispunch — never block it (the
    # punch is a fact), but say so, and the review queue will show it anyway.
    if worked_minutes < 5:
        out["warning"] = (f"Only {worked_minutes} minutes worked — likely an "
                          "accidental punch. A manager can correct or approve "
                          "it on the Timesheets page.")
    return out


@router.post("/pin")
async def set_pin(body: SetPinRequest) -> dict:
    """Set/replace an employee's kiosk PIN (manager action, venue-scoped)."""
    enforce_venue_access(body.venue_id)
    db = get_db()
    emp = _employee_or_404(db, body.venue_id, body.employee_id)
    db.set_timeclock_pin(body.venue_id, body.employee_id,
                         _hash_pin(body.venue_id, body.employee_id, body.pin))
    return {"status": "pin_set", "employee": emp.name}


@router.post("/timesheets/{ts_id}/review")
async def review_timesheet(
    ts_id: str,
    body: ReviewRequest,
    user: UserContext = Depends(get_current_user),
) -> dict:
    """Manager reviews a closed timesheet: optionally corrects the punch times
    or break, then approves. Corrections recompute worked time and variance and
    are recorded with who/when/why — approved time is what payroll exports use."""
    enforce_venue_access(body.venue_id)
    db = get_db()
    ts = db.get_timesheet(ts_id)
    if not ts or ts.get("venue_id") != body.venue_id:
        raise HTTPException(status_code=404, detail="Timesheet not found")
    if not ts.get("clock_out") and not body.clock_out:
        raise HTTPException(status_code=409, detail="Timesheet is still open — clock the employee out first (or supply a corrected clock_out)")
    if ts.get("status") == "approved":
        raise HTTPException(status_code=409, detail="Timesheet is already approved")

    before = {k: (str(ts.get(k)) if k != "break_minutes" else ts.get(k))
              for k in ("clock_in", "clock_out", "break_minutes")}
    adjusted = False
    for field in ("clock_in", "clock_out"):
        val = getattr(body, field)
        if val is not None:
            ts[field] = val.replace(tzinfo=None) if getattr(val, "tzinfo", None) else val
            adjusted = True
    if body.break_minutes is not None and body.break_minutes != ts.get("break_minutes"):
        ts["break_minutes"] = body.break_minutes
        adjusted = True
    if adjusted and not body.note:
        raise HTTPException(status_code=422, detail="A note is required when correcting a timesheet")

    # Recompute worked minutes + variance from (possibly corrected) values
    a, b = ts["clock_in"], ts["clock_out"]
    a = datetime.fromisoformat(a) if isinstance(a, str) else a
    b = datetime.fromisoformat(b) if isinstance(b, str) else b
    if getattr(a, "tzinfo", None) is not None:
        a = a.replace(tzinfo=None)
    if getattr(b, "tzinfo", None) is not None:
        b = b.replace(tzinfo=None)
    if b <= a:
        raise HTTPException(status_code=422, detail="clock_out must be after clock_in")
    worked = max(0, int((b - a).total_seconds() // 60) - int(ts.get("break_minutes") or 0))

    work_date = ts.get("work_date")
    if isinstance(work_date, str):
        work_date = date.fromisoformat(work_date[:10])
    shift = _todays_shift_for(db, body.venue_id, ts["employee_id"], work_date) if work_date else None
    ts["variance_minutes"] = (worked - _shift_minutes(shift)) if shift else ts.get("variance_minutes")

    if body.approve:
        ts["status"] = "approved"
        ts["approved_by"] = user.user_id
        ts["approved_at"] = datetime.utcnow()
    if adjusted:
        ts["adjustment_note"] = body.note
    ts["clock_in"], ts["clock_out"] = a, b
    db.save_timesheet(ts)

    # Event log: a correction and an approval are two separate facts a venue
    # owner must be able to answer for (who changed the punch, who signed it off).
    try:
        emp_name = getattr(db.get_employee(ts["employee_id"]), "name", None) or str(ts["employee_id"])
    except Exception:
        emp_name = str(ts["employee_id"])
    if adjusted:
        after = {"clock_in": str(a), "clock_out": str(b), "break_minutes": ts.get("break_minutes")}
        audit("timesheet.correct", body.venue_id, "timesheet", ts_id,
              employee_id=ts["employee_id"], employee=emp_name,
              work_date=str(work_date), worked_minutes=worked,
              variance_minutes=ts.get("variance_minutes"), note=body.note,
              changed={k: {"old": before[k], "new": after[k]}
                       for k in after if before[k] != after[k]})
    if body.approve:
        audit("timesheet.approve", body.venue_id, "timesheet", ts_id,
              employee_id=ts["employee_id"], employee=emp_name,
              work_date=str(work_date), worked_minutes=worked,
              break_minutes=ts.get("break_minutes"),
              variance_minutes=ts.get("variance_minutes"),
              corrected=adjusted, note=body.note)

    logger.info(
        f"Timesheet {ts_id} reviewed by {user.user_id}: "
        f"{'adjusted, ' if adjusted else ''}{'approved' if body.approve else 'saved'} ({worked}m)"
    )
    return {
        "status": ts["status"],
        "timesheet_id": ts_id,
        "worked_minutes": worked,
        "variance_minutes": ts.get("variance_minutes"),
        "adjusted": adjusted,
        "approved_by": ts.get("approved_by"),
    }


@router.get("/timesheets")
async def list_timesheets(
    venue_id: str = Query(...),
    start_date: date = Query(...),
    end_date: date = Query(...),
    status: str = Query("", description="Filter: open | closed | approved"),
) -> dict:
    """Timesheets with variance — the feed for payroll export and review."""
    enforce_venue_access(venue_id)
    db = get_db()
    emp_names = {e.id: e.name for e in (db.get_employees(venue_id) or [])}
    rows = []
    total_minutes = 0
    for t in db.get_timesheets(venue_id, start_date, end_date) or []:
        if status and t.get("status") != status:
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
            total_minutes += worked
        rows.append({
            "id": t["id"],
            "employee_id": t["employee_id"],
            "employee_name": emp_names.get(t["employee_id"], t["employee_id"]),
            "work_date": str(t["work_date"]),
            "clock_in": str(ci) if ci else None,
            "clock_out": str(co) if co else None,
            "break_minutes": t.get("break_minutes", 0),
            "worked_minutes": worked,
            "variance_minutes": t.get("variance_minutes"),
            "status": t.get("status"),
            "pin_verified": t.get("pin_verified", False),
            "approved_by": t.get("approved_by"),
            "adjustment_note": t.get("adjustment_note"),
        })
    return {
        "venue_id": venue_id,
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "count": len(rows),
        "total_worked_minutes": total_minutes,
        "timesheets": rows,
    }
