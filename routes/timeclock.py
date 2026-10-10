"""
Native time clock — RosterIQ's own clock in/out, the first pillar of making
external attendance software (Deputy et al) unnecessary.

Staff clock in/out on a venue TIME CLOCK DEVICE (a tablet by the till) that a
manager set up once (routes/kiosk.py mints its device token). Each punch is a
timesheet row; clock-out reconciles the worked time against the rostered
shift it was matched to (variance in minutes) so the award engine and payroll
exports price actual worked time, not just the plan.

Who may call what:
    GET  /api/clock/board              the venue's time clock, or a manager
    GET  /api/clock/kiosk              the time clock: which venue/device am I
    POST /api/clock/kiosk/exit         the time clock + a manager's kiosk PIN
    POST /api/clock/pin/check          the time clock: check a PIN, get the card
    POST /api/clock/in | /out          the time clock; or a person clocking
    POST /api/clock/break/start | /end     THEMSELVES when the venue allows it
    POST /api/clock/pin                manager: set one employee's PIN
    GET  /api/clock/timesheets         manager (status=open ignores dates)
    POST /api/clock/timesheets/{id}/review   manager: correct and/or approve
    POST /api/clock/timesheets/approve       manager: approve many / all clean
"""

import logging
import uuid
from datetime import date, datetime, timedelta, timezone
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field

from rosteriq.award_rules import get_minimum_break_minutes
from rosteriq.database import get_db, _jsonb, OpenTimesheetExists
from rosteriq.services.clock import venue_today, venue_timezone
from rosteriq.services.current_roster import live_shifts
from rosteriq.services import kiosk
from rosteriq.services.kiosk import naive_utc
from rosteriq.middleware.auth import get_current_user, UserContext
from rosteriq.middleware.tenant import (
    enforce_venue_access, enforce_venue_manager, enforce_venue_manager_any,
    enforce_kiosk_or_venue_manager, get_tenant_context_optional, kiosk_device_id,
)
from rosteriq.services.events import audit
from rosteriq.services.staff_records import is_active

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/clock", tags=["timeclock"])


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------

class PunchRequest(BaseModel):
    venue_id: str = Field(..., description="Venue the device belongs to")
    employee_id: str = Field(..., description="Employee clocking in/out")
    pin: str = Field(default="", max_length=12, description="Employee PIN, if one is set")
    break_minutes: int = Field(default=0, ge=0, le=480,
                               description="Unpaid break taken but not punched (clock-out only)")
    finished_at: Optional[str] = Field(
        default=None, pattern=r"^\d{1,2}:\d{2}$",
        description="Forgotten clock-out: the venue-local time (HH:MM) they actually finished")


class PinCheckRequest(BaseModel):
    venue_id: str
    employee_id: str
    pin: str = Field(default="", max_length=12)


class KioskExitRequest(BaseModel):
    pin: str = Field(default="", max_length=6)


class SetPinRequest(BaseModel):
    venue_id: str
    employee_id: str
    pin: str = Field(..., min_length=4, max_length=6, pattern=r"^\d{4,6}$",
                     description="4-6 digit PIN")


class ReviewRequest(BaseModel):
    """Manager review of a timesheet — optional corrections + approval."""
    venue_id: str
    approve: bool = Field(default=True, description=(
        "Approve after (optional) corrections. Closing an open punch approves it only when "
        "this is sent explicitly."))
    clock_in: Optional[datetime] = Field(default=None, description="Corrected clock-in (UTC)")
    clock_out: Optional[datetime] = Field(default=None, description="Corrected clock-out (UTC)")
    break_minutes: Optional[int] = Field(default=None, ge=0, le=480)
    note: str = Field(default="", max_length=500, description="Why the correction was made")


class BatchApproveRequest(BaseModel):
    venue_id: str
    ids: List[str] = Field(..., min_length=1, max_length=1000)
    only_clean: bool = Field(default=False, description="Approve only the clean ones; skip the rest")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _tz(venue_id, db):
    from zoneinfo import ZoneInfo
    try:
        return ZoneInfo(venue_timezone(venue_id, db))
    except Exception:
        return timezone.utc


def _local(dt_utc, tz) -> datetime:
    """Naive UTC -> naive venue-local wall clock."""
    return dt_utc.replace(tzinfo=timezone.utc).astimezone(tz).replace(tzinfo=None)


def _utc(dt_local, tz) -> datetime:
    """Naive venue-local wall clock -> naive UTC."""
    return dt_local.replace(tzinfo=tz).astimezone(timezone.utc).replace(tzinfo=None)


def _as_date(v):
    if isinstance(v, datetime):
        return v.date()
    if isinstance(v, str):
        return date.fromisoformat(v[:10])
    return v


def _flags(ts) -> list:
    return list(_jsonb(ts.get("flags"), []) or [])


def _employee_or_404(db, venue_id: str, employee_id: str):
    """Any record at the venue, active or not: clock-in refuses a leaver, but
    one deactivated mid-shift must still be able to clock out."""
    for emp in db.get_employees(venue_id) or []:
        if getattr(emp, "id", None) == employee_id:
            return emp
    raise HTTPException(status_code=404, detail="Employee not found at this venue")


def _shift_window(shift):
    """(start, end) as venue-local naive datetimes; overnight aware."""
    start = datetime.combine(shift.date, shift.start_time)
    end = datetime.combine(shift.date, shift.end_time)
    if end <= start:
        end += timedelta(days=1)
    return start, end


def _shift_minutes(shift) -> int:
    """Rostered paid minutes for a shift (handles overnight, minus breaks)."""
    start, end = _shift_window(shift)
    return int((end - start).total_seconds() // 60) - int(getattr(shift, "break_minutes", 0) or 0)


def _shift_json(s) -> dict:
    return {"shift_id": s.id, "start": s.start_time.strftime("%H:%M"),
            "end": s.end_time.strftime("%H:%M"), "role": s.role,
            "break_minutes": int(getattr(s, "break_minutes", 0) or 0)}


def _shifts_by_employee(db, venue_id: str, start: date, end: date) -> dict:
    out: dict = {}
    try:
        for s in live_shifts(db, venue_id, start, end, staff=True):
            out.setdefault(str(s.employee_id), []).append(s)
    except Exception as e:  # noqa: BLE001 — a roster read never blocks a punch
        logger.warning(f"time clock roster read failed for {venue_id}: {e}")
    for shifts in out.values():
        shifts.sort(key=lambda s: _shift_window(s)[0])
    return out


def _nearest_shift(shifts: list, at_local: datetime, taken=()):
    """The rostered shift whose start is nearest ``at_local``; shifts another
    punch already matched only count when nothing else is left."""
    pool = [s for s in shifts if s.id not in taken] or list(shifts)
    if not pool:
        return None
    return min(pool, key=lambda s: abs((_shift_window(s)[0] - at_local).total_seconds()))


def _matched_shift(db, venue_id: str, ts, tz, by_emp: Optional[dict] = None):
    """The rostered shift a punch belongs to: the one matched at clock-in, else
    the employee's shift that day starting nearest the clock-in."""
    work_date = _as_date(ts.get("work_date"))
    if not work_date:
        return None
    if by_emp is None:
        by_emp = _shifts_by_employee(db, venue_id, work_date, work_date)
    shifts = [s for s in by_emp.get(str(ts["employee_id"]), []) if s.date == work_date]
    sid = ts.get("rostered_shift_id")
    for s in shifts:
        if sid and s.id == sid:
            return s
    return _nearest_shift(shifts, _local(naive_utc(ts["clock_in"]), tz)) if shifts else None


def _current_shift(shifts: list, now_local: datetime):
    """The shift on now, else the next one today, else the last one."""
    for s in shifts:
        if _shift_window(s)[1] > now_local:
            return s
    return shifts[-1] if shifts else None


def _covers_now(shift, now_local: datetime) -> bool:
    start, end = _shift_window(shift)
    return start - timedelta(hours=1) <= now_local <= end


def _fmt_local(dt_utc, tz) -> str:
    d = _local(dt_utc, tz)
    return d.strftime("%a %-I:%M %p").replace("AM", "am").replace("PM", "pm")


def _card(emp, shifts, open_ts, done_today, has_pin, now_local, tz) -> dict:
    """One person on the kiosk board."""
    now = datetime.utcnow()
    state, clock_in, stale, break_started = "off", None, False, None
    if open_ts:
        ci = naive_utc(open_ts["clock_in"])
        clock_in = ci.isoformat()
        state = "break" if open_ts.get("break_started_at") else "on"
        break_started = naive_utc(open_ts.get("break_started_at"))
        stale = (now - ci) > timedelta(hours=kiosk.STALE_OPEN_HOURS)
    elif done_today:
        state = "done"
    current = _current_shift(shifts, now_local)
    return {
        "employee_id": emp.id,
        "name": emp.name,
        "rostered": ({"start": current.start_time.strftime("%H:%M"),
                      "end": current.end_time.strftime("%H:%M"), "role": current.role}
                     if current else None),
        "shifts": [_shift_json(s) for s in shifts],
        "rostered_now": any(_covers_now(s, now_local) for s in shifts),
        "state": state,
        "clock_in": clock_in,
        "clock_in_label": _fmt_local(naive_utc(open_ts["clock_in"]), tz) if open_ts else None,
        "on_break": state == "break",
        "break_started_at": break_started.isoformat() if break_started else None,
        "stale": stale,
        "has_pin": has_pin,
    }


def _punch_actor(db, venue_id: str, employee_id: str) -> dict:
    """A punch comes from the venue's time clock device, or from a person
    clocking THEMSELVES in when the venue allows it (off by default)."""
    enforce_venue_access(venue_id)
    device = kiosk_device_id()
    tenant = get_tenant_context_optional()
    if device or tenant is None:
        return {"source": "kiosk", "device_id": device}
    if not kiosk.load_settings(db, venue_id)["allow_self_clock"]:
        raise HTTPException(status_code=403, detail=(
            "Clock in and out on the venue's time clock — clocking in from a phone is "
            "switched off at this venue."))
    user = db.get_user_by_id(tenant.user_id)
    if employee_id not in kiosk.own_employee_ids(db, user, venue_id):
        raise HTTPException(status_code=403, detail="You can only clock yourself in or out.")
    return {"source": "self", "device_id": None}


def _verify_pin(db, body, actor) -> bool:
    if actor["source"] != "kiosk":
        return False
    return kiosk.check_pin(db, body.venue_id, body.employee_id, body.pin)


def _span_minutes(ts) -> Optional[int]:
    ci, co = naive_utc(ts.get("clock_in")), naive_utc(ts.get("clock_out"))
    if not ci or not co:
        return None
    return max(0, int((co - ci).total_seconds() // 60))


# Payroll prices a punch as one run of wall-clock time from its clock-in, so a
# span of a whole day or more would wrap round to a short shift.
MAX_SPAN_MINUTES = 24 * 60


def _recheck_flags(ts, span: int) -> None:
    """Re-derive the flags that follow from the punch's times and break."""
    flags = set(_flags(ts)) - {"long_shift", "short_punch", "no_break"}
    if span > kiosk.LONG_SHIFT_HOURS * 60:
        flags.add("long_shift")
    if span < 5:
        flags.add("short_punch")
    if not int(ts.get("break_minutes") or 0) and get_minimum_break_minutes(span / 60):
        flags.add("no_break")
    if ts.get("break_source") != "auto":
        flags.discard("auto_break")
    ts["flags"] = sorted(flags)


def _overlapping_ids(sheets, now: Optional[datetime] = None) -> set:
    """Ids of punches that overlap another of the same person's (an open one
    runs to now)."""
    now = now or datetime.utcnow()
    by_emp: dict = {}
    for t in sheets:
        ci = naive_utc(t.get("clock_in"))
        if ci is None:
            continue
        co = naive_utc(t.get("clock_out")) or max(now, ci)
        by_emp.setdefault(t.get("employee_id"), []).append((ci, co, t["id"]))
    out = set()
    for spans in by_emp.values():
        spans.sort()
        for i, (_start, end, ts_id) in enumerate(spans):
            for other_start, _end, other_id in spans[i + 1:]:
                if other_start >= end:
                    break
                out.update((ts_id, other_id))
    return out


def _finish(db, ts, clock_out: datetime, shift, settings: dict, add_flags=(), declared_break: int = 0,
            save: bool = True):
    """Close an open punch: settle the break, worked time, variance and the
    review flags. Returns (worked, span) minutes."""
    ci = naive_utc(ts["clock_in"])
    flags = set(_flags(ts)) | set(add_flags)
    recorded = int(ts.get("break_minutes") or 0)
    started = naive_utc(ts.get("break_started_at"))
    if started:  # still on a break: it ends at clock-out
        recorded += max(0, int((clock_out - started).total_seconds() // 60))
    recorded += int(declared_break or 0)
    span = max(0, int((clock_out - ci).total_seconds() // 60))
    if recorded:
        brk, source = recorded, ts.get("break_source") or "recorded"
    else:
        brk, source = 0, None
        if get_minimum_break_minutes(span / 60):
            rostered = int(getattr(shift, "break_minutes", 0) or 0) if shift else 0
            if settings.get("auto_meal_break") and rostered:
                brk, source = rostered, "auto"
                flags.add("auto_break")
            else:
                flags.add("no_break")
    worked = max(0, span - brk)
    if span > kiosk.LONG_SHIFT_HOURS * 60:
        flags.add("long_shift")
    if span < 5:
        flags.add("short_punch")
    ts.update({
        "clock_out": clock_out,
        "break_minutes": brk,
        "break_source": source,
        "break_started_at": None,
        "status": "closed",
        "variance_minutes": (worked - _shift_minutes(shift)) if shift is not None else None,
        "rostered_shift_id": ts.get("rostered_shift_id") or (shift.id if shift is not None else None),
        "flags": sorted(flags),
    })
    if save:
        db.save_timesheet(ts)
    return worked, span


def _close_stale_punches(db, venue_id: str) -> list:
    """A rostered punch still open STALE_OPEN_HOURS after clock-in, whose shift
    has ended, is a forgotten clock-out: close it at the rostered end and flag
    it. Unrostered ones wait for the person (kiosk asks) or a manager."""
    now = datetime.utcnow()
    stale = [t for t in (db.list_open_timesheets(venue_id) or [])
             if t.get("rostered_shift_id")
             and now - naive_utc(t["clock_in"]) > timedelta(hours=kiosk.STALE_OPEN_HOURS)]
    if not stale:
        return []
    tz = _tz(venue_id, db)
    settings = kiosk.load_settings(db, venue_id)
    closed = []
    for ts in stale:
        shift = _matched_shift(db, venue_id, ts, tz)
        if shift is None:
            continue
        end = _utc(_shift_window(shift)[1], tz)
        if end <= naive_utc(ts["clock_in"]) or end > now:
            continue
        worked, _ = _finish(db, ts, end, shift, settings, add_flags=("auto_closed",))
        audit("timesheet.auto_close", venue_id, "timesheet", ts["id"],
              employee_id=ts["employee_id"], work_date=str(ts.get("work_date")),
              clock_out=end.isoformat(), worked_minutes=worked)
        closed.append(ts["id"])
    return closed


def _resolve_finish(hhmm: str, clock_in: datetime, now: datetime, tz) -> datetime:
    """'HH:MM' venue-local -> the first such moment after clock-in (naive UTC)."""
    try:
        h, m = (int(x) for x in hhmm.split(":"))
        if not (0 <= h <= 23 and 0 <= m <= 59):
            raise ValueError
    except ValueError:
        raise HTTPException(status_code=422, detail="Finish time must be HH:MM, e.g. 23:30")
    start_local = _local(clock_in, tz)
    cand = start_local.replace(hour=h, minute=m, second=0, microsecond=0)
    if cand <= start_local:
        cand += timedelta(days=1)
    out = _utc(cand, tz)
    if out > now:
        raise HTTPException(status_code=422, detail=(
            "That finish time hasn't happened yet. Enter when you actually finished "
            f"the shift you started {_fmt_local(clock_in, tz)}."))
    return out


def _review_state(ts, worked: Optional[int], span: Optional[int],
                  rostered_break: Optional[int] = None, overlaps: bool = False):
    """(clean, reasons). Clean = closed, within CLEAN_VARIANCE_MINUTES of the
    roster, PIN verified, an unpaid break that meets the minimum and isn't
    well over the rostered one, no overlapping punch, nothing flagged."""
    if ts.get("status") != "closed" or span is None:
        return False, []
    reasons = []
    v = ts.get("variance_minutes")
    if v is None:
        reasons.append("no rostered shift to compare")
    elif abs(int(v)) > kiosk.CLEAN_VARIANCE_MINUTES:
        reasons.append(f"{'+' if v > 0 else ''}{int(v)} min against the roster")
    if not ts.get("pin_verified"):
        reasons.append("PIN not verified")
    flags = set(_flags(ts))
    brk = int(ts.get("break_minutes") or 0)
    minimum = get_minimum_break_minutes(span / 60)
    if minimum and not brk:
        reasons.append("no break recorded")
    elif brk < minimum:
        reasons.append(f"{brk} min break, under the {minimum} min meal break")
    if rostered_break is not None and brk > rostered_break + kiosk.CLEAN_VARIANCE_MINUTES:
        reasons.append(f"{brk} min unpaid break, {rostered_break} rostered — paid rest break included?")
    if overlaps:
        reasons.append("overlaps another punch for this person")
    for f in ("finish_entered_later", "auto_closed", "manager_closed", "long_shift", "short_punch"):
        if f in flags:
            reasons.append(kiosk.FLAG_LABELS[f])
    return not reasons, reasons


def _rostered_break(shift) -> Optional[int]:
    return int(getattr(shift, "break_minutes", 0) or 0) if shift is not None else None


def _review_context(db, venue_id: str, sheets: list):
    """({shift id: rostered shift}, {ids of punches overlapping another of the
    same person's}) for judging these timesheets. Neighbouring days and open
    punches are read too, so an overnight or still-open double is seen."""
    dates = [d for d in (_as_date(t.get("work_date")) for t in sheets) if d]
    if not dates:
        return {}, set()
    lo, hi = min(dates), max(dates)
    by_emp = _shifts_by_employee(db, venue_id, lo, hi)
    shift_by_id = {s.id: s for shifts in by_emp.values() for s in shifts}
    nearby = {t["id"]: t for t in (db.get_timesheets(venue_id, lo - timedelta(days=1),
                                                      hi + timedelta(days=1)) or [])}
    for t in list(sheets) + list(db.list_open_timesheets(venue_id) or []):
        nearby.setdefault(t["id"], t)
    return shift_by_id, _overlapping_ids(nearby.values())


def _emp_name(db, venue_id, employee_id) -> str:
    for e in db.get_employees(venue_id) or []:
        if e.id == employee_id:
            return e.name
    return str(employee_id)


# ---------------------------------------------------------------------------
# Kiosk board and device
# ---------------------------------------------------------------------------

@router.get("/board")
async def clock_board(venue_id: str = Query(...)) -> dict:
    """The kiosk's home screen: every active staff member with today's
    rostered shifts and their live clock state (off / on / break / done)."""
    enforce_kiosk_or_venue_manager(venue_id)
    db = get_db()
    _close_stale_punches(db, venue_id)
    tz = _tz(venue_id, db)
    today = venue_today(venue_id, db)
    now_local = _local(datetime.utcnow(), tz)

    by_emp = _shifts_by_employee(db, venue_id, today, today)
    open_by_emp = {t["employee_id"]: t for t in (db.list_open_timesheets(venue_id) or [])}
    done = {t["employee_id"] for t in (db.get_timesheets(venue_id, today, today) or [])
            if t.get("clock_out")}
    pins = db.list_timeclock_pin_employee_ids(venue_id) or set()

    board = [
        _card(emp, by_emp.get(str(emp.id), []), open_by_emp.get(emp.id), emp.id in done,
              emp.id in pins, now_local, tz)
        for emp in (db.get_employees(venue_id) or [])
        if is_active(emp) or open_by_emp.get(emp.id)   # a leaver still clocked on can clock out
    ]
    board.sort(key=lambda b: (b["rostered"] is None, b["name"]))
    venue = db.get_venue(venue_id)
    return {"venue_id": venue_id, "venue_name": getattr(venue, "name", "") if venue else "",
            "date": today.isoformat(), "staff": board}


@router.get("/kiosk")
async def kiosk_info() -> dict:
    """Which venue and device this time clock is."""
    device_id = kiosk_device_id()
    if not device_id:
        raise HTTPException(status_code=403, detail="This isn't a time clock device.")
    db = get_db()
    device = db.get_kiosk_device(device_id) or {}
    venue = db.get_venue(device.get("venue_id"))
    return {"device_id": device_id, "device_name": device.get("name"),
            "venue_id": device.get("venue_id"),
            "venue_name": getattr(venue, "name", "") if venue else ""}


@router.post("/kiosk/exit")
async def kiosk_exit(body: KioskExitRequest) -> dict:
    """Switch this time clock off with a manager's kiosk PIN. The device token
    is revoked, so a copy of it stops working too."""
    device_id = kiosk_device_id()
    if not device_id:
        raise HTTPException(status_code=403, detail="This isn't a time clock device.")
    db = get_db()
    device = db.get_kiosk_device(device_id)
    if not device:
        raise HTTPException(status_code=404, detail="Time clock not found")
    manager_emp = kiosk.check_exit_pin(db, device, body.pin)
    kiosk.revoke_device(db, device, by=f"manager-pin:{manager_emp}")
    audit("timeclock.kiosk_exit", device["venue_id"], "kiosk_device", device_id,
          device=device.get("name"), employee_id=manager_emp)
    return {"status": "revoked", "device_id": device_id}


@router.post("/pin/check")
async def pin_check(body: PinCheckRequest) -> dict:
    """The kiosk checks a PIN before showing someone's card (shifts, actions)."""
    if not kiosk_device_id() and get_tenant_context_optional() is not None:
        raise HTTPException(status_code=403, detail="Only the venue's time clock checks PINs.")
    enforce_venue_access(body.venue_id)
    db = get_db()
    emp = _employee_or_404(db, body.venue_id, body.employee_id)
    open_ts = db.get_open_timesheet(body.venue_id, emp.id)
    if not is_active(emp) and not open_ts:
        raise HTTPException(status_code=409,
                            detail=f"{emp.name} is no longer active here — ask a manager")
    verified = kiosk.check_pin(db, body.venue_id, body.employee_id, body.pin)
    tz = _tz(body.venue_id, db)
    today = venue_today(body.venue_id, db)
    shifts = _shifts_by_employee(db, body.venue_id, today, today).get(str(emp.id), [])
    done = any(t.get("employee_id") == emp.id and t.get("clock_out")
               for t in (db.get_timesheets(body.venue_id, today, today) or []))
    card = _card(emp, shifts, open_ts, done, verified, _local(datetime.utcnow(), tz), tz)
    return {"ok": True, "pin_set": verified, "card": card}


# ---------------------------------------------------------------------------
# Punches
# ---------------------------------------------------------------------------

@router.post("/in")
async def clock_in(body: PunchRequest) -> dict:
    db = get_db()
    actor = _punch_actor(db, body.venue_id, body.employee_id)
    emp = _employee_or_404(db, body.venue_id, body.employee_id)
    if not is_active(emp):
        raise HTTPException(status_code=409,
                            detail=f"{emp.name} is no longer active here — ask a manager")
    verified = _verify_pin(db, body, actor)

    _close_stale_punches(db, body.venue_id)
    tz = _tz(body.venue_id, db)
    open_ts = db.get_open_timesheet(body.venue_id, body.employee_id)
    if open_ts:
        ci = naive_utc(open_ts["clock_in"])
        if datetime.utcnow() - ci > timedelta(hours=kiosk.STALE_OPEN_HOURS):
            raise HTTPException(status_code=409, detail=(
                f"{emp.name} is still clocked in from {_fmt_local(ci, tz)}. Enter the finish "
                "time for that shift first (Clock out → I finished at…)."))
        raise HTTPException(status_code=409, detail=f"{emp.name} is already clocked in")

    now = datetime.utcnow()
    work_date = venue_today(body.venue_id, db)
    shifts = [s for s in _shifts_by_employee(db, body.venue_id, work_date, work_date)
              .get(str(emp.id), []) if s.date == work_date]
    taken = {t.get("rostered_shift_id") for t in (db.get_timesheets(body.venue_id, work_date, work_date) or [])
             if t.get("employee_id") == emp.id and t.get("rostered_shift_id")}
    shift = _nearest_shift(shifts, _local(now, tz), taken)
    ts = {
        "id": f"ts-{uuid.uuid4().hex[:12]}",
        "venue_id": body.venue_id,
        "employee_id": body.employee_id,
        "work_date": work_date,
        "clock_in": now,
        "clock_out": None,
        "break_minutes": 0,
        "break_source": None,
        "break_started_at": None,
        "status": "open",
        "pin_verified": verified,
        "rostered_shift_id": shift.id if shift is not None else None,
        "variance_minutes": None,
        "flags": [],
        "source": actor["source"],
        "device_id": actor["device_id"],
        "created_at": now,
    }
    try:
        db.save_timesheet(ts)
    except OpenTimesheetExists:  # a second tap that reached the other worker
        raise HTTPException(status_code=409, detail=f"{emp.name} is already clocked in")
    logger.info(f"Clock IN: {emp.name} at {body.venue_id} ({ts['id']}, {actor['source']})")
    return {
        "status": "clocked_in",
        "timesheet_id": ts["id"],
        "employee": emp.name,
        "at": now.isoformat(),
        "rostered": shift is not None,
        "shift": _shift_json(shift) if shift is not None else None,
        "pin_verified": verified,
    }


@router.post("/out")
async def clock_out(body: PunchRequest) -> dict:
    db = get_db()
    actor = _punch_actor(db, body.venue_id, body.employee_id)
    emp = _employee_or_404(db, body.venue_id, body.employee_id)
    _verify_pin(db, body, actor)

    ts = db.get_open_timesheet(body.venue_id, body.employee_id)
    if not ts:
        raise HTTPException(status_code=409, detail=f"{emp.name} is not clocked in")

    tz = _tz(body.venue_id, db)
    now = datetime.utcnow()
    clock_in_at = naive_utc(ts["clock_in"])
    add = ()
    clock_out_at = now
    if body.finished_at:
        clock_out_at = _resolve_finish(body.finished_at, clock_in_at, now, tz)
        add = ("finish_entered_later",)
    shift = _matched_shift(db, body.venue_id, ts, tz)
    settings = kiosk.load_settings(db, body.venue_id)
    worked, span = _finish(db, ts, clock_out_at, shift, settings, add, body.break_minutes)
    if body.finished_at:
        audit("timesheet.finish_entered", body.venue_id, "timesheet", ts["id"],
              employee_id=emp.id, employee=emp.name, work_date=str(ts.get("work_date")),
              clock_out=clock_out_at.isoformat(), worked_minutes=worked, source=actor["source"])

    logger.info(f"Clock OUT: {emp.name} at {body.venue_id} — {worked}m worked, "
                f"variance={ts.get('variance_minutes')}")
    out = {
        "status": "clocked_out",
        "timesheet_id": ts["id"],
        "employee": emp.name,
        "at": clock_out_at.isoformat(),
        "worked_minutes": worked,
        "break_minutes": ts["break_minutes"],
        "break_source": ts.get("break_source"),
        "variance_minutes": ts.get("variance_minutes"),
        "flags": ts["flags"],
    }
    # The punch is a fact and is never blocked; odd ones are said out loud and
    # land in the review queue.
    if span < 5:
        out["warning"] = (f"Only {span} minutes worked — likely an accidental punch. A manager "
                          "can correct or approve it on the Timesheets page.")
    elif span > kiosk.LONG_SHIFT_HOURS * 60:
        out["warning"] = (f"That's a {span // 60}h {span % 60:02d}m shift. If you forgot to clock "
                          "out earlier, tell your manager — it's flagged for review.")
    if body.finished_at:
        out["note"] = "Finish time saved and flagged for your manager to check."
    return out


@router.post("/break/start")
async def break_start(body: PunchRequest) -> dict:
    db = get_db()
    actor = _punch_actor(db, body.venue_id, body.employee_id)
    emp = _employee_or_404(db, body.venue_id, body.employee_id)
    _verify_pin(db, body, actor)
    ts = db.get_open_timesheet(body.venue_id, body.employee_id)
    if not ts:
        raise HTTPException(status_code=409, detail=f"{emp.name} is not clocked in")
    if ts.get("break_started_at"):
        raise HTTPException(status_code=409, detail=f"{emp.name} is already on a break")
    now = datetime.utcnow()
    ts["break_started_at"] = now
    db.save_timesheet(ts)
    return {"status": "on_break", "employee": emp.name, "at": now.isoformat()}


@router.post("/break/end")
async def break_end(body: PunchRequest) -> dict:
    db = get_db()
    actor = _punch_actor(db, body.venue_id, body.employee_id)
    emp = _employee_or_404(db, body.venue_id, body.employee_id)
    _verify_pin(db, body, actor)
    ts = db.get_open_timesheet(body.venue_id, body.employee_id)
    if not ts or not ts.get("break_started_at"):
        raise HTTPException(status_code=409, detail=f"{emp.name} is not on a break")
    now = datetime.utcnow()
    minutes = max(0, int((now - naive_utc(ts["break_started_at"])).total_seconds() // 60))
    total = db.end_timesheet_break(ts["id"], minutes)
    if total is None:
        raise HTTPException(status_code=409, detail=f"{emp.name} is not on a break")
    return {"status": "back_on_shift", "employee": emp.name, "at": now.isoformat(),
            "break_minutes": minutes, "total_break_minutes": total}


@router.post("/pin")
async def set_pin(body: SetPinRequest) -> dict:
    """Set/replace an employee's kiosk PIN (manager action, venue-scoped).
    Replacing a PIN also lifts a lockout."""
    enforce_venue_manager(body.venue_id)
    db = get_db()
    emp = _employee_or_404(db, body.venue_id, body.employee_id)
    db.set_timeclock_pin(body.venue_id, body.employee_id,
                         kiosk.hash_pin(body.venue_id, body.employee_id, body.pin))
    audit("timeclock.pin_set", body.venue_id, "employee", emp.id, employee=emp.name, by="manager")
    return {"status": "pin_set", "employee": emp.name}


# ---------------------------------------------------------------------------
# Timesheets: review queue, corrections, approval
# ---------------------------------------------------------------------------

@router.post("/timesheets/{ts_id}/review")
async def review_timesheet(
    ts_id: str,
    body: ReviewRequest,
    user: UserContext = Depends(get_current_user),
) -> dict:
    """Manager reviews a timesheet: optionally corrects the punch times or
    break, then approves. An open punch is closed here by supplying clock_out:
    it settles like a clock-out on the kiosk (a break still running ends at the
    finish time, the meal-break rule applies), is flagged manager_closed, and
    is approved in the same step only when approve is sent explicitly.
    Corrections recompute worked time, variance and flags and are recorded
    with who/when/why — approved time is what payroll exports use."""
    enforce_venue_manager(body.venue_id)
    db = get_db()
    ts = db.get_timesheet(ts_id)
    if not ts or ts.get("venue_id") != body.venue_id:
        raise HTTPException(status_code=404, detail="Timesheet not found")
    ts = dict(ts)  # a refused correction must leave the stored punch untouched
    was_open = not ts.get("clock_out")
    if was_open and not body.clock_out:
        raise HTTPException(status_code=409, detail="Timesheet is still open — clock the employee out first (or supply a corrected clock_out)")
    if ts.get("status") == "approved":
        raise HTTPException(status_code=409, detail="Timesheet is already approved")

    before = {k: (str(ts.get(k)) if k != "break_minutes" else ts.get(k))
              for k in ("clock_in", "clock_out", "break_minutes")}
    a = naive_utc(body.clock_in) if body.clock_in is not None else naive_utc(ts["clock_in"])
    b = naive_utc(body.clock_out) if body.clock_out is not None else naive_utc(ts["clock_out"])
    adjusted = (body.clock_in is not None or body.clock_out is not None
                or (body.break_minutes is not None
                    and body.break_minutes != int(ts.get("break_minutes") or 0)))
    if adjusted and not body.note:
        raise HTTPException(status_code=422, detail="A note is required when correcting a timesheet")
    if b <= a:
        raise HTTPException(status_code=422, detail="clock_out must be after clock_in")
    if b - a >= timedelta(minutes=MAX_SPAN_MINUTES):
        raise HTTPException(status_code=422, detail=(
            "A timesheet can't run 24 hours or more — check the dates on the clock-in and clock-out."))

    tz = _tz(body.venue_id, db)
    ts["clock_in"] = a
    if body.clock_in is not None:
        moved_to = _local(a, tz).date()     # the venue-local day of the clock-in, as at the kiosk
        if moved_to != _as_date(ts.get("work_date")):
            ts["work_date"] = moved_to
            ts["rostered_shift_id"] = None
    work_date = _as_date(ts.get("work_date"))
    shift = _matched_shift(db, body.venue_id, ts, tz) if work_date else None
    if was_open:
        _finish(db, ts, b, shift, kiosk.load_settings(db, body.venue_id),
                add_flags=("manager_closed",), save=False)
    if body.break_minutes is not None and body.break_minutes != int(ts.get("break_minutes") or 0):
        ts["break_minutes"] = body.break_minutes
        ts["break_source"] = "manager"
    ts["clock_out"] = b
    span = int((b - a).total_seconds() // 60)
    worked = max(0, span - int(ts.get("break_minutes") or 0))
    if shift is not None:
        ts["rostered_shift_id"] = shift.id
    ts["variance_minutes"] = (worked - _shift_minutes(shift)) if shift is not None else None
    _recheck_flags(ts, span)

    ts["break_started_at"] = None
    if ts.get("status") == "open":
        ts["status"] = "closed"
    approve = body.approve and (not was_open or "approve" in body.model_fields_set)
    if approve:
        ts["status"] = "approved"
        ts["approved_by"] = user.user_id
        ts["approved_at"] = datetime.utcnow()
    if adjusted:
        ts["adjustment_note"] = body.note
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
    if approve:
        audit("timesheet.approve", body.venue_id, "timesheet", ts_id,
              employee_id=ts["employee_id"], employee=emp_name,
              work_date=str(work_date), worked_minutes=worked,
              break_minutes=ts.get("break_minutes"),
              variance_minutes=ts.get("variance_minutes"),
              corrected=adjusted, note=body.note)

    logger.info(
        f"Timesheet {ts_id} reviewed by {user.user_id}: "
        f"{'adjusted, ' if adjusted else ''}{'approved' if approve else 'saved'} ({worked}m)"
    )
    return {
        "status": ts["status"],
        "timesheet_id": ts_id,
        "work_date": str(work_date),
        "worked_minutes": worked,
        "break_minutes": ts.get("break_minutes"),
        "break_source": ts.get("break_source"),
        "variance_minutes": ts.get("variance_minutes"),
        "flags": ts.get("flags") or [],
        "adjusted": adjusted,
        "approved_by": ts.get("approved_by"),
    }


@router.post("/timesheets/approve")
async def approve_timesheets(
    body: BatchApproveRequest,
    user: UserContext = Depends(get_current_user),
) -> dict:
    """Approve many closed timesheets at once ('Approve all clean', or the
    rows a manager ticked). Each approval is its own audit event. With
    only_clean the server re-checks every row and skips any that isn't clean."""
    enforce_venue_manager(body.venue_id)
    db = get_db()
    names = {e.id: e.name for e in (db.get_employees(body.venue_id) or [])}
    found = {}
    for ts_id in dict.fromkeys(body.ids):
        ts = db.get_timesheet(ts_id)
        found[ts_id] = ts if ts and ts.get("venue_id") == body.venue_id else None
    shift_by_id, overlapping = _review_context(db, body.venue_id, [t for t in found.values() if t])
    approved, skipped = [], []
    for ts_id, ts in found.items():
        if not ts:
            skipped.append({"id": ts_id, "reason": "not found"})
            continue
        who = names.get(ts["employee_id"], ts["employee_id"])
        if ts.get("status") == "approved":
            skipped.append({"id": ts_id, "employee": who, "reason": "already approved"})
            continue
        if ts.get("status") != "closed" or not ts.get("clock_out"):
            skipped.append({"id": ts_id, "employee": who,
                            "reason": "still clocked in — edit it to add the finish time"})
            continue
        span = _span_minutes(ts)
        if span >= MAX_SPAN_MINUTES:
            skipped.append({"id": ts_id, "employee": who,
                            "reason": "runs 24 hours or more — edit the times first"})
            continue
        worked = max(0, span - int(ts.get("break_minutes") or 0))
        clean, reasons = _review_state(ts, worked, span,
                                       _rostered_break(shift_by_id.get(ts.get("rostered_shift_id"))),
                                       ts_id in overlapping)
        if body.only_clean and not clean:
            skipped.append({"id": ts_id, "employee": who, "reason": "; ".join(reasons)})
            continue
        ts["status"] = "approved"
        ts["approved_by"] = user.user_id
        ts["approved_at"] = datetime.utcnow()
        db.save_timesheet(ts)
        audit("timesheet.approve", body.venue_id, "timesheet", ts_id,
              employee_id=ts["employee_id"], employee=who,
              work_date=str(ts.get("work_date")), worked_minutes=worked,
              break_minutes=ts.get("break_minutes"),
              variance_minutes=ts.get("variance_minutes"),
              corrected=False, batch=True, clean=clean)
        approved.append(ts_id)
    logger.info(f"Batch approve at {body.venue_id} by {user.user_id}: "
                f"{len(approved)} approved, {len(skipped)} skipped")
    return {"approved": len(approved), "approved_ids": approved, "skipped": skipped}


@router.get("/timesheets")
async def list_timesheets(
    venue_id: str = Query(...),
    start_date: Optional[date] = Query(None),
    end_date: Optional[date] = Query(None),
    status: str = Query("", description="Filter: open | closed | approved | review"),
) -> dict:
    """Timesheets with variance, flags and whether each is clean — the review
    queue and the feed for payroll. status=open lists every open punch,
    whatever its date."""
    enforce_venue_manager_any(venue_id)
    db = get_db()
    _close_stale_punches(db, venue_id)
    if status == "open":
        sheets = db.list_open_timesheets(venue_id) or []
    else:
        if not start_date or not end_date:
            raise HTTPException(status_code=422, detail="start_date and end_date are required")
        sheets = db.get_timesheets(venue_id, start_date, end_date) or []
    emp_names = {e.id: e.name for e in (db.get_employees(venue_id) or [])}
    shift_by_id, overlapping = _review_context(db, venue_id, sheets)
    now = datetime.utcnow()

    rows = []
    total_minutes = 0
    for t in sheets:
        if status in ("closed", "approved") and t.get("status") != status:
            continue
        ci, co = naive_utc(t.get("clock_in")), naive_utc(t.get("clock_out"))
        span = _span_minutes(t)
        worked = None
        if span is not None:
            worked = max(0, span - int(t.get("break_minutes") or 0))
        shift = shift_by_id.get(t.get("rostered_shift_id"))
        clean, reasons = _review_state(t, worked, span, _rostered_break(shift), t["id"] in overlapping)
        if status == "review" and (t.get("status") != "closed" or clean):
            continue
        total_minutes += worked or 0
        flags = _flags(t)
        rows.append({
            "id": t["id"],
            "employee_id": t["employee_id"],
            "employee_name": emp_names.get(t["employee_id"], t["employee_id"]),
            "work_date": str(t["work_date"]),
            "clock_in": ci.isoformat() if ci else None,
            "clock_out": co.isoformat() if co else None,
            "break_minutes": t.get("break_minutes", 0),
            "break_source": t.get("break_source"),
            "on_break": bool(t.get("break_started_at")) and not co,
            "worked_minutes": worked,
            "open_minutes": int((now - ci).total_seconds() // 60) if ci and not co else None,
            "variance_minutes": t.get("variance_minutes"),
            "rostered": ({"start": shift.start_time.strftime("%H:%M"),
                          "end": shift.end_time.strftime("%H:%M"),
                          "break_minutes": int(shift.break_minutes or 0)} if shift else None),
            "status": t.get("status"),
            "pin_verified": t.get("pin_verified", False),
            "source": t.get("source") or "kiosk",
            "flags": flags,
            "flag_labels": [kiosk.FLAG_LABELS.get(f, f) for f in flags],
            "clean": clean,
            "review_reasons": reasons,
            "approved_by": t.get("approved_by"),
            "adjustment_note": t.get("adjustment_note"),
        })
    return {
        "venue_id": venue_id,
        "start_date": start_date.isoformat() if start_date else None,
        "end_date": end_date.isoformat() if end_date else None,
        "count": len(rows),
        "clean_count": sum(1 for r in rows if r["clean"]),
        "total_worked_minutes": total_minutes,
        "timesheets": rows,
    }
