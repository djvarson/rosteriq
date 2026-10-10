"""
Leave and the roster: what approving leave does to the person's shifts.

Approved leave leaves each of the person's upcoming shifts in the window OPEN:
the shift stays on the roster with its role, times and cost, its employee_id
becomes "" (nobody yet), it goes on the cover board for people who can work
that role, it drops off the person's /my, and the Today briefing lists it
until someone is approved onto it. A shift that has already started stays as
it was (history).

Every roster of those weeks that hasn't been superseded is updated — the one
staff see (published, else imported), the one managers see, and a newer draft
not yet published — so publishing that draft can't put the person back on
their leave day. Only the roster staff can see gets covers straight away; an
open shift on any other roster goes on the cover board when that roster is
published (post_open_covers). See services/current_roster.py.

A cover stands while its shift is still on the roster staff see with the
day, times and area it was posted with, held by the person who asked (or by
nobody, once it is open). One that doesn't is withdrawn, never approved.
"""

import logging
import uuid
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from typing import Iterable, Optional

from rosteriq.database import _jsonb
from rosteriq.middleware.tenant import scoped_employee_map
from rosteriq.services.clock import venue_now
from rosteriq.services.current_roster import (
    SUPERSEDED, live_rosters, live_shifts, roster_state, week_monday,
)

logger = logging.getLogger(__name__)

OPEN = ""

LEAVE_TYPES = {
    "annual": "Annual leave",
    "personal": "Personal/carer's leave",
    "unpaid": "Unpaid leave",
    "unavailable": "Unavailable",
    "other": "Other",
}
# Leave the payroll advisor pays: shown in payroll prep as hours, never priced.
PAID_LEAVE_TYPES = frozenset({"annual", "personal"})
LEAVE_TYPE_PATTERN = "^(" + "|".join(LEAVE_TYPES) + ")$"

_DEAD = ("cancelled", "no_show")
LIVE_COVER = ("open", "claimed")


def is_open(shift) -> bool:
    return not getattr(shift, "employee_id", None)


def leave_type_label(code) -> str:
    return LEAVE_TYPES.get(str(code or "other"), LEAVE_TYPES["other"])


def _clock(t):
    h12 = t.hour % 12 or 12
    text = f"{h12}:{t.minute:02d}" if t.minute else f"{h12}"
    return text, ("am" if t.hour < 12 else "pm")


def span(start, end) -> str:
    """16:00-23:00 -> '4-11pm'; 10:00-18:00 -> '10am-6pm'."""
    s, s_half = _clock(start)
    e, e_half = _clock(end)
    if s_half == e_half:
        return f"{s}-{e}{e_half}"
    return f"{s}{s_half}-{e}{e_half}"


def shift_label(shift) -> str:
    """'Sat 4-11pm Duty Manager'."""
    return f"{shift.date.strftime('%a')} {span(shift.start_time, shift.end_time)} {shift.role}".strip()


def _status(shift) -> str:
    return str(getattr(shift.status, "value", shift.status))


def _started(shift, now: datetime) -> bool:
    return datetime.combine(shift.date, shift.start_time) <= now


def _venue_now(db, venue_id) -> datetime:
    now = venue_now(venue_id, db)
    return now.replace(tzinfo=None) if getattr(now, "tzinfo", None) else now


def _staff_roster_ids(db, venue_id: str, start: date, end: date) -> set:
    return {r.id for r in live_rosters(db, venue_id, start, end, staff=True)}


def unsuperseded_rosters(db, venue_id: str, start: date, end: date) -> list:
    """Every roster of the weeks overlapping [start, end] that hasn't been
    superseded: the one staff see, the managers' one and any newer draft."""
    return [r for r in db.get_rosters_by_date_range(venue_id, start, end) or []
            if getattr(r, "venue_id", None) == venue_id
            and roster_state(db, r.id) != SUPERSEDED]


def _mine(roster, employee_id: str, start: date, end: date) -> list:
    return [s for s in roster.shifts or []
            if str(s.employee_id) == str(employee_id) and start <= s.date <= end
            and _status(s) not in _DEAD]


def shift_slot(shift) -> tuple:
    return (shift.date, shift.start_time, shift.end_time, (shift.role or "").strip().lower())


def _hours(value: float) -> str:
    return f"{round(value, 2):g}h"


def _shift_end(shift) -> datetime:
    return datetime.combine(shift.date, shift.start_time) + timedelta(hours=shift.duration_hours)


def _punched_hours(sheet: dict, until: Optional[datetime]) -> Optional[float]:
    """Hours on one punch less its break. Punches are stored as naive UTC; one
    not clocked out runs to `until`, or is None (unknown) without one."""
    ci, co = sheet.get("clock_in"), sheet.get("clock_out")
    if not ci:
        return 0.0
    if not co and until is None:
        return None
    try:
        a = ci if isinstance(ci, datetime) else datetime.fromisoformat(str(ci).replace("Z", "+00:00"))
        if not co:
            b = until
        else:
            b = co if isinstance(co, datetime) else datetime.fromisoformat(str(co).replace("Z", "+00:00"))
    except ValueError:
        return 0.0
    a, b = a.replace(tzinfo=None), b.replace(tzinfo=None)
    minutes = (b - a).total_seconds() / 60 - int(sheet.get("break_minutes") or 0)
    return max(0.0, minutes / 60)


def _finish_guessed(sheet: dict) -> bool:
    """Closed by the time clock at the rostered end, and since neither
    corrected nor approved by a manager (the flag stays after a correction)."""
    return ("auto_closed" in (_jsonb(sheet.get("flags"), []) or [])
            and sheet.get("status") != "approved" and not sheet.get("adjustment_note"))


def _worked_by_day(db, venue_id: str, employee_id: str, days: set, running: set,
                   now_utc: datetime) -> tuple:
    """(day -> hours this person punched at the venue that day, days with a
    punch never clocked out, days with a punch the time clock closed at the
    rostered end). A punch still open while that day's shift is running
    counts up to now; once the shift is over nobody knows when they left, so
    it isn't counted, and neither is a finish the time clock guessed."""
    if not days:
        return {}, set(), set()
    try:
        sheets = db.get_timesheets(venue_id, min(days), max(days)) or []
    except Exception:
        return {}, set(), set()
    out: dict = {}
    unclosed, guessed = set(), set()
    for t in sheets:
        day = _as_date(t.get("work_date"))
        if str(t.get("employee_id")) != str(employee_id) or day not in days:
            continue
        if _finish_guessed(t):
            guessed.add(day)
            continue
        hours = _punched_hours(t, now_utc if day in running else None)
        if hours is None:
            unclosed.add(day)
        else:
            out[day] = out.get(day, 0.0) + hours
    return out, unclosed, guessed


def leave_impact(db, venue_id: str, employee, start: date, end: date) -> dict:
    """What approving leave for these dates would do, in words a manager reads
    in the approve dialog: 'Jo is rostered Sat 4-11pm as Duty Manager; this
    shift will be left open'.

    Hours come from one roster per week (the one staff see, else the managers'
    draft), so a week that has both never counts a day twice. A shift that has
    started or passed adds only the rostered hours the person didn't punch
    that day."""
    local = venue_now(venue_id, db)
    now = local.replace(tzinfo=None) if local.tzinfo else local
    now_utc = local.astimezone(timezone.utc).replace(tzinfo=None) if local.tzinfo else datetime.utcnow()
    name = getattr(employee, "name", None) or "They"
    counted = live_rosters(db, venue_id, start, end, staff=True)
    staff_weeks = {week_monday(r.week_start) for r in counted}
    counted += [r for r in live_rosters(db, venue_id, start, end)
                if week_monday(r.week_start) not in staff_weeks]
    counted_ids = {r.id for r in counted}
    hour_shifts = [s for r in counted for s in _mine(r, employee.id, start, end)]

    slots = {}
    for s in hour_shifts:
        slots.setdefault(shift_slot(s), s)
    for r in unsuperseded_rosters(db, venue_id, start, end):
        if r.id not in counted_ids:
            for s in _mine(r, employee.id, start, end):
                slots.setdefault(shift_slot(s), s)

    started: dict = {}
    running = set()
    for s in hour_shifts:
        if _started(s, now):
            started[s.date] = started.get(s.date, 0.0) + float(s.net_hours)
            if now < _shift_end(s):
                running.add(s.date)
    worked, unclosed, guessed = _worked_by_day(db, venue_id, employee.id, set(started), running, now_utc)
    leave_hours = sum(float(s.net_hours) for s in hour_shifts if not _started(s, now))
    leave_hours += sum(max(0.0, h - worked.get(d, 0.0)) for d, h in started.items())
    hour_ids = {s.id for s in hour_shifts}

    shifts = []
    for key in sorted(slots):
        s = slots[key]
        when = f"{s.date.strftime('%a')} {span(s.start_time, s.end_time)}"
        past = _started(s, now)
        if past:
            message = (f"{name} was rostered {when} as {s.role}; that shift has started "
                       "or passed, so it stays on the roster as it was")
            if s.id in hour_ids:
                did = worked.get(s.date, 0.0)
                left = _hours(max(0.0, started[s.date] - did))
                how = ("never clocked out" if s.date in unclosed else
                       "closed automatically at the rostered end" if s.date in guessed else None)
                if how and did:
                    message += (f" — they worked {_hours(did)} that day besides a punch {how}, "
                                f"which isn't counted, so {left} of it counts as leave: check the hours")
                elif how:
                    message += (f" — their punch that day was {how}, so it isn't "
                                f"counted and its {left} counts as leave: check the hours")
                elif did:
                    message += f" — they worked {_hours(did)} that day, so {left} of it counts as leave"
                else:
                    message += f" — nothing was punched that day, so its {left} counts as leave"
        else:
            message = f"{name} is rostered {when} as {s.role}; this shift will be left open"
        shifts.append({
            "shift_id": s.id, "date": s.date.isoformat(), "day": s.date.strftime("%a"),
            "start": s.start_time.strftime("%H:%M"), "end": s.end_time.strftime("%H:%M"),
            "role": s.role, "hours": round(float(s.net_hours), 2),
            "past": past, "message": message,
        })
    will_open = [x for x in shifts if not x["past"]]
    if not shifts:
        summary = f"{name} has no rostered shifts in those dates."
    else:
        summary = "; ".join(x["message"] for x in shifts) + "."
    return {
        "employee_id": employee.id, "employee_name": getattr(employee, "name", None),
        "start_date": start.isoformat(), "end_date": end.isoformat(),
        "shifts": shifts, "will_open_count": len(will_open),
        "rostered_hours": round(sum(float(s.net_hours) for s in hour_shifts), 2),
        "leave_hours": round(leave_hours, 2),
        "message": summary,
    }


def default_paid_hours(impact: dict) -> Optional[float]:
    """Paid leave defaults to the rostered hours the leave keeps the person
    from working (None when there are none — the manager records the figure)."""
    hours = impact.get("leave_hours") or 0
    return round(float(hours), 2) if hours else None


def _new_cover(venue_id: str, shift, requested_by: str, origin: str) -> dict:
    return {
        "id": f"cv-{uuid.uuid4().hex[:10]}",
        "venue_id": venue_id,
        "shift_id": shift.id,
        "shift_date": shift.date,
        "shift_start": shift.start_time.strftime("%H:%M"),
        "shift_end": shift.end_time.strftime("%H:%M"),
        "role": shift.role,
        "requested_by": requested_by,
        "reason": "",
        "origin": origin,
        "claimed_by": None,
        "status": "open",
        "created_at": datetime.utcnow(),
    }


def _open_on(db, roster_id: str, venue_id: str, employee, start: date, end: date,
             now: datetime, emp_map: dict, state) -> list:
    """Re-read one roster under its lock, open the person's not-yet-started
    shifts in the window, recost and save it. Returns the shifts opened."""
    from rosteriq.services import roster_edit as edit  # roster_edit imports this module

    with edit.roster_lock(db, roster_id):
        roster = db.get_roster(roster_id)
        if roster is None or getattr(roster, "venue_id", None) != venue_id:
            return []
        roster = roster.model_copy(deep=True)
        leaving = [s for s in _mine(roster, employee.id, start, end) if not _started(s, now)]
        if leaving:
            for s in leaving:
                s.employee_id = OPEN
            edit.recost_total(roster, emp_map, state)
            db.save_roster(roster)
        return leaving


def open_leave_shifts(db, venue_id: str, employee, start: date, end: date) -> list:
    """Leave each of the person's not-yet-started shifts in the window open on
    every roster of those weeks that isn't superseded, and post the ones on
    the roster staff see to the cover board. Each roster is re-read, changed
    and saved under its lock; RosterBusy when another save holds the lock of
    a roster staff see. Returns one entry per shift slot opened (for the
    leave record, the response and the audit trail)."""
    from rosteriq.services import roster_edit as edit  # roster_edit imports this module

    now = _venue_now(db, venue_id)
    staff_ids = _staff_roster_ids(db, venue_id, start, end)
    rosters = sorted(unsuperseded_rosters(db, venue_id, start, end),
                     key=lambda r: (r.id not in staff_ids, r.week_start, str(r.id)))
    covered = {c.get("shift_id"): c.get("id") for c in db.list_shift_covers(venue_id) or []
               if c.get("status") in LIVE_COVER}
    emp_map = scoped_employee_map(db, venue_id)
    state = edit.venue_state(db.get_venue(venue_id))
    opened = {}
    for rid in [r.id for r in rosters]:
        try:
            leaving = _open_on(db, rid, venue_id, employee, start, end, now, emp_map, state)
        except edit.RosterBusy:
            if rid in staff_ids:
                raise
            # A draft mid-save keeps the shift; publishing it is refused while
            # it rosters the person on their leave.
            logger.warning(f"Roster {rid} busy: leave for {employee.id} not applied to it")
            continue
        for s in leaving:
            entry = opened.setdefault(shift_slot(s), {
                "shift_id": s.id, "roster_id": rid,
                "date": s.date.isoformat(), "start": s.start_time.strftime("%H:%M"),
                "end": s.end_time.strftime("%H:%M"), "role": s.role,
                "hours": round(float(s.net_hours), 2), "label": shift_label(s),
                "cover_id": None, "shift_ids": [],
            })
            entry["shift_ids"].append(s.id)
            if rid not in staff_ids:
                continue
            entry["shift_id"], entry["roster_id"] = s.id, rid
            if s.id not in covered:
                cover = _new_cover(venue_id, s, employee.id, "leave")
                db.save_shift_cover(cover)
                covered[s.id] = cover["id"]
            entry["cover_id"] = covered[s.id]
    return [opened[k] for k in sorted(opened)]


def opened_summary(opened: list) -> str:
    """'Left open and posted for cover: Sat 4-11pm Duty Manager.' A shift on a
    roster staff can't see yet is left open and goes on the cover board when
    that roster is published."""
    posted = [o["label"] for o in opened if o.get("cover_id")]
    waiting = [o["label"] for o in opened if not o.get("cover_id")]
    parts = []
    if posted:
        parts.append(f"Left open and posted for cover: {', '.join(posted)}.")
    if waiting:
        parts.append(f"Left open: {', '.join(waiting)} — "
                     + ("it goes" if len(waiting) == 1 else "they go")
                     + " on the cover board when the roster is published.")
    return " ".join(parts)


def cover_day(cover: dict) -> Optional[date]:
    return _as_date(cover.get("shift_date"))


def cover_holders(cover: dict) -> set:
    """Who may hold the shift while the cover stands: nobody, for a shift left
    open; for a staff member's own request, them, or nobody once their
    approved leave has left it open (the leave reuses their cover)."""
    if cover.get("origin") in ("leave", "open"):
        return {OPEN}
    return {OPEN, str(cover.get("requested_by") or "")}


def cover_stands(cover: dict, shift) -> bool:
    """Is `shift` still the one the cover was posted for: same day, times and
    area, held by one of cover_holders?"""
    if shift is None or getattr(shift, "id", None) != cover.get("shift_id"):
        return False
    return (str(shift.employee_id or "") in cover_holders(cover)
            and shift.date == cover_day(cover)
            and shift.start_time.strftime("%H:%M") == str(cover.get("shift_start") or "")[:5]
            and shift.end_time.strftime("%H:%M") == str(cover.get("shift_end") or "")[:5]
            and (shift.role or "").strip().lower() == str(cover.get("role") or "").strip().lower())


def standing_cover_ids(db, venue_id: str, covers: Iterable[dict]) -> set:
    """Ids of the open or claimed covers whose shift still stands on the
    roster staff see."""
    weeks: dict = {}
    out = set()
    for c in covers or []:
        day = cover_day(c)
        if c.get("status") not in LIVE_COVER or day is None:
            continue
        monday = week_monday(day)
        if monday not in weeks:
            weeks[monday] = {s.id: s for s in live_shifts(
                db, venue_id, monday, monday + timedelta(days=6), staff=True)}
        if cover_stands(c, weeks[monday].get(c.get("shift_id"))):
            out.add(c.get("id"))
    return out


def withdraw_cover(db, cover: dict, note: str, actor_id: Optional[str] = None) -> dict:
    cover.update(status="withdrawn", decided_by=actor_id, decided_at=datetime.utcnow(),
                 decision_note=note)
    db.save_shift_cover(cover)
    return cover


def post_open_covers(db, roster, now: datetime = None) -> list:
    """A roster has just become the one staff see: each open shift on it that
    hasn't started and has no open or claimed cover goes on the cover board
    (origin 'leave' when approved leave left it open, else 'open'), and the
    open or claimed covers of that week whose shift isn't on it are withdrawn.
    Returns the covers posted."""
    venue_id = roster.venue_id
    now = now or _venue_now(db, venue_id)
    covers = list(db.list_shift_covers(venue_id) or [])
    on_roster = {s.id for s in roster.shifts or []}
    for c in covers:
        day = cover_day(c)
        if (c.get("status") in LIVE_COVER and c.get("shift_id") not in on_roster
                and day is not None and roster.week_start <= day <= roster.week_end):
            withdraw_cover(db, c, "The week's roster was replaced and this shift isn't on it.")
    covered = {c.get("shift_id") for c in covers if c.get("status") in LIVE_COVER}
    left_by = {}
    for req in db.list_leave_requests(venue_id) or []:
        for item in req.get("opened_shifts") or []:
            for sid in item.get("shift_ids") or [item.get("shift_id")]:
                left_by[sid] = req.get("employee_id")
    posted = []
    for s in roster.shifts or []:
        if not is_open(s) or _status(s) in _DEAD or _started(s, now) or s.id in covered:
            continue
        who = left_by.get(s.id)
        cover = _new_cover(venue_id, s, who or OPEN, "leave" if who else "open")
        db.save_shift_cover(cover)
        covered.add(s.id)
        posted.append(cover)
    return posted


def open_shift_notices(db, venue_id: str, start: date, end: date, now: datetime = None) -> list:
    """Open shifts from start to end on the managers' roster, soonest first:
    [{'shift_id', 'date', 'label', 'after_leave', 'message'}]."""
    now = now or _venue_now(db, venue_id)
    leave_shift_ids = {c.get("shift_id") for c in (db.list_shift_covers(venue_id) or [])
                       if c.get("origin") == "leave"}
    for req in db.list_leave_requests(venue_id) or []:
        for item in req.get("opened_shifts") or []:
            leave_shift_ids.update(item.get("shift_ids") or [item.get("shift_id")])
    out = []
    for roster in live_rosters(db, venue_id, start, end):
        for s in roster.shifts or []:
            if not is_open(s) or not (start <= s.date <= end) or _status(s) in _DEAD:
                continue
            end_dt = datetime.combine(s.date, s.start_time) + timedelta(hours=s.duration_hours)
            if end_dt <= now:
                continue
            after_leave = s.id in leave_shift_ids
            label = shift_label(s)
            out.append({
                "shift_id": s.id, "date": s.date.isoformat(),
                "start": s.start_time.strftime("%H:%M"), "role": s.role,
                "label": label, "after_leave": after_leave,
                "message": f"{label} is open" + (" after leave" if after_leave else ""),
            })
    out.sort(key=lambda x: (x["date"], x["start"]))
    return out


def leave_hours_in_period(req: dict, period_start: date, period_end: date) -> Optional[Decimal]:
    """A paid leave request's recorded hours that fall in a pay period, spread
    evenly over the leave's days when it straddles the period (advisor to
    confirm). None when no hours were recorded."""
    hours = req.get("paid_hours")
    if hours is None:
        return None
    start, end = _as_date(req.get("start_date")), _as_date(req.get("end_date"))
    if start is None or end is None or end < start:
        return None
    lo, hi = max(start, period_start), min(end, period_end)
    if hi < lo:
        return Decimal("0")
    days = (end - start).days + 1
    inside = (hi - lo).days + 1
    return (Decimal(str(hours)) * Decimal(inside) / Decimal(days)).quantize(Decimal("0.01"))


def _as_date(v) -> Optional[date]:
    if isinstance(v, datetime):
        return v.date()
    if isinstance(v, date):
        return v
    try:
        return date.fromisoformat(str(v)[:10])
    except (TypeError, ValueError):
        return None


def approved_paid_leave(requests: Iterable[dict], period_start: date, period_end: date) -> dict:
    """employee_id -> [(leave_type, hours or None, request)] for approved paid
    leave overlapping the period."""
    out: dict = {}
    for req in requests or []:
        if req.get("status") != "approved" or req.get("leave_type") not in PAID_LEAVE_TYPES:
            continue
        start, end = _as_date(req.get("start_date")), _as_date(req.get("end_date"))
        if start is None or end is None or end < period_start or start > period_end:
            continue
        out.setdefault(req.get("employee_id"), []).append(
            (req.get("leave_type"), leave_hours_in_period(req, period_start, period_end), req))
    return out


def apply_approved_leave(db, venue_id: str, req: dict, employee) -> list:
    """Approve-time effects on the roster for an approved leave request:
    opens the person's upcoming shifts in the window (recorded on the request
    as opened_shifts) and, for paid leave with no hours recorded, records the
    hours they were rostered. Returns the opened shifts."""
    start, end = _as_date(req.get("start_date")), _as_date(req.get("end_date"))
    if req.get("leave_type") not in PAID_LEAVE_TYPES:
        req["paid_hours"] = None
    if employee is None or start is None or end is None:
        req.setdefault("opened_shifts", [])
        return []
    if req.get("leave_type") in PAID_LEAVE_TYPES and req.get("paid_hours") is None:
        req["paid_hours"] = default_paid_hours(leave_impact(db, venue_id, employee, start, end))
    opened = open_leave_shifts(db, venue_id, employee, start, end)
    req["opened_shifts"] = opened
    return opened
