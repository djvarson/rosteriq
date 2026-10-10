"""
Leave and the roster: what approving leave does to the person's shifts.

Approved leave leaves each of the person's upcoming shifts in the window OPEN:
the shift stays on the roster with its role, times and cost, its employee_id
becomes "" (nobody yet), it goes on the cover board for people who can work
that role, it drops off the person's /my, and the Today briefing lists it
until someone is approved onto it. A shift that has already started stays as
it was (history).

Both rosters a week can show are updated: the one managers see (published,
else the newest) and the one staff see (published, else imported) — see
services/current_roster.py.
"""

import uuid
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Iterable, Optional

from rosteriq.services.clock import venue_now
from rosteriq.services.current_roster import live_rosters

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


def _week_rosters(db, venue_id: str, start: date, end: date) -> list:
    """Every roster a reader sees for these weeks: managers' and staff's."""
    seen = {}
    for staff in (False, True):
        for roster in live_rosters(db, venue_id, start, end, staff=staff):
            seen.setdefault(roster.id, roster)
    return list(seen.values())


def _staff_roster_ids(db, venue_id: str, start: date, end: date) -> set:
    return {r.id for r in live_rosters(db, venue_id, start, end, staff=True)}


def _person_shifts(db, venue_id: str, employee_id: str, start: date, end: date):
    """[(roster, shift)] of this person's shifts dated inside [start, end]."""
    out = []
    for roster in _week_rosters(db, venue_id, start, end):
        for s in roster.shifts or []:
            if (str(s.employee_id) == str(employee_id) and start <= s.date <= end
                    and _status(s) not in _DEAD):
                out.append((roster, s))
    return out


def _slot(shift) -> tuple:
    return (shift.date, shift.start_time, shift.end_time, (shift.role or "").strip().lower())


def leave_impact(db, venue_id: str, employee, start: date, end: date) -> dict:
    """What approving leave for these dates would do, in words a manager reads
    in the approve dialog: 'Jo is rostered Sat 4-11pm as Duty Manager; this
    shift will be left open'."""
    now = _venue_now(db, venue_id)
    name = getattr(employee, "name", None) or "They"
    slots = {}
    for _roster, s in _person_shifts(db, venue_id, employee.id, start, end):
        slots.setdefault(_slot(s), s)
    shifts = []
    for key in sorted(slots):
        s = slots[key]
        when = f"{s.date.strftime('%a')} {span(s.start_time, s.end_time)}"
        past = _started(s, now)
        if past:
            message = (f"{name} was rostered {when} as {s.role}; that shift has started "
                       "or passed, so it stays on the roster as it was")
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
        "rostered_hours": round(sum(x["hours"] for x in shifts), 2),
        "message": summary,
    }


def default_paid_hours(impact: dict) -> Optional[float]:
    """Paid leave defaults to the hours the person was rostered in the window
    (None when they had none — the manager records the figure)."""
    hours = impact.get("rostered_hours") or 0
    return round(float(hours), 2) if hours else None


def open_leave_shifts(db, venue_id: str, employee, start: date, end: date) -> list:
    """Leave each of the person's not-yet-started shifts in the window open and
    post the staff-visible ones to the cover board. Returns one entry per shift
    slot opened (for the leave record, the response and the audit trail)."""
    now = _venue_now(db, venue_id)
    staff_ids = _staff_roster_ids(db, venue_id, start, end)
    covers = list(db.list_shift_covers(venue_id) or [])
    covered = {c.get("shift_id") for c in covers if c.get("status") in ("open", "claimed")}
    touched = {}
    opened = {}
    for roster, s in _person_shifts(db, venue_id, employee.id, start, end):
        if _started(s, now):
            continue
        s.employee_id = OPEN
        touched[roster.id] = roster
        entry = opened.setdefault(_slot(s), {
            "shift_id": s.id, "roster_id": roster.id,
            "date": s.date.isoformat(), "start": s.start_time.strftime("%H:%M"),
            "end": s.end_time.strftime("%H:%M"), "role": s.role,
            "hours": round(float(s.net_hours), 2), "label": shift_label(s),
            "cover_id": None, "shift_ids": [],
        })
        entry["shift_ids"].append(s.id)
        if roster.id in staff_ids:
            entry["shift_id"], entry["roster_id"] = s.id, roster.id
            if s.id not in covered:
                cover = {
                    "id": f"cv-{uuid.uuid4().hex[:10]}",
                    "venue_id": venue_id,
                    "shift_id": s.id,
                    "shift_date": s.date,
                    "shift_start": s.start_time.strftime("%H:%M"),
                    "shift_end": s.end_time.strftime("%H:%M"),
                    "role": s.role,
                    "requested_by": employee.id,
                    "reason": "",
                    "origin": "leave",
                    "claimed_by": None,
                    "status": "open",
                    "created_at": datetime.utcnow(),
                }
                db.save_shift_cover(cover)
                covered.add(s.id)
                entry["cover_id"] = cover["id"]
    for roster in touched.values():
        db.save_roster(roster)
    return [opened[k] for k in sorted(opened)]


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
