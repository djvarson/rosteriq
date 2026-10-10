"""
SOS — who can a duty manager call in, right now?

One answer for the On-Shift "SOS — who can I call?" card (GET /api/sos) and
the AI's find_available_staff tool, so the two never disagree.

For a window on one day (default: now until the venue closes):

* inactive staff never appear; salaried people are left out unless asked for
* approved leave covering the day takes a person off the list (named under
  on_leave, never with their contact details)
* "already on" means a shift HERE that overlaps the window (with its times);
  someone who finished at 5pm is still callable at 7pm
* "on elsewhere" means a shift at another venue the caller holds that
  overlaps the window (services/person_identity, when present)
* availability is read through services/availability_rules; people marked
  unavailable are returned only when include_unavailable is set, flagged
* each person carries their rostered hours this week and, when a fortnightly
  visa cap was RECORDED, where they stand against it (services/visa)
* casuals first, then part-time, then full-time
"""

from __future__ import annotations

import importlib
import logging
from datetime import date, datetime, time, timedelta
from typing import Iterable, List, Optional

from rosteriq.models import normalise_area
from rosteriq.services.availability_rules import is_available
from rosteriq.services.current_roster import live_shifts, week_monday
from rosteriq.services.venue_areas import can_fill, find_area, is_closed, trading_window, venue_areas
from rosteriq.services.visa import cap_headroom

logger = logging.getLogger(__name__)

TYPE_ORDER = {"casual": 0, "part_time": 1, "full_time": 2}
CALL_IN_ROUNDING_MIN = 30
DEFAULT_CALL_IN_HOURS = 4
_NOT_WORKING = ("cancelled", "no_show")


def _text(value) -> str:
    return str(getattr(value, "value", value) or "")


def is_active(emp) -> bool:
    return getattr(emp, "active", True) is not False


def is_salaried(emp) -> bool:
    return _text(getattr(emp, "pay_basis", "hourly")).lower() == "salaried"


def employment_type(emp) -> str:
    return _text(getattr(emp, "employment_type", "casual")).lower() or "casual"


def dial_number(phone) -> Optional[str]:
    """A recorded phone number as a tel:/sms: target: digits and a leading +."""
    raw = str(phone or "").strip()
    digits = "".join(ch for ch in raw if ch.isdigit())
    if not digits:
        return None
    return ("+" if raw.startswith("+") else "") + digits


def _hhmm(value) -> Optional[time]:
    if value is None or value == "":
        return None
    if isinstance(value, time):
        return value.replace(second=0, microsecond=0)
    text = str(value).strip()[:5]
    try:
        hh, mm = (text.split(":") + ["0"])[:2]
        h, m = int(hh), int(mm)
    except ValueError:
        raise ValueError(f"time must be HH:MM, got {value!r}")
    if h == 24 and m == 0:
        return time(0, 0)
    if not (0 <= h <= 23 and 0 <= m <= 59):
        raise ValueError(f"time must be HH:MM, got {value!r}")
    return time(h, m)


def shift_span(shift):
    start = datetime.combine(shift.date, shift.start_time)
    return start, start + timedelta(hours=shift.duration_hours)


def _working(shift) -> bool:
    return _text(getattr(shift, "status", "")).lower() not in _NOT_WORKING


def _round_up(dt: datetime, minutes: int) -> datetime:
    dt = dt.replace(second=0, microsecond=0)
    extra = (-dt.minute) % minutes
    return dt + timedelta(minutes=extra)


def default_window(venue, now: datetime):
    """Now (rounded up to the half hour) until close. (start, end, note)."""
    now = now.replace(tzinfo=None)
    start = _round_up(now, CALL_IN_ROUNDING_MIN)
    windows = [w for w in (trading_window(venue, now.date() - timedelta(days=1)),
                           trading_window(venue, now.date())) if w]
    for opens, closes in windows:
        if opens <= now < closes and start < closes:
            return start, closes, None
    for opens, closes in windows:
        if start < opens:
            return opens, closes, None
    fallback = (start, start + timedelta(hours=DEFAULT_CALL_IN_HOURS))
    if windows:
        return (*fallback, f"Trading has finished for today, so the call-in is taken as "
                           f"{DEFAULT_CALL_IN_HOURS} hours from {start:%H:%M}.")
    if is_closed(venue, now.date()):
        return (*fallback, f"Today is recorded as a closed day, so the call-in is taken as "
                           f"{DEFAULT_CALL_IN_HOURS} hours from {start:%H:%M}.")
    return (*fallback, f"No trading hours are recorded for today, so the call-in is taken as "
                       f"{DEFAULT_CALL_IN_HOURS} hours from {start:%H:%M}.")


def _clock_label(t: time, with_suffix: bool) -> str:
    if t.hour == 0 and t.minute == 0:
        return "midnight"
    h12 = t.hour % 12 or 12
    text = f"{h12}" if t.minute == 0 else f"{h12}:{t.minute:02d}"
    return text + (("am" if t.hour < 12 else "pm") if with_suffix else "")


def friendly_range(start: datetime, end: datetime) -> str:
    """'7-11pm', '7:30-11pm', '11am-3pm', '10pm-1am', '7pm-midnight'."""
    s, e = start.time(), end.time()
    same_half = (s.hour < 12) == (e.hour < 12) and end.date() == start.date() and not (e.hour == 0 and e.minute == 0)
    return f"{_clock_label(s, not same_half)}-{_clock_label(e, True)}"


def call_in_message(venue_name: str, start: datetime, end: datetime, today: date) -> str:
    day = start.date()
    evening = start.hour >= 17
    if day == today:
        when = "tonight" if evening else "today"
    elif day == today + timedelta(days=1):
        when = "tomorrow night" if evening else "tomorrow"
    else:
        when = f"on {day.strftime('%a')} {day.day} {day.strftime('%b')}"
    return f"Can you come in {friendly_range(start, end)} {when}? — {venue_name}"


def approved_leave(db, venue_id: str) -> dict:
    """employee_id -> [(start, end)] of APPROVED leave."""
    out: dict = {}
    try:
        rows = db.list_leave_requests(venue_id) or []
    except Exception as e:  # noqa: BLE001 — a leave lookup must not sink the call-out
        logger.warning("SOS leave lookup failed for %s: %s", venue_id, e)
        return out
    for r in rows:
        if r.get("status") != "approved":
            continue
        try:
            rs, re_ = r.get("start_date"), r.get("end_date")
            rs = rs if isinstance(rs, date) else date.fromisoformat(str(rs)[:10])
            re_ = re_ if isinstance(re_, date) else date.fromisoformat(str(re_)[:10])
        except ValueError:
            continue
        out.setdefault(str(r.get("employee_id")), []).append((rs, re_))
    return out


def _person_identity():
    try:
        return importlib.import_module("rosteriq.services.person_identity")
    except ImportError:
        return None


def _shared_emails(db, venue_id: str) -> set:
    """Emails that also appear on a staff record at another venue the caller
    holds — only those people can be on somewhere else."""
    if _person_identity() is None:
        return set()
    from rosteriq.middleware.tenant import scoped_venue_ids
    venue_ids = scoped_venue_ids()
    if venue_ids is None:
        venue_ids = [v.id for v in (db.list_venues() or [])]
    emails = set()
    for vid in venue_ids:
        if vid == venue_id:
            continue
        for other in db.get_employees(vid) or []:
            key = (getattr(other, "email", None) or "").strip().lower()
            if key:
                emails.add(key)
    return emails


def _elsewhere(db, emp, day: date, start: Optional[str], end: Optional[str]) -> List[dict]:
    module = _person_identity()
    busy = getattr(module, "busy_elsewhere", None) if module else None
    if busy is None:
        return []
    try:
        return list(busy(db, emp, day, start, end) or [])
    except Exception as e:  # noqa: BLE001
        logger.warning("busy_elsewhere failed for %s: %s", getattr(emp, "id", "?"), e)
        return []


def _other_records(db, emp) -> list:
    module = _person_identity()
    same = getattr(module, "same_person_records", None) if module else None
    if same is None:
        return []
    try:
        return list(same(db, emp) or [])
    except Exception as e:  # noqa: BLE001
        logger.warning("same_person_records failed for %s: %s", getattr(emp, "id", "?"), e)
        return []


def _fits(emp, area: str, venue) -> Optional[bool]:
    """None = can't fill; True = holds the area skill; False = may fill it
    without holding the skill (no areas recorded for them)."""
    area = normalise_area(area)
    skills = [normalise_area(s) for s in (getattr(emp, "skills", None) or []) if s]
    if find_area(venue, area) is not None:
        if area in skills:
            return True
        return False if can_fill(emp, area, venue) else None
    # Not one of the venue's areas (a free-text role from the AI): the skill
    # has to be recorded.
    return True if any(area in s for s in skills) else None


def area_choices(venue, allowed: Optional[Iterable[str]] = None) -> list:
    allowed_set = {normalise_area(a) for a in allowed} if allowed is not None else None
    return [{"name": a["name"], "label": a["label"]} for a in venue_areas(venue)
            if allowed_set is None or a["name"] in allowed_set]


def viewer_sections(venue_id: str) -> Optional[list]:
    """The areas a department manager is confined to at this venue, or None
    for a full manager / owner / a call outside a request."""
    try:
        from rosteriq.middleware.tenant import get_tenant_context_optional
        tenant = get_tenant_context_optional()
    except Exception:  # pragma: no cover
        tenant = None
    if tenant is None:
        return None
    sections = tenant.managed_sections(venue_id)
    return None if sections is None else [normalise_area(s) for s in sections]


def find_cover(db, venue_id: str, *, day: Optional[date] = None,
               start=None, end=None, areas: Optional[Iterable[str]] = None,
               include_unavailable: bool = False, include_salaried: bool = False,
               now: Optional[datetime] = None) -> dict:
    """Who can be called in for [start, end) on day. With no start and no
    end (the AI asking about a whole day) every shift that day counts as
    already on and availability is "available at some point that day"."""
    from rosteriq.services.clock import venue_now
    venue = db.get_venue(venue_id)
    if venue is None:
        raise LookupError("Venue not found")
    now = (now or venue_now(venue_id, db)).replace(tzinfo=None)
    notes: List[str] = []

    start_t, end_t = _hhmm(start), _hhmm(end)
    whole_day = False
    if day is None and start_t is None and end_t is None:
        win_start, win_end, note = default_window(venue, now)
        if note:
            notes.append(note)
        day = win_start.date()
    else:
        day = day or now.date()
        if start_t is None and end_t is None:
            whole_day = True
            win_start = datetime.combine(day, time(0, 0))
            win_end = win_start + timedelta(days=1)
        else:
            if start_t is None:
                raise ValueError("Give a start time with the end time.")
            win_start = datetime.combine(day, start_t)
            if end_t is None:
                window = trading_window(venue, day)
                if window and window[0] <= win_start < window[1]:
                    win_end = window[1]
                else:
                    win_end = win_start + timedelta(hours=DEFAULT_CALL_IN_HOURS)
            else:
                win_end = datetime.combine(day, end_t)
                if win_end <= win_start:
                    win_end += timedelta(days=1)
    start_s, end_s = win_start.strftime("%H:%M"), win_end.strftime("%H:%M")
    window_hours = round((win_end - win_start).total_seconds() / 3600, 2)

    wanted = [normalise_area(a) for a in (areas or []) if normalise_area(a)]

    monday = week_monday(day)
    sunday = monday + timedelta(days=6)
    fortnight_start = monday - timedelta(days=7)
    shifts = [s for s in live_shifts(db, venue_id, fortnight_start, max(day + timedelta(days=1), sunday))
              if _working(s)]
    by_emp: dict = {}
    for s in shifts:
        by_emp.setdefault(str(s.employee_id), []).append(s)

    leave = approved_leave(db, venue_id)
    shared = _shared_emails(db, venue_id)

    available, unavailable, already_on, on_leave = [], [], [], []
    salaried_left_out = unavailable_hidden = 0
    for emp in db.get_employees(venue_id) or []:
        if not is_active(emp):
            continue
        eid = str(emp.id)
        fit = True
        if wanted:
            fits = [_fits(emp, a, venue) for a in wanted]
            if all(f is None for f in fits):
                continue
            fit = any(f is True for f in fits)
        if is_salaried(emp) and not include_salaried:
            salaried_left_out += 1
            continue
        spans = [(rs, re_) for rs, re_ in leave.get(eid, []) if rs <= day <= re_]
        if spans:
            on_leave.append({"id": eid, "name": emp.name,
                             "from": min(s[0] for s in spans).isoformat(),
                             "to": max(s[1] for s in spans).isoformat()})
            continue

        mine = by_emp.get(eid, [])
        # Visa caps count rostered (gross) hours, as visa.fortnight_cap_flags does.
        week_hours = sum(s.net_hours for s in mine if monday <= s.date <= sunday)
        fortnight_hours = sum(s.duration_hours for s in mine if fortnight_start <= s.date <= sunday)
        email = (getattr(emp, "email", None) or "").strip().lower()
        is_shared = bool(email) and email in shared
        if is_shared:
            for other, other_vid in _other_records(db, emp):
                if other_vid == venue_id:
                    continue
                try:
                    theirs = [s for s in live_shifts(db, other_vid, fortnight_start, sunday)
                              if str(s.employee_id) == str(other.id) and _working(s)]
                except Exception:  # noqa: BLE001
                    theirs = []
                week_hours += sum(s.net_hours for s in theirs if s.date >= monday)
                fortnight_hours += sum(s.duration_hours for s in theirs)

        skills = [str(s) for s in (getattr(emp, "skills", None) or []) if s]
        entry = {
            "id": eid,
            "name": emp.name,
            "role": skills[0] if skills else None,
            "skills": skills,
            "employment_type": employment_type(emp),
            "phone": getattr(emp, "phone", None),
            "tel": dial_number(getattr(emp, "phone", None)),
            "email": getattr(emp, "email", None),
            "skill_match": fit,
            "hours_this_week": round(week_hours, 2),
            "visa": cap_headroom(emp, fortnight_hours, window_hours),
            "flags": [],
        }
        if wanted and not fit:
            entry["flags"].append("No areas recorded for them")
        if entry["visa"] and entry["visa"]["would_exceed"]:
            entry["flags"].append(entry["visa"]["message"])

        overlapping, same_day = [], []
        for s in mine:
            a, b = shift_span(s)
            if a < win_end and win_start < b:
                overlapping.append(s)
            elif s.date == day:
                same_day.append(s)
        if overlapping:
            s = min(overlapping, key=lambda x: shift_span(x)[0])
            entry["status"] = "already_on"
            entry["shift_here"] = f"{s.start_time:%H:%M}-{s.end_time:%H:%M}"
            entry["flags"].insert(0, f"Already on {entry['shift_here']}")
            already_on.append(entry)
            continue
        if same_day:
            s = same_day[0]
            entry["shift_today"] = f"{s.start_time:%H:%M}-{s.end_time:%H:%M}"
            entry["flags"].append(f"Rostered {entry['shift_today']} that day")

        elsewhere = _elsewhere(db, emp, day, None if whole_day else start_s,
                               None if whole_day else end_s) if is_shared else []
        if elsewhere:
            entry["status"] = "on_elsewhere"
            entry["elsewhere"] = [{"venue_id": h.get("venue_id"), "venue_name": h.get("venue_name"),
                                   "start": h.get("start"), "end": h.get("end")} for h in elsewhere]
            entry["flags"].insert(0, "; ".join(
                f"On at {h.get('venue_name') or 'another venue'} {h.get('start')}-{h.get('end')}"
                for h in elsewhere))
            already_on.append(entry)
            continue

        avail = getattr(emp, "availability", None)
        if not isinstance(avail, dict):
            avail = None
        if whole_day:
            free = is_available(avail, day)
        else:
            free = is_available(avail, day, start_s, end_s)
        if free:
            entry["status"] = "available"
            available.append(entry)
        else:
            entry["status"] = "unavailable"
            entry["flags"].insert(0, "Marked unavailable then")
            if include_unavailable:
                unavailable.append(entry)
            else:
                unavailable_hidden += 1

    def rank(e):
        over = bool(e["visa"] and e["visa"]["would_exceed"])
        return (TYPE_ORDER.get(e["employment_type"], 3), not e["skill_match"], over,
                e["hours_this_week"], e["name"].lower())

    for bucket in (available, unavailable, already_on):
        bucket.sort(key=rank)

    if salaried_left_out:
        notes.append(f"{salaried_left_out} salaried {'person is' if salaried_left_out == 1 else 'people are'} "
                     "left out — ask to include them.")

    allowed = viewer_sections(venue_id)
    venue_name = getattr(venue, "name", None) or venue_id
    return {
        "venue_id": venue_id,
        "venue_name": venue_name,
        "date": day.isoformat(),
        "window": {"start": start_s, "end": end_s, "whole_day": whole_day,
                   "hours": window_hours, "label": "all day" if whole_day else friendly_range(win_start, win_end)},
        "requested_shift": None if whole_day else f"{start_s} - {end_s}",
        "areas": wanted,
        "message": None if whole_day else call_in_message(venue_name, win_start, win_end, now.date()),
        "available": available,
        "available_count": len(available),
        "unavailable": unavailable,
        "unavailable_count": len(unavailable),
        "include_unavailable": bool(include_unavailable),
        "unavailable_hidden": unavailable_hidden,
        "already_on": already_on,
        "on_leave": on_leave,
        "salaried_left_out": salaried_left_out,
        "area_choices": area_choices(venue, allowed),
        "notes": notes,
    }


def record_shown(venue_id: str, inputs: dict, result: dict, source: str,
                 user_id: Optional[str] = None) -> None:
    """Keep who the manager was offered to call, so a call-out can be checked
    later. Never raises."""
    from rosteriq.services.events import record_event
    record_event("audit", "ops.sos_search", venue_id=venue_id, resource_type="sos",
                 details={"source": source, "inputs": inputs, "shown": audit_summary(result)},
                 user_id=user_id)


def audit_summary(result: dict) -> dict:
    """What the manager was shown, compact enough for the event log."""
    return {
        "date": result.get("date"),
        "window": f"{result['window']['start']}-{result['window']['end']}",
        "areas": result.get("areas") or ["all"],
        "available": [e["name"] for e in result.get("available", [])][:25],
        "unavailable": [e["name"] for e in result.get("unavailable", [])][:15],
        "already_on": len(result.get("already_on", [])),
        "on_leave": len(result.get("on_leave", [])),
    }
