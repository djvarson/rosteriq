"""
Hand edits to a roster: add, move, reassign, retime, re-area or delete one
shift, and copy a week forward as a new draft.

Checks a manager should see but may override come back as warnings
(availability, approved leave, recorded visa cap or expiry, max hours, busy at
another venue, award compliance, area skill). Two things are refused outright:
a shift that overlaps the same person's own shift, and rostering someone who
is inactive.

A roster staff can already see (published, or imported from the old system)
stays live while it is edited. Each change is kept against the roster; the
ones no one has been told about are its "changed since publish" list until a
manager notifies the affected staff, or publishes the roster again.
"""

import json
import logging
import threading
import time as _time
import uuid
from contextlib import contextmanager
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from typing import Optional

from rosteriq.award_rules import (
    MAX_CONSECUTIVE_DAYS, MINIMUM_HOURS_BETWEEN_SHIFTS, validate_shift_compliance,
)
from rosteriq.cost_calculator import calculate_roster_cost, calculate_shift_cost_breakdown
from rosteriq.models import Roster, Shift, ShiftStatus, State, normalise_area
from rosteriq.services.availability_rules import is_available
from rosteriq.services.current_roster import (
    ARCHIVED, PUBLISHED, SUPERSEDED, current_roster, is_imported, roster_state, week_monday,
)
from rosteriq.services.leave_shifts import OPEN, is_open
from rosteriq.services.venue_areas import area_names, can_fill

logger = logging.getLogger(__name__)

CHANGES_KEY = "roster_changes:"
MAX_CHANGES_KEPT = 500
LOCKED_STATES = frozenset({ARCHIVED, SUPERSEDED})
LIVE_COVER = ("open", "claimed")

_lock = threading.Lock()
_PG_LOCK_CLASS = 7_734_101


class RosterBusy(Exception):
    """Another save of the same roster is in progress."""


class ShiftClash(Exception):
    """The shift overlaps the same person's own shift."""


# ---------------------------------------------------------------------------
# locking: save_roster replaces the roster's whole shift list, so two edits
# saved from stale copies would drop each other's shifts.
# ---------------------------------------------------------------------------

def _is_pg(db) -> bool:
    return hasattr(db, "_cursor") and db.__class__.__name__ == "PostgresStore"


def _pg_lock(db, roster_id: str, unlock: bool = False) -> bool:
    fn = "pg_advisory_unlock" if unlock else "pg_try_advisory_lock"
    with db._cursor() as cur:
        cur.execute(f"SELECT {fn}(%s, hashtext(%s)) AS got", (_PG_LOCK_CLASS, roster_id))
        row = cur.fetchone()
        return bool(row["got"] if isinstance(row, dict) else row[0])


@contextmanager
def roster_lock(db, roster_id: str, wait_seconds: float = 5.0):
    if not _lock.acquire(timeout=wait_seconds):
        raise RosterBusy()
    got_pg = False
    try:
        if _is_pg(db):
            deadline = _time.monotonic() + wait_seconds
            got_pg = _pg_lock(db, roster_id)
            while not got_pg and _time.monotonic() < deadline:
                _time.sleep(0.1)
                got_pg = _pg_lock(db, roster_id)
            if not got_pg:
                raise RosterBusy()
        yield
    finally:
        if got_pg:
            try:
                _pg_lock(db, roster_id, unlock=True)
            except Exception:
                pass
        _lock.release()


# ---------------------------------------------------------------------------
# small readers
# ---------------------------------------------------------------------------

def staff_visible(db, roster) -> bool:
    return roster_state(db, roster.id) == PUBLISHED or is_imported(roster)


def hhmm(t) -> str:
    return t.strftime("%H:%M") if isinstance(t, (time, datetime)) else str(t or "")[:5]


def snapshot(shift, employee=None) -> dict:
    return {
        "shift_id": shift.id,
        "employee_id": shift.employee_id,
        "employee_name": getattr(employee, "name", None),
        "date": shift.date.isoformat(),
        "start": hhmm(shift.start_time),
        "end": hhmm(shift.end_time),
        "break_minutes": shift.break_minutes,
        "role": shift.role,
    }


def is_active(employee) -> bool:
    return bool(getattr(employee, "active", True))


def leave_spans(first: date, last: date, working_days=()) -> list:
    """[(first_day, last_day)] of first..last less the days of it the person
    also works part of (a leave record's working_days)."""
    worked = set()
    for d in working_days or ():
        try:
            worked.add(d if isinstance(d, date) else date.fromisoformat(str(d)[:10]))
        except ValueError:
            continue
    spans, start = [], first
    for d in sorted(x for x in worked if first <= x <= last):
        if d > start:
            spans.append((start, d - timedelta(days=1)))
        start = d + timedelta(days=1)
    if start <= last:
        spans.append((start, last))
    return spans


def approved_leave(db, venue_id: str, with_working_days: bool = False) -> dict:
    """employee_id -> [(first_day, last_day)] of APPROVED leave, less the days
    the person also works part of, which a manager may roster them on.
    with_working_days keeps those days, for code that rosters people by itself
    (copying a week), so it never puts anyone on them."""
    out: dict = {}
    try:
        rows = db.list_leave_requests(venue_id) or []
    except Exception as e:
        logger.warning(f"Leave lookup failed for {venue_id}: {e}")
        return out
    for r in rows:
        if r.get("status") != "approved":
            continue
        try:
            first, last = r.get("start_date"), r.get("end_date")
            first = first if isinstance(first, date) else date.fromisoformat(str(first)[:10])
            last = last if isinstance(last, date) else date.fromisoformat(str(last)[:10])
        except (TypeError, ValueError):
            continue
        spans = [(first, last)] if with_working_days else leave_spans(first, last,
                                                                      r.get("working_days"))
        if spans:
            out.setdefault(str(r.get("employee_id")), []).extend(spans)
    return out


def on_leave(leave: dict, employee_id: str, day: date) -> bool:
    return any(first <= day <= last for first, last in leave.get(str(employee_id), []))


def _interval(shift) -> tuple:
    start = datetime.combine(shift.date, shift.start_time)
    return start, start + timedelta(hours=shift.duration_hours)


def _busy_elsewhere(db, employee, day, start, end) -> list:
    """The same person's shifts at the venue's sister venues, when the
    person-identity service is installed; none otherwise."""
    try:
        from rosteriq.services import person_identity
    except ImportError:
        return []
    fn = getattr(person_identity, "busy_elsewhere", None)
    if fn is None:
        return []
    try:
        return list(fn(db, employee, day, start, end) or [])
    except Exception as e:
        logger.warning(f"busy_elsewhere failed for {getattr(employee, 'id', '?')}: {e}")
        return []


def _booked_elsewhere(db, venue_id: str, employees: list, start: date, end: date) -> list:
    """The same people's shifts at the venue's sister venues dated start..end,
    each carrying the LOCAL employee id."""
    try:
        from rosteriq.services.person_identity import bookings_elsewhere
        return list(bookings_elsewhere(db, venue_id, employees, start, end) or [])
    except Exception as e:
        logger.warning(f"Other-venue bookings lookup failed for {venue_id}: {e}")
        return []


def visa_cap_flags(db, roster, employees, prev, shifts: Optional[list] = None) -> list:
    """Recorded fortnight visa caps over the roster's week (``shifts``, else
    the roster's own) and the week before (``prev``). A recorded cap covers
    every job, so the same people's shifts at the owner's other venues count."""
    from types import SimpleNamespace
    from rosteriq.services.visa import fortnight_cap_flags
    employees = list(employees)
    monday = week_monday(roster.week_start)
    this_week = list(roster.shifts if shifts is None else shifts)
    this_week += _booked_elsewhere(db, roster.venue_id, employees, monday, monday + timedelta(days=6))
    last_week = list(getattr(prev, "shifts", None) or [])
    last_week += _booked_elsewhere(db, roster.venue_id, employees,
                                   monday - timedelta(days=7), monday - timedelta(days=1))
    return fortnight_cap_flags(SimpleNamespace(shifts=this_week), employees,
                               SimpleNamespace(shifts=last_week))


def cross_venue_clash_rows(db, roster, employees) -> list:
    """One row per shift on the roster and the same person's shift at another
    venue that it overlaps, or leaves less than the minimum rest around, with
    no repeats: {employee_id, name, shift_id, date, start, end, role} of the
    shift here and the clash's message ('Meca Rossi is on at Apothecary
    17:00-00:00 on Wed 21 Oct')."""
    try:
        from rosteriq.services.person_identity import cross_venue_clashes
        clashes = cross_venue_clashes(db, roster, employees, min_rest_hours=MINIMUM_HOURS_BETWEEN_SHIFTS)
    except Exception as e:
        logger.warning(f"Cross-venue check failed for roster {getattr(roster, 'id', '?')}: {e}")
        return []
    names = {str(e.id): getattr(e, "name", None) for e in employees}
    here = {s.id: s for s in roster.shifts}
    out, seen = [], set()
    for c in clashes:
        s = here.get(c.get("shift_id"))
        if s is None or (s.id, c["message"]) in seen:
            continue
        seen.add((s.id, c["message"]))
        out.append({"employee_id": s.employee_id, "name": names.get(str(s.employee_id)),
                    "shift_id": s.id, "date": s.date.isoformat(), "start": hhmm(s.start_time),
                    "end": hhmm(s.end_time), "role": s.role, "message": c["message"]})
    return out


def neighbour_shifts(db, roster, employee_id: str, exclude_id: Optional[str] = None) -> list:
    """The person's other shifts in this roster and in the live rosters of the
    weeks either side (an overnight close runs into the next week)."""
    out = [s for s in roster.shifts if s.employee_id == employee_id and s.id != exclude_id]
    monday = week_monday(roster.week_start)
    for other_monday in (monday - timedelta(days=7), monday + timedelta(days=7)):
        try:
            other = current_roster(db, roster.venue_id, other_monday)
        except Exception:
            other = None
        if other is None or other.id == roster.id:
            continue
        out += [s for s in other.shifts if s.employee_id == employee_id]
    return out


# ---------------------------------------------------------------------------
# checks
# ---------------------------------------------------------------------------

def area_warnings(venue, role) -> list:
    """A warning when the venue has named its areas and `role` isn't one."""
    if venue is None or not role:
        return []
    known = area_names(venue)
    if known and normalise_area(role) not in known:
        return [{"code": "area", "message": f"'{role}' isn't one of this venue's areas."}]
    return []


def check_shift(db, roster, venue, employee, shift, exclude_id: Optional[str] = None) -> list:
    """Warnings for putting `shift` on `employee`. Raises ShiftClash when it
    overlaps one of their own shifts."""
    others = neighbour_shifts(db, roster, employee.id, exclude_id)
    start, end = _interval(shift)
    for other in others:
        o_start, o_end = _interval(other)
        if start < o_end and o_start < end:
            raise ShiftClash(
                f"{employee.name} is already rostered {other.date.isoformat()} "
                f"{hhmm(other.start_time)}-{hhmm(other.end_time)} ({other.role or 'no area'}).")

    warnings = []

    def warn(code, message):
        warnings.append({"code": code, "message": message})

    if not is_available(getattr(employee, "availability", None), shift.date,
                        shift.start_time, shift.end_time):
        warn("availability", f"{employee.name} has marked themselves unavailable "
                             f"{shift.date.strftime('%A')} at these times.")
    if on_leave(approved_leave(db, roster.venue_id), employee.id, shift.date):
        warn("leave", f"{employee.name} is on approved leave on {shift.date.isoformat()}.")

    if venue is not None and shift.role:
        unknown = area_warnings(venue, shift.role)
        if unknown:
            warnings.extend(unknown)
        elif not can_fill(employee, shift.role, venue):
            warn("skill", f"{employee.name} doesn't have {shift.role} in their skills.")

    week_shifts = [s for s in roster.shifts if s.employee_id == employee.id and s.id != exclude_id]
    week_hours = sum(s.net_hours for s in week_shifts) + shift.net_hours
    cap = float(getattr(employee, "max_hours_per_week", 0) or 0)
    if cap and week_hours > cap:
        warn("max_hours", f"{employee.name} would be rostered {week_hours:g}h this week, "
                          f"over their {cap:g}h maximum.")

    from rosteriq.services.visa import visa_alerts
    try:
        prev = current_roster(db, roster.venue_id, week_monday(roster.week_start) - timedelta(days=7))
    except Exception:
        prev = None
    if prev is not None and prev.id == roster.id:
        prev = None
    for flag in visa_cap_flags(db, roster, [employee], prev, shifts=week_shifts + [shift]):
        warn("visa_cap", flag["message"])
    for alert in visa_alerts([employee], today=shift.date):
        if alert["kind"] == "expired":
            warn("visa_expiry", f"{employee.name}'s recorded visa expires {alert['expiry']}, "
                                f"before this shift — confirm their work rights.")

    for busy in _busy_elsewhere(db, employee, shift.date, shift.start_time, shift.end_time):
        warn("busy_elsewhere", f"{employee.name} is also rostered at "
                               f"{busy.get('venue_name') or 'another venue'} "
                               f"{hhmm(busy.get('start'))}-{hhmm(busy.get('end'))} that day.")

    try:
        found = validate_shift_compliance(employee, shift, others)
    except Exception as e:
        logger.warning(f"Compliance check failed for shift {shift.id}: {e}")
        found = []
    following = [o for o in others if _interval(o)[0] >= end]
    if following:
        nxt = min(following, key=lambda o: _interval(o)[0])
        rest = (_interval(nxt)[0] - end).total_seconds() / 3600
        if rest < MINIMUM_HOURS_BETWEEN_SHIFTS:
            found.append(f"Only {rest:.1f}h rest before the next shift "
                         f"(minimum {MINIMUM_HOURS_BETWEEN_SHIFTS}h required)")
    if not any("consecutive" in m for m in found):
        days = {o.date for o in others} | {shift.date}
        run, d = 1, shift.date - timedelta(days=1)
        while d in days:
            run, d = run + 1, d - timedelta(days=1)
        d = shift.date + timedelta(days=1)
        while d in days:
            run, d = run + 1, d + timedelta(days=1)
        limit = min(MAX_CONSECUTIVE_DAYS, int(getattr(employee, "consecutive_days_limit", 0)
                                             or MAX_CONSECUTIVE_DAYS))
        if run > limit:
            found.append(f"Shift creates {run} consecutive days (maximum {limit} allowed)")
    for message in found:
        warn("compliance", message)
    return warnings


# ---------------------------------------------------------------------------
# cost
# ---------------------------------------------------------------------------

def venue_state(venue):
    return getattr(venue, "state", None) or State.wa


def cost_shift(shift, employee, state) -> None:
    try:
        shift.cost = calculate_shift_cost_breakdown(employee, shift, state).total_cost
    except Exception as e:
        logger.warning(f"Could not cost shift {shift.id}: {e}")


def recost_total(roster, emp_map: dict, state) -> None:
    costable = [s for s in roster.shifts if s.employee_id in emp_map]
    try:
        roster.total_cost = calculate_roster_cost(
            roster.model_copy(update={"shifts": costable}), emp_map, state)
    except Exception as e:
        logger.warning(f"Could not recost roster {roster.id}: {e}")
        roster.total_cost = sum((s.cost or Decimal("0") for s in costable), Decimal("0"))


# ---------------------------------------------------------------------------
# change record
# ---------------------------------------------------------------------------

def _load_changes(db, roster) -> dict:
    try:
        raw = db.load_from_key(CHANGES_KEY + roster.id)
    except Exception:
        raw = None
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except ValueError:
            raw = None
    if not isinstance(raw, dict) or raw.get("venue_id") != roster.venue_id:
        raw = {"venue_id": roster.venue_id, "roster_id": roster.id, "changes": []}
    raw.setdefault("changes", [])
    return raw


def _history(db, roster_id: str) -> list:
    try:
        return list(db.get_roster_state_history(roster_id) or [])
    except Exception:
        return []


def record_change(db, roster, action: str, before: Optional[dict], after: Optional[dict],
                  actor_id: Optional[str]) -> None:
    """Keep a change made to a roster staff can see."""
    record = _load_changes(db, roster)
    record["changes"].append({
        "id": uuid.uuid4().hex[:10],
        "at": datetime.utcnow().isoformat(timespec="seconds"),
        "by": actor_id,
        "action": action,
        "before": before,
        "after": after,
        "epoch": len(_history(db, roster.id)),
        "notified": False,
    })
    record["changes"] = record["changes"][-MAX_CHANGES_KEPT:]
    db.save_to_key(CHANGES_KEY + roster.id, record)


def pending_changes(db, roster) -> list:
    """Changes staff haven't been told about: made while the roster was
    visible to them, not yet notified, and not followed by a fresh publish
    (which notifies everyone)."""
    if not staff_visible(db, roster):
        return []
    history = _history(db, roster.id)
    published_upto = max((i + 1 for i, h in enumerate(history) if h.get("state") == PUBLISHED),
                         default=0)
    return [c for c in _load_changes(db, roster)["changes"]
            if not c.get("notified") and int(c.get("epoch", 0)) >= published_upto]


def mark_notified(db, roster, change_ids: set, actor_id: Optional[str]) -> None:
    record = _load_changes(db, roster)
    now = datetime.utcnow().isoformat(timespec="seconds")
    for c in record["changes"]:
        if c.get("id") in change_ids:
            c["notified"] = True
            c["notified_at"] = now
            c["notified_by"] = actor_id
    db.save_to_key(CHANGES_KEY + roster.id, record)


def change_roles(change: dict) -> list:
    return [s["role"] for s in (change.get("before"), change.get("after")) if s and s.get("role") is not None]


def affected_staff(changes: list) -> tuple:
    """(changed, cancelled): employee ids whose shifts were added or altered,
    and those who only lost a shift."""
    changed, lost = [], []
    for c in changes:
        before, after = c.get("before"), c.get("after")
        if after and after.get("employee_id") and after["employee_id"] not in changed:
            changed.append(after["employee_id"])
        if before and before.get("employee_id") and (
                not after or after.get("employee_id") != before["employee_id"]):
            if before["employee_id"] not in lost:
                lost.append(before["employee_id"])
    return changed, [e for e in lost if e not in changed]


# ---------------------------------------------------------------------------
# covers on a shift the manager changes by hand
# ---------------------------------------------------------------------------

def _cover_terms(shift) -> tuple:
    """What a cover request promises: who holds the shift, when, and the
    area that decides who may claim it."""
    return (shift.employee_id or OPEN, shift.date, shift.start_time, shift.end_time,
            normalise_area(shift.role))


def settle_covers(db, venue_id: str, before, after, actor_id: Optional[str]) -> list:
    """Withdraw the open or claimed cover requests on a shift the manager has
    filled, reassigned, moved, retimed, re-areaed or (``after`` None)
    deleted, so a later claim can't take it off whoever the manager put on
    it. A shift that is still open is posted for cover again with its new
    details. Returns warnings for the manager."""
    if after is not None and _cover_terms(before) == _cover_terms(after):
        return []
    try:
        covers = [c for c in (db.list_shift_covers(venue_id) or [])
                  if c.get("shift_id") == before.id and c.get("status") in LIVE_COVER]
    except Exception as e:
        logger.warning(f"Cover lookup failed for shift {before.id} at {venue_id}: {e}")
        return []
    if not covers:
        return []
    from rosteriq.services.events import audit
    if after is None:
        note = "The shift was taken off the roster."
    elif not is_open(after):
        note = "The shift was filled on the roster." if is_open(before) else "The shift was changed on the roster."
    else:
        note = "The shift was changed on the roster and posted for cover again."
    claimants, withdrawn = [], []
    for c in covers:
        # Conditional, so a claim made since the read is kept and named, and a
        # cover approved or cancelled since stays as it is.
        row = db.withdraw_shift_cover(c["id"], actor_id, note)
        if row is None:
            continue
        withdrawn.append(row)
        claimed_by = row.get("claimed_by")
        # a claim always sets claimed_by and a decline clears it
        audit("cover.withdrawn", venue_id, "shift_cover", row.get("id"), shift_id=before.id,
              previous_status="claimed" if claimed_by else "open", claimed_by=claimed_by,
              origin=row.get("origin") or "staff", deleted=after is None, note=note)
        claimant = db.get_employee(claimed_by) if claimed_by else None
        if claimant is not None and getattr(claimant, "venue_id", None) == venue_id:
            claimants.append(claimant)
    if not withdrawn:
        return []
    message = "The cover request for this shift was withdrawn"
    if after is not None and is_open(after):
        first = next((c for c in withdrawn if c.get("origin") == "leave"), withdrawn[0])
        db.save_shift_cover({
            "id": f"cv-{uuid.uuid4().hex[:10]}",
            "venue_id": venue_id,
            "shift_id": after.id,
            "shift_date": after.date,
            "shift_start": hhmm(after.start_time),
            "shift_end": hhmm(after.end_time),
            "role": after.role,
            "requested_by": first.get("requested_by"),
            "reason": "",
            "origin": "leave",
            "claimed_by": None,
            "status": "open",
            "created_at": datetime.utcnow(),
        })
        message += " and the open shift posted for cover again with its new details"
    message += "."
    for e in claimants:
        if after is not None and e.id == after.employee_id:
            message += f" {e.name} had claimed it and is now on it."
        else:
            message += f" {e.name} had claimed it — let them know."
    return [{"code": "cover_withdrawn", "message": message}]


# ---------------------------------------------------------------------------
# copy a week forward
# ---------------------------------------------------------------------------

def copy_roster(db, source, target_monday: date, venue) -> tuple:
    """(new draft roster, skipped rows, left-open rows). A shift that was
    open, or whose person is no longer on this venue's staff, inactive, on
    approved leave or unavailable on the new date, is copied as an open
    shift for the manager to fill. A shift that would land outside the
    target week is skipped."""
    shift_days = (target_monday - week_monday(source.week_start)).days
    target_end = target_monday + timedelta(days=6)
    leave = approved_leave(db, source.venue_id, with_working_days=True)
    state = venue_state(venue)
    employees: dict = {}
    shifts, skipped, left_open = [], [], []
    for s in sorted(source.shifts, key=lambda x: (x.date, x.start_time)):
        new_date = s.date + timedelta(days=shift_days)
        emp = None
        if not is_open(s):
            if s.employee_id not in employees:
                emp = db.get_employee(s.employee_id)
                employees[s.employee_id] = emp if emp is not None and emp.venue_id == source.venue_id else None
            emp = employees[s.employee_id]
        row = {"employee_id": s.employee_id,
               "name": "Open shift" if is_open(s) else getattr(emp, "name", None) or "A former staff member",
               "date": new_date.isoformat(), "start": hhmm(s.start_time),
               "end": hhmm(s.end_time), "role": s.role}
        if not target_monday <= new_date <= target_end:
            skipped.append(dict(row, reason="outside the week"))
            continue
        reason = None
        if is_open(s):
            reason = "open last week"
        elif emp is None:
            reason = "no longer on this venue's staff"
        elif not is_active(emp):
            reason = "inactive"
        elif on_leave(leave, emp.id, new_date):
            reason = "on approved leave"
        elif not is_available(getattr(emp, "availability", None), new_date, s.start_time, s.end_time):
            reason = "unavailable"
        new = Shift(id=f"shift-{uuid.uuid4().hex[:10]}", employee_id=OPEN if reason else s.employee_id,
                    date=new_date, start_time=s.start_time, end_time=s.end_time,
                    break_minutes=s.break_minutes, status=ShiftStatus.scheduled, role=s.role)
        if reason:
            left_open.append(dict(row, reason=reason))
        else:
            cost_shift(new, emp, state)
        shifts.append(new)
    roster = Roster(id=f"copy-{uuid.uuid4().hex[:10]}", venue_id=source.venue_id,
                    week_start=target_monday, week_end=target_end, shifts=shifts,
                    total_cost=None, created_at=datetime.utcnow())
    recost_total(roster, {k: v for k, v in employees.items() if v is not None}, state)
    return roster, skipped, left_open
