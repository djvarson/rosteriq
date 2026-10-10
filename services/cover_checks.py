"""
The checks a cover approval runs before a shift changes hands, each named so
the manager's confirm dialog can say exactly what was found:

  * trained   — can this person work the shift's area (venue_areas.can_fill)?
                A supervisory area (requires_skill: duty manager, supervisor)
                is a hard block for anyone without the skill; any other area
                is a warning the manager may accept.
  * hours     — their week's rostered hours with this shift vs their
                max_hours_per_week (warning).
  * visa      — their rostered hours over this week and last vs the
                fortnight cap the venue recorded from VEVO (warning; nothing
                is checked when no cap was recorded).
  * rest      — the gap to their shifts either side vs the award table's
                minimum rest (warning; advisor to confirm).
  * clash     — another shift of theirs here overlapping this one (block).
  * elsewhere — rostered at another venue at the same time (block), through
                services/person_identity when it is present; a warning when
                one of their venues couldn't be checked.

Hours at the person's other venues count towards hours, visa and rest when
services/person_identity can find their other records; without it the person
is treated as working only here.
"""

from datetime import date, datetime, time, timedelta
from typing import Optional

from rosteriq.award_rules import MINIMUM_HOURS_BETWEEN_SHIFTS
from rosteriq.roster_optimiser import GENERIC_ROLES
from rosteriq.services.current_roster import live_shifts, week_monday
from rosteriq.services.leave_shifts import span
from rosteriq.services.venue_areas import can_fill, find_area, _SUPERVISORY

OK, WARN, BLOCK = "ok", "warn", "block"
_DEAD = ("cancelled", "no_show")


def _person_identity():
    try:
        from rosteriq.services import person_identity
        return person_identity
    except Exception:
        return None


def _hhmm(value) -> time:
    if isinstance(value, time):
        return value
    hh, mm = str(value).strip()[:5].split(":")
    return time(int(hh), int(mm))


def _span_dt(day: date, start, end) -> tuple:
    a = datetime.combine(day, _hhmm(start))
    b = datetime.combine(day, _hhmm(end))
    if b <= a:
        b += timedelta(days=1)
    return a, b


def _live(shift) -> bool:
    return str(getattr(shift.status, "value", shift.status)) not in _DEAD


def _norm(name) -> str:
    return " ".join(str(name or "").strip().lower().split())


def requires_skill(venue, role) -> bool:
    area = find_area(venue, role) if venue is not None else None
    if area:
        return bool(area["requires_skill"])
    return any(s in _norm(role) for s in _SUPERVISORY)


def not_trained_message(role) -> str:
    return f"You're not trained for {role}"


def trained_for(employee, role, venue) -> bool:
    """Can this person work `role`? A generic role ('general') is anyone's."""
    if _norm(role) in GENERIC_ROLES or not _norm(role):
        return True
    return can_fill(employee, role, venue)


def can_take(employee, role, venue) -> bool:
    """May this person be offered / claim a cover for `role`? Inactive staff
    are offered nothing."""
    return getattr(employee, "active", True) and trained_for(employee, role, venue)


def _their_shifts(db, venue_id: str, employee, start: date, end: date, skip_id=None) -> tuple:
    """([(shift, venue_name or None)], checked) of this person's live shifts
    dated start..end here and, when person_identity is present, at their
    other venues: by name where the caller may see the venue, as "another
    venue" where it only shares an owner with this one. ``checked`` is
    False when any of those venues couldn't be read."""
    out = []
    for s in live_shifts(db, venue_id, start, end):
        if str(s.employee_id) == str(employee.id) and s.id != skip_id and _live(s):
            out.append((s, None))
    identity = _person_identity()
    finder = getattr(identity, "same_person_records", None) if identity else None
    if finder is None:
        return out, False
    try:
        others = finder(db, employee) or []
    except Exception:
        return out, False
    by_venue: dict = {}
    for other, vid in others:
        if vid and vid != venue_id:
            by_venue.setdefault(vid, set()).add(str(other.id))
    checked = True
    for vid, ids in by_venue.items():
        venue = db.get_venue(vid)
        name = getattr(venue, "name", None) or vid
        try:
            theirs = live_shifts(db, vid, start, end)
        except Exception:
            checked = False
            continue
        for s in theirs:
            if str(s.employee_id) in ids and _live(s):
                out.append((s, name))
    unseen = getattr(identity, "unseen_shifts", None)
    if unseen is not None:
        try:
            out += [(s, "another venue") for s in unseen(db, employee, start, end) or []]
        except Exception:
            checked = False
    return out, checked


def cover_checks(db, venue_id: str, cover: dict, claimant, cover_shift=None) -> dict:
    """Every check for `claimant` taking `cover`'s shift. `blocked` is True
    when any check is a hard block."""
    venue = db.get_venue(venue_id)
    name = getattr(claimant, "name", None) or "This person"
    role = cover.get("role") or ""
    day = cover.get("shift_date")
    if not isinstance(day, date):
        day = date.fromisoformat(str(day)[:10])
    start = cover.get("shift_start") or "00:00"
    end = cover.get("shift_end") or "23:59"
    begin, finish = _span_dt(day, start, end)
    if cover_shift is not None:
        hours = float(cover_shift.net_hours)
    else:
        hours = (finish - begin).total_seconds() / 3600
    checks = []

    def add(check, result, message):
        checks.append({"check": check, "result": result, "message": message})

    if not getattr(claimant, "active", True):
        add("active", BLOCK, f"{name} is marked inactive")

    if trained_for(claimant, role, venue):
        add("trained", OK, f"{name} can work {role}")
    elif requires_skill(venue, role):
        add("trained", BLOCK, f"{name} isn't trained for {role} — a supervisory role "
                              "needs that skill on their record")
    else:
        add("trained", WARN, f"{name} isn't trained for {role} (no '{role}' skill on their record)")

    monday = week_monday(day)
    fortnight_start = monday - timedelta(days=7)
    theirs, checked = _their_shifts(db, venue_id, claimant, fortnight_start - timedelta(days=1),
                                    monday + timedelta(days=7), skip_id=cover.get("shift_id"))

    # Same venue double-booking and other-venue overlap
    for s, where in theirs:
        a, b = _span_dt(s.date, s.start_time, s.end_time)
        if a < finish and begin < b:
            when = f"{s.date.strftime('%a')} {span(s.start_time, s.end_time)}"
            if where is None:
                add("clash", BLOCK, f"{name} is already rostered {when} here, which overlaps this shift")
            else:
                add("elsewhere", BLOCK, f"{name} is rostered at {where} {when}, which overlaps this shift")
    if not any(c["check"] == "clash" for c in checks):
        add("clash", OK, f"{name} has no overlapping shift here")
    if _person_identity() is not None and not any(c["check"] == "elsewhere" for c in checks):
        if checked:
            add("elsewhere", OK, f"{name} isn't rostered at another venue then")
        else:
            add("elsewhere", WARN, f"Couldn't check {name}'s other venues for a shift then")

    week_hours = sum(float(s.net_hours) for s, _ in theirs if monday <= s.date <= monday + timedelta(days=6))
    total_week = round(week_hours + hours, 2)
    cap = float(getattr(claimant, "max_hours_per_week", 0) or 0)
    if cap and total_week > cap:
        add("hours", WARN, f"Takes {name} to {total_week:g}h this week — over their {cap:g}h maximum")
    else:
        add("hours", OK, f"{name} would be on {total_week:g}h this week"
                         + (f" (max {cap:g}h)" if cap else ""))

    visa_cap = getattr(claimant, "visa_work_limit_fortnight", None)
    if visa_cap:
        fortnight = sum(float(s.net_hours) for s, _ in theirs if fortnight_start <= s.date <= monday + timedelta(days=6))
        total_fn = round(fortnight + hours, 2)
        if total_fn > float(visa_cap):
            add("visa", WARN, f"Takes {name} to {total_fn:g}h over this week and last — over the "
                              f"{float(visa_cap):g}h fortnight visa cap recorded for them")
        else:
            add("visa", OK, f"{name} would be on {total_fn:g}h over this week and last "
                            f"(recorded visa cap {float(visa_cap):g}h)")
    else:
        add("visa", OK, f"No visa hour cap recorded for {name}")

    rest_needed = timedelta(hours=MINIMUM_HOURS_BETWEEN_SHIFTS)
    worst = None
    for s, where in theirs:
        a, b = _span_dt(s.date, s.start_time, s.end_time)
        if b <= begin:
            gap = begin - b
        elif a >= finish:
            gap = a - finish
        else:
            continue
        if gap < rest_needed and (worst is None or gap < worst[0]):
            worst = (gap, s, where)
    if worst is not None:
        gap, s, where = worst
        when = f"{s.date.strftime('%a')} {span(s.start_time, s.end_time)}"
        at = f" at {where}" if where else ""
        add("rest", WARN, f"Only {round(gap.total_seconds() / 3600, 1):g}h between this shift and {name}'s "
                          f"{when} shift{at} (the rest-gap rule is {MINIMUM_HOURS_BETWEEN_SHIFTS}h — "
                          "advisor to confirm)")
    else:
        add("rest", OK, f"At least {MINIMUM_HOURS_BETWEEN_SHIFTS}h rest either side for {name}")

    blocks = [c["message"] for c in checks if c["result"] == BLOCK]
    return {
        "claimant_id": getattr(claimant, "id", None),
        "claimant_name": getattr(claimant, "name", None),
        "role": role,
        "checks": checks,
        "blocked": bool(blocks),
        "block_reasons": blocks,
        "warnings": [c["message"] for c in checks if c["result"] == WARN],
    }
