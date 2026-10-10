"""
The same person across venues.

Someone who works at two venues has one staff record at each (each venue
prices, rosters and pays its own record), so an employee id never spans
venues. Two records are the same person when they carry the same email,
trimmed and lower-cased. Linking a login with a join code stamps the login's
email onto the record (services/linking.py), so every record a login is
linked to shares that key as well. A record with no email is only itself.

Every lookup is limited to the venues the CALLER may see: the request's
tenant venues (a platform owner, or a call outside any request, sees every
venue). A same-email record at a venue the caller doesn't hold is never
returned, so nothing here reveals another tenant's staff, shifts or venue
names. The demo venue and real venues never match each other.

One exception keeps double-booking checks working for a manager who doesn't
run the other venue (Wembley's assistant manager, staff-only at
Apothecary): with no ``scope``, the shift checks (shifts_elsewhere,
busy_elsewhere, booked_elsewhere, cross_venue_clashes, unseen_shifts) also
search the venues that share an owner with the person's venue
(unseen_venue_ids). There an email match isn't enough, since any manager can
type any email onto a record: the person's own login must hold both venues
(_proven). What they find there carries no venue name or id and no shift
id: the venue is "another venue". Nothing ever looks across owners.

Callers may pass ``scope`` to look in a narrower or different set of venues;
they are then responsible for that set being one the caller may see (the
staff app passes the venues the person's own login is linked to).
"""

from __future__ import annotations

from collections import defaultdict
from datetime import date, datetime, time, timedelta
from typing import Dict, Iterable, List, Optional, Tuple

_INACTIVE_STATUSES = ("cancelled", "no_show")
ANOTHER_VENUE = "another venue"


def person_key(employee) -> Optional[str]:
    email = (getattr(employee, "email", None) or "").strip().lower()
    return email or None


def _demo_venue_id() -> Optional[str]:
    try:
        from rosteriq.services.demo import DEMO_VENUE_ID
        return DEMO_VENUE_ID
    except Exception:  # pragma: no cover
        return None


def visible_venue_ids(db) -> List[str]:
    """The venues the current caller may see."""
    from rosteriq.middleware.tenant import get_tenant_context_optional
    tenant = get_tenant_context_optional()
    if tenant is None or getattr(tenant, "is_owner", False):
        return [v.id for v in (db.list_venues() or []) if getattr(v, "id", None)]
    return list(getattr(tenant, "venue_ids", None) or [])


def _scope(db, home_venue: Optional[str], scope: Optional[Iterable[str]]) -> List[str]:
    venues = list(dict.fromkeys(scope)) if scope is not None else visible_venue_ids(db)
    demo = _demo_venue_id()
    if demo:
        if home_venue == demo:
            venues = [v for v in venues if v == demo]
        else:
            venues = [v for v in venues if v != demo]
    return venues


def unseen_venue_ids(db, home_venue: Optional[str], *, strict: bool = False) -> List[str]:
    """Venues that share an owner with ``home_venue`` but that the current
    caller can't open: a login fully manages both (same_owner_venue_ids) and
    they aren't two separate signups (separate_signups). Only for a manager
    of ``home_venue``; empty outside a request and for a caller who sees
    every venue. With ``strict``, raises when that can't be worked out."""
    from rosteriq.middleware.tenant import get_tenant_context_optional
    tenant = get_tenant_context_optional()
    if tenant is None or getattr(tenant, "is_owner", False) or not home_venue:
        return []
    held = list(getattr(tenant, "venue_ids", None) or [])
    if home_venue not in held or not tenant.is_manager_or_owner():
        return []
    demo = _demo_venue_id()
    if demo and home_venue == demo:
        return []
    from rosteriq.services.venue_access import same_owner_venue_ids, separate_signups
    return [v for v in same_owner_venue_ids(db, home_venue, strict=strict)
            if v not in held and v != demo and not separate_signups(db, home_venue, v, strict=strict)]


def _reach(db, home_venue: Optional[str], scope: Optional[Iterable[str]]) -> Tuple[List[str], set]:
    """(venues a shift check searches, the ones among them to report as
    "another venue")."""
    venues = _scope(db, home_venue, scope)
    if scope is not None:
        return venues, set()
    unseen = [v for v in unseen_venue_ids(db, home_venue) if v not in venues]
    return venues + unseen, set(unseen)


def _proven(db, home_venue: Optional[str], unseen: Iterable[str], keys: Iterable[Optional[str]], *,
            strict: bool = False) -> Dict[str, set]:
    """person key -> the venues among ``unseen`` that ONE live login with
    that email holds together with ``home_venue``. A login holds a venue
    through its own join code there, or through a manager of every venue it
    holds, so this is the person's proof they work both."""
    unseen = set(unseen)
    keys = {k for k in keys if k}
    if not unseen or not keys or not home_venue:
        return {}
    try:
        users = db.list_users() or []
    except Exception:
        if strict:
            raise
        return {}
    from rosteriq.services.venue_access import held_venue_ids
    out: Dict[str, set] = defaultdict(set)
    for u in users:
        if not u or not u.get("is_active", True):
            continue
        key = (u.get("email") or "").strip().lower()
        if key in keys:
            held = set(held_venue_ids(u))
            if home_venue in held:
                out[key] |= held & unseen
    return out


def _shown(vid: str, key: Optional[str], unseen: set, proven: Dict[str, set]) -> bool:
    return vid not in unseen or vid in proven.get(key or "", ())


def _records(db, employee, venue_ids: Iterable[str]) -> List[Tuple[object, str]]:
    key = person_key(employee)
    if not key:
        return []
    own_id = getattr(employee, "id", None)
    out = []
    for vid in venue_ids:
        for other in db.get_employees(vid) or []:
            if getattr(other, "id", None) != own_id and person_key(other) == key:
                out.append((other, vid))
    return out


def same_person_records(db, employee, scope: Optional[Iterable[str]] = None) -> List[Tuple[object, str]]:
    """Every OTHER staff record of this person, as (employee, venue_id), in
    venues the caller may see. Never includes ``employee`` itself."""
    return _records(db, employee, _scope(db, getattr(employee, "venue_id", None), scope))


def records_by_person(db, venue_ids: Iterable[str]) -> Dict[str, List[Tuple[object, str]]]:
    """person key -> [(employee, venue_id)] over ``venue_ids``, read once."""
    index: Dict[str, List[Tuple[object, str]]] = defaultdict(list)
    for vid in venue_ids:
        for emp in db.get_employees(vid) or []:
            key = person_key(emp)
            if key:
                index[key].append((emp, vid))
    return index


def _clock(value) -> Optional[time]:
    if value is None or value == "":
        return None
    if isinstance(value, time):
        return value
    text = str(value).strip()
    try:
        hh, mm = (text.split(":") + ["0"])[:2]
        return time(int(hh) % 24, int(mm[:2]))
    except (TypeError, ValueError):
        return None


def span(day: date, start, end) -> Tuple[datetime, datetime]:
    """[start, end) on ``day``; an end at or before the start runs past
    midnight, and a missing start/end means the whole day."""
    s, e = _clock(start), _clock(end)
    begin = datetime.combine(day, s or time(0, 0))
    if e is None:
        return begin, datetime.combine(day + timedelta(days=1), time(0, 0))
    finish = datetime.combine(day, e)
    if finish <= begin:
        finish += timedelta(days=1)
    return begin, finish


def shift_span(shift) -> Tuple[datetime, datetime]:
    return span(shift.date, shift.start_time, shift.end_time)


def _is_live(shift) -> bool:
    status = getattr(shift, "status", None)
    return str(getattr(status, "value", status)) not in _INACTIVE_STATUSES


def _venue_shifts(db, venue_id: str, start: date, end: date, published_only: bool) -> list:
    from rosteriq.services.current_roster import live_shifts
    try:
        return [s for s in live_shifts(db, venue_id, start, end, staff=published_only) if _is_live(s)]
    except Exception:
        return []


def _venue_name(db, venue_id: Optional[str], cache: Optional[dict] = None) -> str:
    if not venue_id:
        return ANOTHER_VENUE
    if cache is not None and venue_id in cache:
        return cache[venue_id]
    try:
        venue = db.get_venue(venue_id)
    except Exception:
        venue = None
    name = getattr(venue, "name", None) or venue_id
    if cache is not None:
        cache[venue_id] = name
    return name


def shifts_elsewhere(db, employee, start: date, end: date, *,
                     scope: Optional[Iterable[str]] = None,
                     published_only: bool = False) -> List[Tuple[object, Optional[str]]]:
    """This person's shifts at OTHER venues dated start-1 .. end (the day
    before, so a shift running past midnight into ``start`` is seen), as
    (shift, venue_id); venue_id is None for a venue the caller can't see.
    Manager view of each week (the published roster, else the one being
    drafted) unless ``published_only``."""
    home = getattr(employee, "venue_id", None)
    venues, unseen = _reach(db, home, scope)
    key = person_key(employee)
    proven = _proven(db, home, unseen, [key])
    ids_by_venue: Dict[str, set] = defaultdict(set)
    for other, vid in _records(db, employee, venues):
        if vid != home and _shown(vid, key, unseen, proven):
            ids_by_venue[vid].add(str(other.id))
    out = []
    for vid, ids in ids_by_venue.items():
        for s in _venue_shifts(db, vid, start - timedelta(days=1), end, published_only):
            if str(getattr(s, "employee_id", "")) in ids:
                out.append((s, None if vid in unseen else vid))
    return out


def unseen_shifts(db, employee, start: date, end: date, *, strict: bool = False) -> list:
    """This person's live shifts dated start..end at the venues in
    unseen_venue_ids where their own login holds both venues, for counting
    hours and spotting clashes. They carry the other venue's ids, so nothing
    from them is shown but the times. Logins or a roster that can't be read
    raise, so "couldn't look" never reads as "free"; with ``strict`` so does
    a same-email record there that their login doesn't prove, since that
    venue then goes unchecked."""
    from rosteriq.services.current_roster import live_shifts
    home = getattr(employee, "venue_id", None)
    unseen = unseen_venue_ids(db, home, strict=True)
    key = person_key(employee)
    proven = _proven(db, home, unseen, [key], strict=True).get(key or "", set())
    ids_by_venue: Dict[str, set] = defaultdict(set)
    for other, vid in _records(db, employee, unseen):
        if vid not in proven:
            if strict:
                raise LookupError("A same-email record at another venue isn't linked to this person's login")
            continue
        ids_by_venue[vid].add(str(other.id))
    out = []
    for vid, ids in ids_by_venue.items():
        for s in live_shifts(db, vid, start, end):
            if str(getattr(s, "employee_id", "")) in ids and _is_live(s):
                out.append(s)
    return out


def _window(day: date, start, end, gap_minutes) -> Tuple[datetime, datetime]:
    begin, finish = span(day, start, end)
    pad = timedelta(minutes=max(0, int(gap_minutes or 0)))
    return begin - pad, finish + pad


def _hits(db, shifts: Iterable[Tuple[object, Optional[str]]], begin: datetime, finish: datetime,
          names: dict) -> List[dict]:
    hits = []
    for s, vid in shifts:
        a, b = shift_span(s)
        if a < finish and begin < b:
            hits.append({
                "venue_id": vid,
                "venue_name": _venue_name(db, vid, names),
                "date": s.date.isoformat(),
                "start": s.start_time.strftime("%H:%M"),
                "end": s.end_time.strftime("%H:%M"),
                "shift_id": s.id if vid else None,
                "_at": a,
            })
    hits.sort(key=lambda h: h["_at"])
    for h in hits:
        h.pop("_at", None)
    return hits


def busy_elsewhere(db, employee, day: date, start=None, end=None, *,
                   scope: Optional[Iterable[str]] = None, gap_minutes: int = 0,
                   published_only: bool = False) -> List[dict]:
    """Where this person is already rostered at another venue during
    ``start``-``end`` on ``day`` (HH:MM strings or times; omit both for the
    whole day). ``gap_minutes`` widens the window on both sides, for a rest
    gap. Returns [{venue_id, venue_name, date, start, end, shift_id}],
    earliest first; [] when they are free or have no other venue. A venue
    the caller can't see comes back as venue_id None, "another venue", no
    shift_id."""
    begin, finish = _window(day, start, end, gap_minutes)
    shifts = shifts_elsewhere(db, employee, day - timedelta(days=1), day + timedelta(days=1),
                              scope=scope, published_only=published_only)
    return _hits(db, shifts, begin, finish, {})


def busy_elsewhere_many(db, venue_id: str, employees: Iterable, day: date, start=None, end=None, *,
                        scope: Optional[Iterable[str]] = None, gap_minutes: int = 0,
                        published_only: bool = False) -> Dict[str, List[dict]]:
    """busy_elsewhere for a list of ``venue_id``'s staff at once (an SOS
    shortlist), reading each other venue's staff and roster once. Returns
    {employee_id: hits} for the people who are busy; free people are left
    out."""
    begin, finish = _window(day, start, end, gap_minutes)
    employees = list(employees)
    venues, unseen = _reach(db, venue_id, scope)
    others = [v for v in venues if v != venue_id]
    index = records_by_person(db, others)
    proven = _proven(db, venue_id, unseen, (person_key(e) for e in employees))
    wanted: Dict[str, Dict[str, list]] = defaultdict(lambda: defaultdict(list))  # vid -> other id -> local ids
    for emp in employees:
        key = person_key(emp)
        for other, vid in index.get(key or "", []):
            if _shown(vid, key, unseen, proven):
                wanted[vid][str(other.id)].append(str(emp.id))
    shifts_by_local: Dict[str, list] = defaultdict(list)
    for vid, by_id in wanted.items():
        for s in _venue_shifts(db, vid, day - timedelta(days=2), day + timedelta(days=1), published_only):
            for local in by_id.get(str(getattr(s, "employee_id", "")), []):
                shifts_by_local[local].append((s, None if vid in unseen else vid))
    names: dict = {}
    out = {}
    for local, shifts in shifts_by_local.items():
        hits = _hits(db, shifts, begin, finish, names)
        if hits:
            out[local] = hits
    return out


def booked_elsewhere(db, venue_id: str, employees: Iterable, start: date, end: date, *,
                     scope: Optional[Iterable[str]] = None) -> List[Tuple[object, str]]:
    """The other-venue shifts of ``venue_id``'s people dated start..end, as
    (shift, venue_id), each shift copied with the LOCAL employee id so the
    roster generator treats the person as booked then (the day, the rest
    gap and their weekly hours all count). venue_id is None for a venue the
    caller can't see."""
    employees = list(employees)
    keys = {person_key(e): e for e in employees if person_key(e)}
    if not keys:
        return []
    venues, unseen = _reach(db, venue_id, scope)
    others = [v for v in venues if v != venue_id]
    index = records_by_person(db, others)
    proven = _proven(db, venue_id, unseen, keys)
    wanted: Dict[str, Dict[str, object]] = defaultdict(dict)   # vid -> other id -> local emp
    for key, local in keys.items():
        for other, vid in index.get(key, []):
            if _shown(vid, key, unseen, proven):
                wanted[vid][str(other.id)] = local
    out = []
    for vid, by_id in wanted.items():
        for s in _venue_shifts(db, vid, start, end, published_only=False):
            local = by_id.get(str(getattr(s, "employee_id", "")))
            if local is None or not (start <= s.date <= end):
                continue
            out.append((s.model_copy(update={"id": f"elsewhere-{s.id}",
                                             "employee_id": local.id}),
                        None if vid in unseen else vid))
    out.sort(key=lambda pair: (pair[0].date, pair[0].start_time))
    return out


def bookings_elsewhere(db, venue_id: str, employees: Iterable, start: date, end: date, *,
                       scope: Optional[Iterable[str]] = None) -> List[object]:
    """Just the shifts of booked_elsewhere, for the generator."""
    return [s for s, _ in booked_elsewhere(db, venue_id, employees, start, end, scope=scope)]


def _when(shift) -> str:
    return f"{shift.start_time.strftime('%H:%M')}-{shift.end_time.strftime('%H:%M')}"


def _day(d: date) -> str:
    return f"{d.strftime('%a')} {d.day} {d.strftime('%b')}"


def describe_bookings(db, bookings: Iterable[Tuple[object, str]], employees: Iterable) -> List[str]:
    """'Meca Rossi is on at Apothecary 16:00-23:00 on Fri 16 Oct' for each
    (shift, venue_id) from booked_elsewhere."""
    names = {str(e.id): e.name for e in employees}
    venues: dict = {}
    return [f"{names.get(str(s.employee_id), 'Someone')} is on at {_venue_name(db, vid, venues)} "
            f"{_when(s)} on {_day(s.date)}" for s, vid in bookings]


def cross_venue_clashes(db, roster, employees: Optional[Iterable] = None, *,
                        scope: Optional[Iterable[str]] = None,
                        min_rest_hours: Optional[float] = None) -> List[dict]:
    """Shifts on ``roster`` that collide with the same person's shifts at
    another venue: an overlap, or (with ``min_rest_hours``) a turnaround
    shorter than that. Returns [{employee_id, shift_id, date, venue_id,
    venue_name, start, end, kind, message}]."""
    venue_id = getattr(roster, "venue_id", None)
    staff = list(employees) if employees is not None else list(db.get_employees(venue_id) or [])
    by_id = {str(e.id): e for e in staff}
    mine: Dict[str, list] = defaultdict(list)
    for s in getattr(roster, "shifts", None) or []:
        if _is_live(s) and str(s.employee_id) in by_id:
            mine[str(s.employee_id)].append(s)
    if not mine:
        return []
    keyed = {person_key(by_id[eid]): eid for eid in mine if person_key(by_id[eid])}
    if not keyed:
        return []
    venues, unseen = _reach(db, venue_id, scope)
    others = [v for v in venues if v != venue_id]
    index = records_by_person(db, others)
    proven = _proven(db, venue_id, unseen, keyed)
    first = getattr(roster, "week_start", None) or min(s.date for ss in mine.values() for s in ss)
    last = getattr(roster, "week_end", None) or max(s.date for ss in mine.values() for s in ss)
    shift_cache: dict = {}
    names: dict = {}
    rest = timedelta(hours=float(min_rest_hours)) if min_rest_hours else None
    out = []
    for key, local_id in keyed.items():
        emp = by_id[local_id]
        for other, vid in index.get(key, []):
            if not _shown(vid, key, unseen, proven):
                continue
            if vid not in shift_cache:
                shift_cache[vid] = _venue_shifts(db, vid, first - timedelta(days=1),
                                                 last + timedelta(days=1), published_only=False)
            theirs = [s for s in shift_cache[vid] if str(s.employee_id) == str(other.id)]
            for here in mine[local_id]:
                a, b = shift_span(here)
                for there in theirs:
                    c, d = shift_span(there)
                    if a < d and c < b:
                        kind = "overlap"
                    elif rest is not None and (timedelta(0) <= c - b < rest or timedelta(0) <= a - d < rest):
                        kind = "short_rest"
                    else:
                        continue
                    shown = None if vid in unseen else vid
                    vname = _venue_name(db, shown, names)
                    when = _when(there)
                    day = _day(there.date)
                    if kind == "overlap":
                        msg = f"{emp.name} is on at {vname} {when} on {day}"
                    else:
                        gap_h = round(min(abs((c - b).total_seconds()), abs((a - d).total_seconds())) / 3600, 1)
                        msg = (f"{emp.name} is on at {vname} {when} on {day} — only "
                               f"{gap_h:g}h between that shift and this one")
                    out.append({
                        "employee_id": emp.id, "shift_id": here.id, "date": here.date,
                        "venue_id": shown, "venue_name": vname,
                        "start": there.start_time.strftime("%H:%M"),
                        "end": there.end_time.strftime("%H:%M"),
                        "kind": kind, "message": msg,
                    })
    out.sort(key=lambda c: (c["date"], c["employee_id"]))
    return out


def _worked_hours(sheet: dict) -> float:
    ci, co = sheet.get("clock_in"), sheet.get("clock_out")
    if not ci or not co:
        return 0.0
    try:
        a = ci if isinstance(ci, datetime) else datetime.fromisoformat(str(ci).replace("Z", "+00:00"))
        b = co if isinstance(co, datetime) else datetime.fromisoformat(str(co).replace("Z", "+00:00"))
    except ValueError:
        return 0.0
    a, b = a.replace(tzinfo=None), b.replace(tzinfo=None)
    minutes = (b - a).total_seconds() / 60 - int(sheet.get("break_minutes") or 0)
    return max(0.0, minutes / 60)


def _work_day(sheet: dict) -> Optional[date]:
    wd = sheet.get("work_date")
    if isinstance(wd, datetime):
        return wd.date()
    if isinstance(wd, date):
        return wd
    try:
        return date.fromisoformat(str(wd)[:10])
    except (TypeError, ValueError):
        return None


def combined_hours_flags(db, venue_id: str, start: date, end: date, venue_group: Iterable[str],
                         weekly_threshold: float) -> List[dict]:
    """People at ``venue_id`` whose worked hours across ``venue_group`` pass
    ``weekly_threshold`` in a Mon-Sun week of start..end while working at
    more than one venue that week. Flags only: whether the venues are one
    employer, and so whether overtime is owed, is for the owner's advisor.
    Salaried staff are skipped (their pay never carries overtime pricing)."""
    group = [v for v in dict.fromkeys(venue_group) if v]
    if venue_id not in group:
        group.insert(0, venue_id)
    if len(group) < 2:
        return []
    index = records_by_person(db, group)
    home_staff = [e for e in (db.get_employees(venue_id) or [])
                  if getattr(e, "pay_basis", "hourly") != "salaried"]
    names: dict = {}
    monday = start - timedelta(days=start.weekday())
    sheets_by_venue = {v: db.get_timesheets(v, monday, end + timedelta(days=6 - end.weekday())) or []
                       for v in group}
    out = []
    for emp in home_staff:
        key = person_key(emp)
        if not key:
            continue
        records = [(e, v) for e, v in index.get(key, []) if v in group]
        if len({v for _, v in records}) < 2:
            continue
        weeks: Dict[date, Dict[str, float]] = defaultdict(lambda: defaultdict(float))
        for rec, vid in records:
            for t in sheets_by_venue.get(vid, []):
                if str(t.get("employee_id")) != str(rec.id):
                    continue
                day = _work_day(t)
                if day is None:
                    continue
                weeks[day - timedelta(days=day.weekday())][vid] += _worked_hours(t)
        for wk, per_venue in sorted(weeks.items()):
            if wk + timedelta(days=6) < start or wk > end:
                continue
            worked = {v: h for v, h in per_venue.items() if h > 0}
            total = round(sum(worked.values()), 2)
            if len(worked) < 2 or venue_id not in worked or total <= float(weekly_threshold):
                continue
            parts = ", ".join(f"{_venue_name(db, v, names)} {round(h, 2):g}h"
                              for v, h in worked.items())
            across = "both venues" if len(worked) == 2 else f"{len(worked)} venues"
            out.append({
                "employee_id": emp.id, "name": emp.name, "week_start": wk.isoformat(),
                "total_hours": total,
                "per_venue": {v: round(h, 2) for v, h in worked.items()},
                "message": (f"{emp.name}: {total:g}h across {across} in the week of "
                            f"{wk.strftime('%d %b').lstrip('0')} ({parts}) — check overtime "
                            f"with your advisor"),
            })
    return out
