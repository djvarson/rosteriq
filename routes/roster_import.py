"""
Roster import from any program's export — bring a switching venue's existing
schedule across without a live connection. Paste a shift export from Deputy /
Tanda / a spreadsheet; we match each shift to a staff member (by email or
name), group the shifts into Mon-Sun weeks, cost them from the rates the
venue recorded, and save them as rosters.

Real exports are messy, so the parser is forgiving:
- delimiter sniffed (comma/tab/semicolon/pipe), Excel BOM stripped
- header mapped by column aliases; First + Last name columns combined
- dates as DD/MM/YYYY, ISO, 28-Sep-2026, with a leading weekday word, or
  carried inside the Start/End cell ('28/09/2026 17:00'); the day/month order
  is decided once per file (AU unless a date only reads month-first, as in
  10/13/2026), and a file whose dates read both ways is refused
- 12h ("5:30pm") and 24h ("17:30", "17:30:00") times
- meal breaks as minutes, H:MM, HH:MM:SS, decimal hours, '1h', '30 min'
- an export covering several locations keeps only this venue's rows
- every row reported back: imported, replaced or skipped-with-reason.

A paste is the truth for each person it names, on the dates it covers: their
imported shifts on those dates become exactly the pasted ones. An identical
shift is skipped (never doubled), a moved one is reported 'replaced', one no
longer in the export is removed, and split shifts stay as two. A row that
can't be read leaves that person's shifts on that date alone. Only the
imported roster is written — never a generated roster, and never a week
published in RosterIQ (a publish supersedes the week's import for good, so
a recalled week stays RosterIQ's too); the response names those weeks.

Approved leave wins, as everywhere else: a row for someone on approved leave
that day, whose shift hasn't started, comes in as an open shift posted for
cover (or, on a re-paste, finds the shift their leave left open, moving it
when the export moved its times), and is flagged. A row for anyone else in the
slot of a stored open shift, or overlapping it in its area, fills it.

Spans of zero or over 16 hours are skipped. In a file with no am/pm
anywhere, an end before its start that would make sense 12 hours later is
flagged as an ambiguous 12-hour time.

Route:
    POST /api/setup/import-roster  { venue_id, content, location?: str }
"""

import asyncio
import logging
import re
import uuid
from collections import Counter
from datetime import date, datetime, time, timedelta
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from rosteriq.cost_calculator import calculate_roster_cost, calculate_shift_cost_breakdown
from rosteriq.database import get_db
from rosteriq.middleware.auth import UserContext, get_current_user
from rosteriq.middleware.tenant import enforce_venue_manager
from rosteriq.models import Roster, Shift, ShiftStatus, State, normalise_area
from rosteriq.services import roster_edit as edit
from rosteriq.services.clock import venue_now
from rosteriq.services.current_roster import (
    ARCHIVED, PUBLISHED, SUPERSEDED, roster_state, supersede_for_import, week_monday,
)
from rosteriq.services.import_parsing import (
    LOCATION_KEYS, LocationChoiceNeeded, MixedDateOrder, PasteUnreadable, build_name, cell_date,
    clean_header, date_order, header_matches, location_choice_response, location_matches,
    map_columns, norm_header, norm_name, read_rows, resolve_location, split_when, unclosed_quote,
)
from rosteriq.services.leave_shifts import (
    LIVE_COVER, OPEN, _new_cover, cover_day, is_open, shift_label,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/setup", tags=["setup"])


class RosterImportBody(BaseModel):
    venue_id: str
    content: str = Field(..., min_length=1, max_length=1_000_000)
    location: Optional[str] = Field(
        None, max_length=200,
        description="When the export covers several locations, the value to keep")


_NAME_KEYS = {"name", "employee", "employee name", "staff", "staff name", "full name",
              "display name"}
_FIRST_KEYS = {"first name", "firstname", "given name", "first"}
_LAST_KEYS = {"last name", "surname", "lastname", "family name", "last"}
_EMAIL_KEYS = {"email", "e-mail", "email address", "e-mail address", "work email"}
_DATE_KEYS = {"date", "shift date", "day", "work date", "roster date", "start date"}
_START_KEYS = {"start", "start time", "from", "shift start", "starttime", "shift start time",
               "start date time", "start datetime", "start date/time", "time in"}
_END_KEYS = {"end", "end time", "to", "finish", "finish time", "shift end", "endtime",
             "shift end time", "end date time", "end datetime", "end date/time", "time out"}
_ROLE_KEYS = {"role", "area", "position", "team", "department", "job", "operational unit",
              "work area"}
_BREAK_KEYS = {"break", "break minutes", "meal break", "unpaid break", "mealbreak",
               "break mins", "break length", "break duration", "unpaid break minutes",
               "meal break minutes", "breaks"}

_FIELDS = [
    ("name", _NAME_KEYS), ("first_name", _FIRST_KEYS), ("last_name", _LAST_KEYS),
    ("email", _EMAIL_KEYS), ("date", _DATE_KEYS), ("start", _START_KEYS),
    ("end", _END_KEYS), ("role", _ROLE_KEYS), ("break", _BREAK_KEYS),
    ("location", LOCATION_KEYS),
]
_HEADER_HINT_KEYS = _NAME_KEYS | _FIRST_KEYS | _LAST_KEYS | _DATE_KEYS | _START_KEYS | _END_KEYS

_BREAK_FLAG_MINUTES = 120
_MAX_SPAN_MINUTES = 16 * 60
_AMPM_RE = re.compile(r"\d\s*[ap]\.?\s*m?\.?(?![a-z])", re.IGNORECASE)


def _parse_time(v: str):
    """'17:30' / '17:30:00' / '5:30pm' / '9am' / '1730' -> time, else None."""
    s = str(v or "").strip().lower().replace(" ", "")
    s = s.replace("a.m.", "am").replace("p.m.", "pm").replace("a.m", "am").replace("p.m", "pm")
    if not s:
        return None
    m = re.match(r"^(\d{1,2})(?::?(\d{2}))?(?::(\d{2})(?:\.\d+)?)?(am|pm|a|p)?$", s)
    if not m:
        return None
    hh = int(m.group(1))
    mm = int(m.group(2) or 0)
    ap = m.group(4)
    if ap in ("pm", "p") and hh < 12:
        hh += 12
    elif ap in ("am", "a") and hh == 12:
        hh = 0
    if hh == 24 and mm == 0 and not ap:
        hh = 0                                  # a spreadsheet's '24:00' close
    if not (0 <= hh <= 23 and 0 <= mm <= 59):
        return None
    return time(hh, mm)


def _break_unit(header: str) -> Optional[str]:
    h = norm_header(header)
    if "min" in h:
        return "minutes"
    if re.search(r"hour|hrs|\(h\)", h):
        return "hours"
    return None


def _parse_break(v: str, unit: Optional[str]):
    """-> (minutes, flag | None). H:MM and HH:MM:SS are hours:minutes; a bare
    number under 10 is hours unless the header says minutes; '1h', '30m',
    '30 min', '1h 30m' read as written. Long breaks are flagged, not capped."""
    s = str(v or "").strip().lower()
    if not s or s in {"-", "none", "nil", "n/a", "na"}:
        return 0, None
    m = re.fullmatch(r"(\d{1,2}):(\d{2})(?::(\d{2}))?", s)
    if m:
        mins = int(m.group(1)) * 60 + int(m.group(2)) + round(int(m.group(3) or 0) / 60)
    elif re.fullmatch(r"\d+(?:\.\d+)?", s):
        x = float(s)
        if unit == "minutes":
            mins = round(x)
        elif unit == "hours" or x < 10:
            mins = round(x * 60)
        else:
            mins = round(x)
    elif m := re.fullmatch(r"(\d+(?:\.\d+)?)\s*h(?:rs?|ours?)?(?:\s*(\d+)\s*(?:m|mins?|minutes?)?)?", s):
        mins = round(float(m.group(1)) * 60) + int(m.group(2) or 0)
    elif m := re.fullmatch(r"(\d+)\s*(?:m|mins?|minutes?)", s):
        mins = int(m.group(1))
    else:
        return 0, "break_unreadable"
    if mins > _BREAK_FLAG_MINUTES:
        return mins, "break_over_120"
    return mins, None


def _span_minutes(st: time, en: time) -> int:
    return (en.hour * 60 + en.minute - st.hour * 60 - st.minute) % (24 * 60)


def _interval(s: Shift):
    start = datetime.combine(s.date, s.start_time)
    end = datetime.combine(s.date, s.end_time)
    return start, (end if end > start else end + timedelta(days=1))


def _overlaps(a: Shift, b: Shift) -> bool:
    a0, a1 = _interval(a)
    b0, b1 = _interval(b)
    return a0 < b1 and b0 < a1


def _hhmm(s: Shift) -> str:
    return f"{s.start_time:%H:%M}-{s.end_time:%H:%M}"


def _read_roster_paste(content: str, venue_name: str, location: Optional[str],
                       by_email: dict, by_name: dict) -> dict:
    """The paste read, matched to staff and checked, row by row. Pure CPU, run
    off the event loop; nothing here touches the database. Raises
    HTTPException (422) or LocationChoiceNeeded."""
    try:
        rows = read_rows(content)
    except PasteUnreadable as e:
        raise HTTPException(status_code=422, detail=str(e))
    if not rows:
        raise HTTPException(status_code=422, detail="No rows found")

    header = rows[0][1]
    has_header = any(header_matches(norm_header(h), _HEADER_HINT_KEYS) for h in header)
    if not has_header:
        raise HTTPException(status_code=422,
                            detail="Couldn't find a header row — the roster export needs columns "
                                   "like Name, Date, Start, End (and optionally Role, Break).")
    bad_quote = unclosed_quote(*rows[0], len(header))
    if bad_quote:
        raise HTTPException(status_code=422, detail=f"Couldn't read the header row: {bad_quote}")
    col, column_map, unmapped = map_columns(header, _FIELDS, multi=("date",))
    data = [(line, [c.strip() for c in raw]) for line, raw in rows[1:]]

    def cell(cells, field):
        j = col[field]
        return cells[j] if j is not None and j < len(cells) else ""

    date_cols = col["date"] + [j for j in (col["start"], col["end"]) if j is not None]
    try:
        order = date_order((line, cells[j]) for line, cells in data for j in date_cols if j < len(cells))
    except MixedDateOrder as e:
        raise HTTPException(status_code=422, detail=str(e))

    missing = [f for f in ("start", "end") if col[f] is None]
    if not col["date"] and col["start"] is not None and not any(
            split_when(cell(cells, "start"), order)[0] for _, cells in data):
        missing.insert(0, "date")
    if all(col[f] is None for f in ("name", "email", "first_name", "last_name")):
        missing.append("name or email")
    if missing:
        raise HTTPException(status_code=422,
                            detail=f"Export is missing required column(s): {', '.join(missing)}")
    break_unit = _break_unit(header[col["break"]]) if col["break"] is not None else None

    location_info, kept_location = None, None
    if col["location"] is not None:
        active, kept_location, _ = resolve_location(
            [cell(cells, "location") for _, cells in data], venue_name, location)
        if active:
            location_info = {"column": clean_header(header[col["location"]]),
                             "kept": kept_location, "skipped_other": 0}

    twelve_hour = any(_AMPM_RE.search(cell(cells, f)[:40]) for _, cells in data for f in ("start", "end"))

    out = {"column_map": column_map, "unmapped": unmapped, "location": location_info,
           "date_order": order,
           "pasted": {},          # (employee_id, date) -> [(line, name, Shift)]
           "covered": {},         # week -> employees with a row in it
           "paste_dates": set(),  # every date a row for this venue names
           "held": set(),         # (employee_id, date) with a row that couldn't be read
           "swallowed": False,    # an unclosed quote hid rows: nothing is removed
           "duplicates": 0, "skipped": [], "report": [], "flagged": []}
    pasted, held, flagged = out["pasted"], out["held"], out["flagged"]
    seen = set()
    for line, cells in data:
        full = cell(cells, "name")
        first_last = build_name("", cell(cells, "first_name"), cell(cells, "last_name"))
        shown = build_name(full, cell(cells, "first_name"), cell(cells, "last_name"))
        email = cell(cells, "email").strip().lower()
        date_raw = next((cells[j] for j in col["date"] if j < len(cells) and cells[j]), "")
        d = next((dd for dd in (cell_date(cells[j], order) for j in col["date"] if j < len(cells)) if dd),
                 None)
        st_date, st_txt = split_when(cell(cells, "start"), order)
        _, en_txt = split_when(cell(cells, "end"), order)
        d = d or st_date

        def skip(reason):
            _note_row(out, line, shown or email, d, "skipped", reason, date_raw)

        bad_quote = unclosed_quote(line, cells, len(header))
        if bad_quote:
            out["swallowed"] = True
            skip(bad_quote)
            continue

        loc_cell = cell(cells, "location")
        if kept_location and not location_matches(loc_cell, kept_location):
            location_info["skipped_other"] += 1
            skip(f"other location: {loc_cell}")
            continue
        if d:
            out["paste_dates"].add(d)

        emp_id, ambiguous = None, None
        if email and len(by_email.get(email, [])) == 1:
            emp_id = by_email[email][0]
        if not emp_id:
            for nm in dict.fromkeys(n for n in (first_last, full, shown) if n):
                ids = by_name.get(norm_name(nm), [])
                if len(ids) == 1:
                    emp_id = ids[0]
                    break
                if len(ids) > 1 and not ambiguous:
                    ambiguous = (nm, len(ids))
        if not emp_id:
            if ambiguous:
                nm, n = ambiguous
                fix = ("add an Email column or rename one" if col["email"] is None
                       else "put their email on this row or rename one")
                skip(f"{'two' if n == 2 else n} staff named {nm} — {fix}")
            else:
                skip("no matching staff member")
            continue

        if not d:
            skip(f"bad date '{date_raw or cell(cells, 'start')}'")
            continue
        out["covered"].setdefault(week_monday(d), set()).add(emp_id)

        st = _parse_time(st_txt)
        en = _parse_time(en_txt)
        if not st or not en:
            held.add((emp_id, d))
            skip("bad start/end time")
            continue
        span = _span_minutes(st, en)
        twelve_hour_end = (not twelve_hour and 1 <= en.hour <= 11 and st.hour <= 12 and en < st
                           and time(en.hour + 12, en.minute) > st)
        later_end = f"{en.hour + 12}:{en.minute:02d}"
        if span == 0:
            held.add((emp_id, d))
            skip(f"starts and ends at {st:%H:%M} — a shift needs a length")
            continue
        if span > _MAX_SPAN_MINUTES:
            held.add((emp_id, d))
            reason = f"{st_txt} to {en_txt} reads as a {span / 60:g}-hour shift"
            if twelve_hour_end:
                reason += (f" — ambiguous 12-hour time; if it ends at {later_end}, write that "
                           "(or add am/pm) and paste again")
            else:
                reason += " — check the times and paste again"
            skip(reason)
            continue

        key = (emp_id, d, st, en)
        if key in seen:
            out["duplicates"] += 1
            skip("already imported")
            continue
        seen.add(key)

        brk, brk_flag = _parse_break(cell(cells, "break"), break_unit)
        if brk_flag:
            detail = (f"meal break '{cell(cells, 'break')}' read as {brk} minutes — check it"
                      if brk_flag == "break_over_120" else
                      f"meal break '{cell(cells, 'break')}' couldn't be read — imported with no break")
            flagged.append({"line": line, "name": shown or email, "date": d.isoformat(),
                            "flag": brk_flag, "detail": detail})
        if twelve_hour_end:
            flagged.append({"line": line, "name": shown or email, "date": d.isoformat(),
                            "flag": "ambiguous_12h",
                            "detail": f"ambiguous 12-hour time: {st_txt} to {en_txt} imported as an "
                                      f"overnight {span / 60:g}-hour shift — if it ends at "
                                      f"{later_end}, write that and paste again"})
        pasted.setdefault((emp_id, d), []).append((line, shown or email, Shift(
            id=f"impshift-{uuid.uuid4().hex[:10]}",
            employee_id=emp_id, date=d, start_time=st, end_time=en,
            break_minutes=brk, status=ShiftStatus.scheduled,
            role=(cell(cells, "role") or ""),
        )))
    return out


def _note_row(out: dict, line, name, d, status, reason, date_raw=""):
    if status == "skipped":
        out["skipped"].append({"row": line, "name": name, "reason": reason})
    out["report"].append({"line": line, "name": name, "date": d.isoformat() if d else date_raw,
                          "status": status, "reason": reason})


def _paste_weeks(paste: dict) -> list:
    return sorted(set(paste["covered"]) | {week_monday(d) for _, d in paste["pasted"]})


def _held(roster) -> dict:
    """What a stored week holds, to tell whether it changed since it was read."""
    return {s.id: s.model_dump() for s in (roster.shifts if roster else [])}


def _slot(s: Shift) -> tuple:
    return (s.date, s.start_time, s.end_time, normalise_area(s.role))


def _open_slot(old: list, sh: Shift, kept_ids: set):
    return next((s for s in old if is_open(s) and s.id not in kept_ids and _slot(s) == _slot(sh)),
                None)


def _left_open(old: list, sh: Shift, ids) -> Optional[Shift]:
    """The stored shift in the row's slot that the person's approved leave
    left open (it may have been covered since)."""
    return next((s for s in old if s.id in ids and _slot(s) == _slot(sh)), None)


def _open_overlap(old: list, sh: Shift, kept_ids: set, ids=None) -> Optional[Shift]:
    """The open shift (among ids, when given) not yet matched, on the row's
    day and in its area, whose times overlap the row's most."""
    best, most = None, timedelta(0)
    b0, b1 = _interval(sh)
    for s in old:
        if (not is_open(s) or s.id in kept_ids or (ids is not None and s.id not in ids)
                or s.date != sh.date or normalise_area(s.role) != normalise_area(sh.role)):
            continue
        a0, a1 = _interval(s)
        shared = min(a1, b1) - max(a0, b0)
        if shared > most:
            best, most = s, shared
    return best


def _fill(out: dict, updates: dict, kept_ids: set, hole: Shift, change: dict) -> None:
    kept_ids.add(hole.id)
    updates[hole.id] = hole.model_copy(update=change)
    out["filled"].append((hole, updates[hole.id]))
    out["replaced"] += 1


def _leave_flag(line, name, d, detail: str) -> dict:
    return {"line": line, "name": name, "date": d.isoformat(), "flag": "on_leave", "detail": detail}


def _reconcile_week(paste: dict, week_start, roster, published: bool, emp_map: dict, state,
                    venue_id: str, leave: dict, now: datetime) -> dict:
    """One week of the paste against the imported roster stored for it (None
    when there is none): the roster to save (None when nothing changes) and
    what became of each row. Notes go in the result rather than the paste, so
    a week that changed before it could be saved can be redone.

    leave: {"days": edit.approved_leave(), "opened": {(employee_id, date):
    ids of the shifts their approved leave left open}}; now: venue-local."""
    out = {"week_start": week_start, "read": _held(roster), "roster": None, "imported": 0,
           "replaced": 0, "removed": [], "duplicates": 0, "skipped": [], "report": [],
           "flagged": [], "filled": [], "left_open": [], "published": published}
    pasted, held, covered = paste["pasted"], paste["held"], paste["covered"]
    week_items = sorted(((pair, line, name, sh) for pair, items in pasted.items()
                         if week_monday(pair[1]) == week_start for line, name, sh in items),
                        key=lambda x: x[1])
    if published:
        for (_, d), line, name, _ in week_items:
            _note_row(out, line, name, d, "skipped",
                      f"the week of {week_start:%d/%m/%Y} has been published in RosterIQ, so staff "
                      "see RosterIQ's roster for it, never an import \u2014 not imported; make the "
                      "change in RosterIQ")
        return out
    old = list(roster.shifts) if roster else []
    by_key = {}
    for s in old:
        by_key.setdefault((s.employee_id, s.date, s.start_time, s.end_time), s)
    kept_ids, updates, added, off, loose = set(), {}, [], [], []
    for (emp_id, d), line, name, sh in week_items:
        prev = by_key.get((emp_id, d, sh.start_time, sh.end_time))
        if prev is None or prev.id in kept_ids:
            # A day they work part of still leaves open the shifts the leave opened.
            if datetime.combine(d, sh.start_time) > now and (
                    edit.on_leave(leave["days"], emp_id, d)
                    or _left_open(old, sh, leave["opened"].get((emp_id, d), ())) is not None):
                off.append((line, name, sh))
                continue
            hole = _open_slot(old, sh, kept_ids)
            if hole is None:
                loose.append((line, name, sh))
                continue
            _fill(out, updates, kept_ids, hole,
                  {"employee_id": emp_id, "break_minutes": sh.break_minutes})
            _note_row(out, line, name, d, "replaced", f"filled the open shift {_hhmm(hole)}")
            continue
        kept_ids.add(prev.id)
        if (prev.break_minutes, prev.role) != (sh.break_minutes, sh.role):
            updates[prev.id] = prev.model_copy(
                update={"break_minutes": sh.break_minutes, "role": sh.role})
            out["replaced"] += 1
            _note_row(out, line, name, d, "replaced",
                      f"meal break or area changed (was {prev.break_minutes} min, "
                      f"{prev.role or 'no area'})")
        else:
            out["duplicates"] += 1
            _note_row(out, line, name, d, "skipped", "already imported")
    # After every other row, so someone pasted into the slot keeps it.
    retimed = []
    for line, name, sh in off:
        d, mine = sh.date, leave["opened"].get((sh.employee_id, sh.date), ())
        left = _left_open(old, sh, mine) or _open_slot(old, sh, kept_ids)
        if left is None:
            retimed.append((line, name, sh))
            continue
        kept_ids.add(left.id)
        out["duplicates"] += 1
        _note_row(out, line, name, d, "skipped",
                  f"already imported \u2014 {name} is on approved leave in RosterIQ that day, "
                  "so their shift was left open for cover")
        out["flagged"].append(_leave_flag(line, name, d, (
            f"{name} is on approved leave in RosterIQ that day; the export still has them on "
            f"{_hhmm(sh)}, which the leave left open for cover")))
    # A row that matches nothing stored may be an open shift the export moved:
    # it fills the one it overlaps, unless it's the person's own shift moved.
    for line, name, sh in loose:
        hole = None
        if not any(s.employee_id == sh.employee_id and s.date == sh.date
                   and s.id not in kept_ids and _overlaps(s, sh) for s in old):
            hole = _open_overlap(old, sh, kept_ids)
        if hole is None:
            added.append((line, name, sh))
            continue
        _fill(out, updates, kept_ids, hole,
              {"employee_id": sh.employee_id, "break_minutes": sh.break_minutes,
               "start_time": sh.start_time, "end_time": sh.end_time})
        _note_row(out, line, name, sh.date, "replaced",
                  f"filled the open shift {_hhmm(hole)}, moved to {_hhmm(sh)}")
    for line, name, sh in retimed:
        d, mine = sh.date, leave["opened"].get((sh.employee_id, sh.date), ())
        hole = _open_overlap(old, sh, kept_ids, mine)
        taken = None if hole else next(
            (s for s in old if s.id in mine and s.date == d and _overlaps(s, sh)
             and normalise_area(s.role) == normalise_area(sh.role)), None)
        if taken is not None:
            kept_ids.add(taken.id)
            out["duplicates"] += 1
            _note_row(out, line, name, d, "skipped",
                      f"{name} is on approved leave in RosterIQ that day and the shift their "
                      f"leave left open ({_hhmm(taken)}) has been covered \u2014 not imported")
            out["flagged"].append(_leave_flag(line, name, d, (
                f"{name} is on approved leave in RosterIQ that day; the export has them on "
                f"{_hhmm(sh)}, and the shift their leave left open ({_hhmm(taken)}) has been "
                "covered \u2014 check its times")))
            continue
        hole = hole or _open_overlap(old, sh, kept_ids)
        if hole is not None:
            _fill(out, updates, kept_ids, hole,
                  {"break_minutes": sh.break_minutes, "start_time": sh.start_time,
                   "end_time": sh.end_time})
            _note_row(out, line, name, d, "replaced",
                      f"moved the open shift {_hhmm(hole)} to {_hhmm(sh)} \u2014 {name} is on "
                      "approved leave in RosterIQ that day, so it stays open for cover")
            out["flagged"].append(_leave_flag(line, name, d, (
                f"{name} is on approved leave in RosterIQ that day \u2014 the open shift "
                f"{_hhmm(hole)} moved to {_hhmm(sh)} and stays posted for cover")))
            continue
        hole = sh.model_copy(update={"employee_id": OPEN, "cost": None})
        added.append((line, name, hole))
        out["left_open"].append((hole, sh.employee_id))
        out["flagged"].append(_leave_flag(line, name, d, (
            f"{name} is on approved leave in RosterIQ that day \u2014 {_hhmm(sh)} left open "
            "and posted for cover")))
    gone = [s for s in old
            if s.id not in kept_ids and not paste["swallowed"]
            and s.employee_id in covered.get(week_start, ())
            and s.date in paste["paste_dates"] and (s.employee_id, s.date) not in held]
    replaced_ids = set()
    for line, name, sh in added:
        moved = [g for g in gone if g.employee_id == sh.employee_id and g.date == sh.date
                 and _overlaps(g, sh)]
        if moved:
            out["replaced"] += 1
            replaced_ids.update(g.id for g in moved)
            _note_row(out, line, name, sh.date, "replaced",
                      "replaced " + ", ".join(_hhmm(g) for g in moved))
    out["imported"] += len(added)
    for g in gone:
        if g.id not in replaced_ids:
            out["removed"].append({
                "name": getattr(emp_map.get(g.employee_id), "name", g.employee_id),
                "date": g.date.isoformat(), "start": f"{g.start_time:%H:%M}",
                "end": f"{g.end_time:%H:%M}"})
    if not (added or updates or gone):
        return out
    gone_ids = {g.id for g in gone}
    # Copies: costing must not touch the stored week, which may be redone.
    shifts = ([updates[s.id] if s.id in updates else s.model_copy() for s in old if s.id not in gone_ids]
              + [sh for _, _, sh in added])
    for s in shifts:
        if s.employee_id in emp_map:
            s.cost = calculate_shift_cost_breakdown(emp_map[s.employee_id], s, state).total_cost
    rebuilt = Roster(
        id=f"imported-{venue_id}-{week_start.isoformat()}", venue_id=venue_id,
        week_start=week_start, week_end=week_start + timedelta(days=6),
        shifts=shifts, total_cost=None,
        created_at=roster.created_at if roster else datetime.utcnow(),
    )
    # Saved as the stored week it replaces: Postgres deletes only ids the
    # copy held when read, so the gone shifts go and one added since stays.
    rebuilt._saved_shift_ids = frozenset(s.id for s in old)
    costable = [s for s in shifts if s.employee_id in emp_map]
    rebuilt.total_cost = calculate_roster_cost(
        rebuilt.model_copy(update={"shifts": costable}), emp_map, state)
    out["roster"] = rebuilt
    return out


def _reconcile(paste: dict, existing: dict, emp_map: dict, state, venue_id: str, leave: dict,
               now: datetime) -> list:
    """Each week's imported roster rebuilt from the paste and costed from the
    rates the venue recorded. Pure CPU, run off the event loop. existing:
    {monday: (imported roster | None, published in RosterIQ)}."""
    return [_reconcile_week(paste, week_start, *existing[week_start], emp_map, state, venue_id,
                            leave, now)
            for week_start in _paste_weeks(paste)]


def _published_in_rosteriq(db, venue_id: str, monday, imported) -> bool:
    """A roster of the week is published or archived, or a publish superseded
    the week's import (recalling that roster never brings the import back)."""
    if imported is not None and roster_state(db, imported.id) == SUPERSEDED:
        return True
    rosters = db.get_rosters_by_date_range(venue_id, monday, monday + timedelta(days=6)) or []
    return any(getattr(r, "venue_id", None) == venue_id and week_monday(r.week_start) == monday
               and roster_state(db, r.id) in (PUBLISHED, ARCHIVED) for r in rosters)


def _save_week(db, paste: dict, week: dict, emp_map: dict, state, venue_id: str, leave: dict,
               now: datetime) -> dict:
    """Saves the week's roster under the roster lock. If another import (a
    double click) or an edit saved the week after it was read, or a roster of
    the week was published since, the week is redone against what is stored
    now. This runs on the event loop: the lock is a threading lock, so it
    can't be held across an await."""
    rid = week["roster"].id
    with edit.roster_lock(db, rid):
        fresh = db.get_roster(rid)
        published = _published_in_rosteriq(db, venue_id, week["week_start"], fresh)
        if published or _held(fresh) != week["read"]:
            week = _reconcile_week(paste, week["week_start"], fresh, published, emp_map, state,
                                   venue_id, leave, now)
        if week["roster"] is not None:
            db.save_roster(week["roster"])
    return week


def _paste_leave(db, venue_id: str) -> dict:
    """Approved leave as _reconcile_week reads it. The shifts a person's leave
    left open, on days it still covers, are the ones its record opened and the
    ones its covers were posted for (a paste records none on the leave), even
    once someone has been approved onto them."""
    opened, spans = {}, {}
    for r in db.list_leave_requests(venue_id) or []:
        if r.get("status") != "approved":
            continue
        who = str(r.get("employee_id") or "")
        try:
            first = date.fromisoformat(str(r.get("start_date"))[:10])
            last = date.fromisoformat(str(r.get("end_date"))[:10])
        except ValueError:
            continue
        spans.setdefault(who, []).append((first, last))
        for o in r.get("opened_shifts") or []:
            try:
                d = date.fromisoformat(str(o.get("date"))[:10])
            except ValueError:
                continue
            if first <= d <= last:
                opened.setdefault((who, d), set()).update(o.get("shift_ids") or [o.get("shift_id")])
    for cv in db.list_shift_covers(venue_id) or []:
        who, d = str(cv.get("requested_by") or ""), cover_day(cv)
        if (cv.get("origin") == "leave" and cv.get("status") in LIVE_COVER + ("approved",)
                and d is not None and any(a <= d <= b for a, b in spans.get(who, ()))):
            opened.setdefault((who, d), set()).add(cv.get("shift_id"))
    return {"days": edit.approved_leave(db, venue_id), "opened": opened}


def _skip_week(paste: dict, week_start, reason: str) -> None:
    for (_, d), items in paste["pasted"].items():
        if week_monday(d) == week_start:
            for line, name, _ in items:
                _note_row(paste, line, name, d, "skipped", reason)


@router.post("/import-roster")
async def import_roster(body: RosterImportBody,
                        user: UserContext = Depends(get_current_user)) -> dict:
    enforce_venue_manager(body.venue_id)
    db = get_db()
    venue = db.get_venue(body.venue_id)
    if not venue:
        raise HTTPException(status_code=404, detail="Venue not found")
    state = getattr(venue, "state", State.nsw)

    employees = db.get_employees(body.venue_id) or []
    if not employees:
        raise HTTPException(status_code=422,
                            detail="Import your staff first — shifts are matched to staff by name/email.")
    by_email, by_name = {}, {}
    for e in employees:
        em = (getattr(e, "email", "") or "").strip().lower()
        if em:
            by_email.setdefault(em, []).append(e.id)
        by_name.setdefault(norm_name(getattr(e, "name", "")), []).append(e.id)
    emp_map = {e.id: e for e in employees}

    # Reading and costing run in a worker thread; every database call stays
    # on the event loop, which PostgresStore's one shared connection needs.
    try:
        paste = await asyncio.to_thread(_read_roster_paste, body.content,
                                        getattr(venue, "name", "") or "", body.location,
                                        by_email, by_name)
    except LocationChoiceNeeded as e:
        return location_choice_response(e)
    existing = {}
    for week_start in _paste_weeks(paste):
        roster = db.get_roster(f"imported-{body.venue_id}-{week_start.isoformat()}")
        existing[week_start] = (roster,
                                _published_in_rosteriq(db, body.venue_id, week_start, roster))
    leave = _paste_leave(db, body.venue_id)
    now = venue_now(body.venue_id, db)
    now = now.replace(tzinfo=None) if getattr(now, "tzinfo", None) else now
    weeks = await asyncio.to_thread(_reconcile, paste, existing, emp_map, state, body.venue_id,
                                    leave, now)

    skipped, week_costs, written, done, busy = paste["skipped"], [], [], [], False
    for week in weeks:
        if week["roster"] is not None:
            when = f"{week['week_start']:%d/%m/%Y}"
            try:
                week = _save_week(db, paste, week, emp_map, state, body.venue_id, leave, now)
            except edit.RosterBusy:
                busy = True
                _skip_week(paste, week["week_start"], f"someone else was saving the week of {when} "
                           "— paste again in a moment")
                continue
            except Exception as e:
                _skip_week(paste, week["week_start"], f"the week of {when} couldn't be saved: {e}")
                continue
        done.append(week)
        roster = week["roster"]
        if roster is not None:
            written.append(roster.week_start)
            week_costs.append({"week_start": roster.week_start.isoformat(), "roster_id": roster.id,
                               "shifts": len(roster.shifts), "total_cost": str(roster.total_cost)})
    if busy and not written:
        raise HTTPException(status_code=409,
                            detail="Someone else is saving this roster right now — try again.")
    supersede_for_import(db, body.venue_id, written)
    cover_notes = []
    for week in done:
        skipped.extend(week["skipped"])
        paste["report"].extend(week["report"])
        paste["duplicates"] += week["duplicates"]
        if week["roster"] is None:
            continue
        for before, after in week["filled"]:
            cover_notes += [f"{shift_label(before)}: {n['message']}" for n in
                            edit.settle_covers(db, body.venue_id, before, after, user.user_id)]
        for shift, employee_id in week["left_open"]:
            db.save_shift_cover(_new_cover(body.venue_id, shift, employee_id, "leave"))
    imported = sum(w["imported"] for w in done)
    replaced = sum(w["replaced"] for w in done)
    removed = [r for w in done for r in w["removed"]]
    report = sorted(paste["report"], key=lambda r: r["line"])
    flagged = sorted(paste["flagged"] + [f for w in done for f in w["flagged"]],
                     key=lambda f: f["line"])
    published = [w["week_start"].isoformat() for w in done if w["published"]]

    logger.info(f"Roster import at {body.venue_id}: {imported} shifts across "
                f"{len(written)} week(s), {replaced} replaced, {len(removed)} removed, "
                f"{len(skipped)} skipped ({paste['duplicates']} already imported)")
    return {
        "status": "imported",
        "shifts_imported": imported,
        "weeks": len(written),
        "skipped_count": len(skipped),
        "skipped": skipped[:50],
        "duplicates_skipped": paste["duplicates"],
        "shifts_replaced": replaced,
        "shifts_removed": len(removed),
        "removed": removed,
        "rows": report,
        "flagged": flagged,
        "flags_summary": dict(Counter(f["flag"] for f in flagged)),
        "unmapped_columns": paste["unmapped"],
        "column_map": paste["column_map"],
        "location": paste["location"],
        "date_order": paste["date_order"],
        "week_costs": week_costs,
        "published_weeks": published,
        "cover_notes": cover_notes,
    }
