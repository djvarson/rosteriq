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
already published in RosterIQ.

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
from datetime import datetime, time, timedelta
from typing import Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from rosteriq.cost_calculator import calculate_roster_cost, calculate_shift_cost_breakdown
from rosteriq.database import get_db
from rosteriq.middleware.tenant import enforce_venue_manager
from rosteriq.models import Roster, Shift, ShiftStatus, State
from rosteriq.services.current_roster import (
    ARCHIVED, PUBLISHED, roster_state, supersede_for_import, week_monday,
)
from rosteriq.services.import_parsing import (
    LOCATION_KEYS, LocationChoiceNeeded, MixedDateOrder, PasteUnreadable, build_name, cell_date,
    clean_header, date_order, header_matches, location_choice_response, location_matches,
    map_columns, norm_header, norm_name, read_rows, resolve_location, split_when, unclosed_quote,
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


def _reconcile(paste: dict, existing: dict, emp_map: dict, state, venue_id: str) -> dict:
    """Each week's imported roster rebuilt from the paste and costed from the
    rates the venue recorded. Pure CPU, run off the event loop. existing:
    {monday: (imported roster | None, published in RosterIQ)}."""
    out = {"rosters": [], "imported": 0, "replaced": 0, "removed": []}
    pasted, held, covered = paste["pasted"], paste["held"], paste["covered"]
    for week_start in _paste_weeks(paste):
        roster, published = existing[week_start]
        week_items = sorted(((pair, line, name, sh) for pair, items in pasted.items()
                             if week_monday(pair[1]) == week_start for line, name, sh in items),
                            key=lambda x: x[1])
        if published:
            for (_, d), line, name, _ in week_items:
                _note_row(paste, line, name, d, "skipped",
                          f"the week of {week_start:%d/%m/%Y} is published in RosterIQ — an "
                          "import doesn't change a published week")
            continue
        old = list(roster.shifts) if roster else []
        by_key = {}
        for s in old:
            by_key.setdefault((s.employee_id, s.date, s.start_time, s.end_time), s)
        kept_ids, updates, added = set(), {}, []
        for (emp_id, d), line, name, sh in week_items:
            prev = by_key.get((emp_id, d, sh.start_time, sh.end_time))
            if prev is None or prev.id in kept_ids:
                added.append((line, name, sh))
                continue
            kept_ids.add(prev.id)
            if (prev.break_minutes, prev.role) != (sh.break_minutes, sh.role):
                updates[prev.id] = prev.model_copy(
                    update={"break_minutes": sh.break_minutes, "role": sh.role})
                out["replaced"] += 1
                _note_row(paste, line, name, d, "replaced",
                          f"meal break or area changed (was {prev.break_minutes} min, "
                          f"{prev.role or 'no area'})")
            else:
                paste["duplicates"] += 1
                _note_row(paste, line, name, d, "skipped", "already imported")
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
                _note_row(paste, line, name, sh.date, "replaced",
                          "replaced " + ", ".join(_hhmm(g) for g in moved))
        out["imported"] += len(added)
        for g in gone:
            if g.id not in replaced_ids:
                out["removed"].append({
                    "name": getattr(emp_map.get(g.employee_id), "name", g.employee_id),
                    "date": g.date.isoformat(), "start": f"{g.start_time:%H:%M}",
                    "end": f"{g.end_time:%H:%M}"})
        if not (added or updates or gone):
            continue
        gone_ids = {g.id for g in gone}
        shifts = [updates.get(s.id, s) for s in old if s.id not in gone_ids] + [sh for _, _, sh in added]
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
        out["rosters"].append(rebuilt)
    return out


@router.post("/import-roster")
async def import_roster(body: RosterImportBody) -> dict:
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
        existing[week_start] = (roster, roster is not None
                                and roster_state(db, roster.id) in (PUBLISHED, ARCHIVED))
    plan = await asyncio.to_thread(_reconcile, paste, existing, emp_map, state, body.venue_id)

    skipped, week_costs, written = paste["skipped"], [], []
    for roster in plan["rosters"]:
        try:
            db.save_roster(roster)
        except Exception as e:
            skipped.append({"week_of": roster.week_start.isoformat(), "reason": f"roster save failed: {e}"})
            continue
        written.append(roster.week_start)
        week_costs.append({"week_start": roster.week_start.isoformat(), "roster_id": roster.id,
                           "shifts": len(roster.shifts), "total_cost": str(roster.total_cost)})
    supersede_for_import(db, body.venue_id, written)
    report = sorted(paste["report"], key=lambda r: r["line"])
    flagged = paste["flagged"]

    logger.info(f"Roster import at {body.venue_id}: {plan['imported']} shifts across "
                f"{len(written)} week(s), {plan['replaced']} replaced, {len(plan['removed'])} removed, "
                f"{len(skipped)} skipped ({paste['duplicates']} already imported)")
    return {
        "status": "imported",
        "shifts_imported": plan["imported"],
        "weeks": len(written),
        "skipped_count": len(skipped),
        "skipped": skipped[:50],
        "duplicates_skipped": paste["duplicates"],
        "shifts_replaced": plan["replaced"],
        "shifts_removed": len(plan["removed"]),
        "removed": plan["removed"],
        "rows": report,
        "flagged": flagged,
        "flags_summary": dict(Counter(f["flag"] for f in flagged)),
        "unmapped_columns": paste["unmapped"],
        "column_map": paste["column_map"],
        "location": paste["location"],
        "date_order": paste["date_order"],
        "week_costs": week_costs,
    }
