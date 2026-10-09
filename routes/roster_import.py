"""
Roster import from any program's export — bring a switching venue's existing
schedule across without a live connection. Paste a shift export from Deputy /
Tanda / a spreadsheet; we match each shift to a staff member (by email or
name), group the shifts into Mon-Sun weeks, cost them from the rates the
venue recorded, and save them as rosters.

Real exports are messy, so the parser is forgiving:
- delimiter sniffed (comma/tab/semicolon/pipe), Excel BOM stripped
- header mapped by column aliases; First + Last name columns combined
- AU-first date parsing (DD/MM/YYYY) plus ISO, 28-Sep-2026, a leading weekday
  word, and a date carried inside the Start/End cell ('28/09/2026 17:00')
- 12h ("5:30pm") and 24h ("17:30", "17:30:00") times
- meal breaks as minutes, H:MM, HH:MM:SS, decimal hours, '1h', '30 min'
- an export covering several locations keeps only this venue's rows
- every row reported back: imported or skipped-with-reason; a re-paste of
  shifts already imported is skipped, never doubled.

Route:
    POST /api/setup/import-roster  { venue_id, content, location?: str }
"""

import logging
import re
import uuid
from collections import Counter
from datetime import date, datetime, time, timedelta
from typing import Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from rosteriq.cost_calculator import calculate_roster_cost, calculate_shift_cost_breakdown
from rosteriq.database import get_db
from rosteriq.middleware.tenant import enforce_venue_manager
from rosteriq.models import Roster, Shift, ShiftStatus, State
from rosteriq.services.import_parsing import (
    LOCATION_KEYS, LocationChoiceNeeded, build_name, clean_header, header_matches,
    location_matches, map_columns, norm_header, norm_name, read_rows, resolve_location,
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

_WEEKDAY_RE = re.compile(r"^(?:mon|tue|wed|thu|fri|sat|sun)[a-z]*\.?,?\s+", re.IGNORECASE)
_DATE_FORMATS = ("%Y-%m-%d", "%d/%m/%Y", "%d/%m/%y", "%d-%m-%Y", "%d-%m-%y", "%d.%m.%Y",
                 "%d %b %Y", "%d %B %Y", "%d-%b-%Y", "%d-%b-%y", "%d %b %y", "%d-%B-%Y",
                 "%Y/%m/%d", "%m/%d/%Y")
_DATETIME_RE = re.compile(
    r"^\s*((?:[A-Za-z]{3,9}\.?,?\s+)?"
    r"(?:\d{4}-\d{1,2}-\d{1,2}|\d{1,2}[/.-]\d{1,2}[/.-]\d{2,4}|\d{1,2}[\s-][A-Za-z]{3,9}[\s-]\d{2,4}))"
    r"(?:[T\s,]+(.+?))?\s*$")


def _parse_date(v: str):
    """Try common export date formats, AU (DD/MM) preferred over US. A
    leading weekday word ('Mon', 'Monday,') is ignored."""
    s = _WEEKDAY_RE.sub("", str(v or "").strip())
    if not s:
        return None
    for fmt in _DATE_FORMATS:
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    return None


def _split_when(v: str):
    """'28/09/2026 17:00' -> (date, '17:00'); '17:00' -> (None, '17:00')."""
    s = str(v or "").strip()
    m = _DATETIME_RE.match(s)
    if m:
        d = _parse_date(m.group(1))
        if d:
            return d, (m.group(2) or "").strip()
    return None, s


def _cell_date(v: str):
    d, _ = _split_when(v)
    return d or _parse_date(v)


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

    rows = read_rows(body.content)
    if not rows:
        raise HTTPException(status_code=422, detail="No rows found")

    header = rows[0][1]
    has_header = any(header_matches(norm_header(h), _HEADER_HINT_KEYS) for h in header)
    if not has_header:
        raise HTTPException(status_code=422,
                            detail="Couldn't find a header row — the roster export needs columns "
                                   "like Name, Date, Start, End (and optionally Role, Break).")
    col, column_map, unmapped = map_columns(header, _FIELDS, multi=("date",))
    data = [(line, [c.strip() for c in raw]) for line, raw in rows[1:]]

    def cell(cells, field):
        j = col[field]
        return cells[j] if j is not None and j < len(cells) else ""

    missing = [f for f in ("start", "end") if col[f] is None]
    if not col["date"] and col["start"] is not None and not any(
            _split_when(cell(cells, "start"))[0] for _, cells in data):
        missing.insert(0, "date")
    if all(col[f] is None for f in ("name", "email", "first_name", "last_name")):
        missing.append("name or email")
    if missing:
        raise HTTPException(status_code=422,
                            detail=f"Export is missing required column(s): {', '.join(missing)}")
    break_unit = _break_unit(header[col["break"]]) if col["break"] is not None else None

    location_info, kept_location = None, None
    if col["location"] is not None:
        try:
            active, kept_location, _ = resolve_location(
                [cell(cells, "location") for _, cells in data],
                getattr(venue, "name", "") or "", body.location)
        except LocationChoiceNeeded as e:
            raise HTTPException(status_code=422, detail=str(e))
        if active:
            location_info = {"column": clean_header(header[col["location"]]),
                             "kept": kept_location, "skipped_other": 0}

    existing_rosters: dict = {}
    existing_keys: dict = {}

    def week_keys(week_start):
        if week_start not in existing_keys:
            r = db.get_roster(f"imported-{body.venue_id}-{week_start.isoformat()}")
            existing_rosters[week_start] = r
            existing_keys[week_start] = {
                (s.employee_id, s.date, s.start_time, s.end_time) for s in (r.shifts if r else [])}
        return existing_keys[week_start]

    # Parse rows -> shifts grouped by ISO week (Monday)
    weeks: dict = {}
    skipped, report, flagged = [], [], []
    imported = duplicates = 0
    seen = set()
    for line, cells in data:
        full = cell(cells, "name")
        first_last = build_name("", cell(cells, "first_name"), cell(cells, "last_name"))
        shown = build_name(full, cell(cells, "first_name"), cell(cells, "last_name"))
        email = cell(cells, "email").strip().lower()
        date_raw = next((cells[j] for j in col["date"] if j < len(cells) and cells[j]), "")
        d = next((dd for dd in (_cell_date(cells[j]) for j in col["date"] if j < len(cells)) if dd),
                 None)
        st_date, st_txt = _split_when(cell(cells, "start"))
        _, en_txt = _split_when(cell(cells, "end"))
        d = d or st_date

        def skip(reason):
            skipped.append({"row": line, "name": shown or email, "reason": reason})
            report.append({"line": line, "name": shown or email,
                           "date": d.isoformat() if d else date_raw,
                           "status": "skipped", "reason": reason})

        loc_cell = cell(cells, "location")
        if kept_location and not location_matches(loc_cell, kept_location):
            location_info["skipped_other"] += 1
            skip(f"other location: {loc_cell}")
            continue

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
        st = _parse_time(st_txt)
        en = _parse_time(en_txt)
        if not st or not en:
            skip("bad start/end time")
            continue

        week_start = d - timedelta(days=d.weekday())
        key = (emp_id, d, st, en)
        if key in seen or key in week_keys(week_start):
            duplicates += 1
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
        weeks.setdefault(week_start, []).append(Shift(
            id=f"impshift-{uuid.uuid4().hex[:10]}",
            employee_id=emp_id, date=d, start_time=st, end_time=en,
            break_minutes=brk, status=ShiftStatus.scheduled,
            role=(cell(cells, "role") or ""),
        ))
        imported += 1

    # Save one roster per week, MERGING into an existing imported roster for
    # that week so a re-paste adds rather than clobbers. Every shift is costed
    # from the rate the venue recorded, so imported weeks carry real labour.
    rosters_written = 0
    week_costs = []
    for week_start, shifts in sorted(weeks.items()):
        rid = f"imported-{body.venue_id}-{week_start.isoformat()}"
        existing = existing_rosters.get(week_start)
        all_shifts = (list(existing.shifts) if existing else []) + shifts
        try:
            for s in all_shifts:
                if s.employee_id in emp_map:
                    s.cost = calculate_shift_cost_breakdown(emp_map[s.employee_id], s, state).total_cost
            roster = Roster(
                id=rid, venue_id=body.venue_id,
                week_start=week_start, week_end=week_start + timedelta(days=6),
                shifts=all_shifts, total_cost=None,
                created_at=existing.created_at if existing else datetime.utcnow(),
            )
            costable = [s for s in all_shifts if s.employee_id in emp_map]
            roster.total_cost = calculate_roster_cost(
                roster.model_copy(update={"shifts": costable}), emp_map, state)
            db.save_roster(roster)
            rosters_written += 1
            week_costs.append({"week_start": week_start.isoformat(), "roster_id": rid,
                               "shifts": len(all_shifts), "total_cost": str(roster.total_cost)})
        except Exception as e:
            skipped.append({"week_of": week_start.isoformat(), "reason": f"roster save failed: {e}"})

    logger.info(f"Roster import at {body.venue_id}: {imported} shifts across "
                f"{rosters_written} week(s), {len(skipped)} skipped ({duplicates} already imported)")
    return {
        "status": "imported",
        "shifts_imported": imported,
        "weeks": rosters_written,
        "skipped_count": len(skipped),
        "skipped": skipped[:50],
        "duplicates_skipped": duplicates,
        "rows": report,
        "flagged": flagged,
        "flags_summary": dict(Counter(f["flag"] for f in flagged)),
        "unmapped_columns": unmapped,
        "column_map": column_map,
        "location": location_info,
        "week_costs": week_costs,
    }
