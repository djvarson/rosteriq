"""
Roster import from any program's export — bring a switching venue's existing
schedule across without a live connection. Paste a shift export from Deputy /
Tanda / a spreadsheet; we match each shift to a staff member (by email or
name), group the shifts into Mon-Sun weeks, and save them as rosters.

Real exports are messy, so the parser is forgiving:
- delimiter sniffed (comma/tab/semicolon/pipe)
- header mapped by column aliases; date/time columns detected by content
- AU-first date parsing (DD/MM/YYYY) plus ISO and other common formats
- 12h ("5:30pm") and 24h ("17:30") times
- every row reported back: imported or skipped-with-reason.

Route:
    POST /api/setup/import-roster  { venue_id, content }
"""

import csv
import io
import logging
import re
import uuid
from datetime import date, datetime, time, timedelta
from decimal import Decimal

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from rosteriq.database import get_db
from rosteriq.middleware.tenant import enforce_venue_manager
from rosteriq.models import Roster, Shift, ShiftStatus

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/setup", tags=["setup"])


class RosterImportBody(BaseModel):
    venue_id: str
    content: str = Field(..., min_length=1, max_length=1_000_000)


_NAME_KEYS = {"name", "employee", "employee name", "staff", "staff name", "full name"}
_EMAIL_KEYS = {"email", "e-mail", "email address"}
_DATE_KEYS = {"date", "shift date", "day", "work date"}
_START_KEYS = {"start", "start time", "from", "shift start", "starttime"}
_END_KEYS = {"end", "end time", "to", "finish", "shift end", "endtime"}
_ROLE_KEYS = {"role", "area", "position", "team", "department", "job"}
_BREAK_KEYS = {"break", "break minutes", "meal break", "unpaid break"}


def _parse_date(v: str):
    """Try common export date formats, AU (DD/MM) preferred over US."""
    s = str(v or "").strip()
    if not s:
        return None
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d/%m/%y", "%d-%m-%Y", "%d-%m-%y",
                "%d %b %Y", "%d %B %Y", "%a %d/%m/%Y", "%m/%d/%Y"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    return None


def _parse_time(v: str):
    """'17:30' / '5:30pm' / '9am' / '1730' -> time, else None."""
    s = str(v or "").strip().lower().replace(" ", "")
    if not s:
        return None
    m = re.match(r"^(\d{1,2}):?(\d{2})?\s*(am|pm)?$", s)
    if not m:
        return None
    hh = int(m.group(1))
    mm = int(m.group(2) or 0)
    ap = m.group(3)
    if ap == "pm" and hh < 12:
        hh += 12
    elif ap == "am" and hh == 12:
        hh = 0
    if not (0 <= hh <= 23 and 0 <= mm <= 59):
        return None
    return time(hh, mm)


@router.post("/import-roster")
async def import_roster(body: RosterImportBody) -> dict:
    enforce_venue_manager(body.venue_id)
    db = get_db()
    venue = db.get_venue(body.venue_id)
    if not venue:
        raise HTTPException(status_code=404, detail="Venue not found")

    employees = db.get_employees(body.venue_id) or []
    if not employees:
        raise HTTPException(status_code=422,
                            detail="Import your staff first — shifts are matched to staff by name/email.")
    by_email = {(getattr(e, "email", "") or "").strip().lower(): e.id
                for e in employees if getattr(e, "email", None)}
    by_name = {getattr(e, "name", "").strip().lower(): e.id for e in employees}

    text = body.content.replace("\r\n", "\n").replace("\r", "\n").strip()
    sample = text.split("\n", 1)[0]
    delim = "\t" if "\t" in sample else (
        ";" if sample.count(";") > sample.count(",") else (
            "|" if "|" in sample and "," not in sample else ","))
    rows = [r for r in csv.reader(io.StringIO(text), delimiter=delim) if any(c.strip() for c in r)]
    if not rows:
        raise HTTPException(status_code=422, detail="No rows found")

    def _norm(h):
        return str(h).strip().lower()
    header = [_norm(h) for h in rows[0]]
    has_header = any(h in (_NAME_KEYS | _DATE_KEYS | _START_KEYS | _END_KEYS) for h in header)
    if not has_header:
        raise HTTPException(status_code=422,
                            detail="Couldn't find a header row — the roster export needs columns "
                                   "like Name, Date, Start, End (and optionally Role, Break).")
    col = {"name": None, "email": None, "date": None, "start": None,
           "end": None, "role": None, "break": None}
    keymap = [("name", _NAME_KEYS), ("email", _EMAIL_KEYS), ("date", _DATE_KEYS),
              ("start", _START_KEYS), ("end", _END_KEYS), ("role", _ROLE_KEYS),
              ("break", _BREAK_KEYS)]
    for idx, h in enumerate(header):
        for field, keys in keymap:
            if col[field] is None and h in keys:
                col[field] = idx
                break
    missing = [f for f in ("date", "start", "end") if col[f] is None]
    if col["name"] is None and col["email"] is None:
        missing.append("name or email")
    if missing:
        raise HTTPException(status_code=422,
                            detail=f"Export is missing required column(s): {', '.join(missing)}")

    # Parse rows -> shifts grouped by ISO week (Monday)
    weeks: dict = {}
    skipped = []
    imported = 0
    for i, raw in enumerate(rows[1:], start=2):
        cells = [c.strip() for c in raw]

        def cell(field):
            j = col[field]
            return cells[j] if j is not None and j < len(cells) else ""

        emp_id = None
        email = cell("email").strip().lower()
        if email and email in by_email:
            emp_id = by_email[email]
        if not emp_id:
            nm = cell("name").strip().lower()
            emp_id = by_name.get(nm)
        if not emp_id:
            skipped.append({"row": i, "reason": "no matching staff member"})
            continue

        d = _parse_date(cell("date"))
        st = _parse_time(cell("start"))
        en = _parse_time(cell("end"))
        if not d:
            skipped.append({"row": i, "reason": f"bad date '{cell('date')}'"})
            continue
        if not st or not en:
            skipped.append({"row": i, "reason": "bad start/end time"})
            continue
        brk = 0
        try:
            brk = int(re.sub(r"[^\d]", "", cell("break")) or 0)
        except ValueError:
            brk = 0

        week_start = d - timedelta(days=d.weekday())
        weeks.setdefault(week_start, []).append(Shift(
            id=f"impshift-{uuid.uuid4().hex[:10]}",
            employee_id=emp_id, date=d, start_time=st, end_time=en,
            break_minutes=min(brk, 480), status=ShiftStatus.scheduled,
            role=(cell("role") or ""),
        ))
        imported += 1

    # Save one roster per week, MERGING into an existing imported roster for
    # that week so a re-paste adds rather than clobbers.
    rosters_written = 0
    for week_start, shifts in sorted(weeks.items()):
        rid = f"imported-{body.venue_id}-{week_start.isoformat()}"
        existing = db.get_roster(rid)
        all_shifts = (list(existing.shifts) if existing else []) + shifts
        try:
            db.save_roster(Roster(
                id=rid, venue_id=body.venue_id,
                week_start=week_start, week_end=week_start + timedelta(days=6),
                shifts=all_shifts, total_cost=None, created_at=datetime.utcnow(),
            ))
            rosters_written += 1
        except Exception as e:
            skipped.append({"week_of": week_start.isoformat(), "reason": f"roster save failed: {e}"})

    logger.info(f"Roster import at {body.venue_id}: {imported} shifts across "
                f"{rosters_written} week(s), {len(skipped)} skipped")
    return {
        "status": "imported",
        "shifts_imported": imported,
        "weeks": rosters_written,
        "skipped_count": len(skipped),
        "skipped": skipped[:50],
    }
