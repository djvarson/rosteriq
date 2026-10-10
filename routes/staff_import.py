"""
Staff import from any program's export — the no-live-connection onboarding
path. A venue exports its staff list from Deputy / Tanda / a spreadsheet
(every one of them has an Export to CSV button) and pastes or uploads it here;
we ingest it without ever touching a live API, so nothing can fail mid-demo.

The parser is deliberately forgiving of real export files: it sniffs the
delimiter, detects and maps a header row by common column aliases, and where
there's no header it infers columns by content (an @ is an email, a number is
a pay rate). Every row is reported back as created, updated or
skipped-with-reason, with flags for anything we had to assume — nothing is
silently dropped or silently guessed.

Pay figures are what the venue recorded. A salary or weekly figure is turned
into an hourly PLANNING equivalent (annual / 52 / 38, weekly / 38) and flagged
for the payroll advisor; RosterIQ never pays anyone.

Route:
    POST /api/setup/import-staff
        { venue_id, content, update_existing?: bool, location?: str }
"""

import asyncio
import logging
import re
import uuid
from collections import Counter
from datetime import date, datetime, timedelta
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation
from typing import Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from rosteriq.database import get_db
from rosteriq.middleware.tenant import enforce_venue_manager
from rosteriq.models import Employee, EmploymentType, AwardLevel, State
from rosteriq.services.import_parsing import (
    DAY_FIRST, LOCATION_KEYS, MONTH_FIRST, LocationChoiceNeeded, MixedDateOrder,
    PasteUnreadable, build_name, cell_date, clean_header, date_order, header_matches,
    location_choice_response, location_matches, map_columns, norm_header, norm_name,
    read_rows, resolve_location, unclosed_quote,
)
from rosteriq.services.sms import format_au_number

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/setup", tags=["setup"])


class StaffImportBody(BaseModel):
    venue_id: str
    content: str = Field(..., min_length=1, max_length=500_000,
                         description="Pasted CSV/TSV export of a staff list")
    update_existing: bool = Field(
        False, description="Refresh name/rate/type/level/phone/skills of staff "
                           "already at this venue (matched by email)")
    location: Optional[str] = Field(
        None, max_length=200,
        description="When the export covers several locations, the value to keep")


DEFAULT_RATE = Decimal("31.50")      # award-review placeholder, flagged wherever used
_HOURLY_CEILING = Decimal("150")     # a bare figure above this is not an hourly rate
# An hourly figure outside this band is flagged for review — a sanity range,
# not an award value.
_PLAUSIBLE_HOURLY = (Decimal("5"), Decimal("100"))
_RATE_CELL_MAX = 100                 # a longer cell is not a pay figure
_WEEKS_PER_YEAR = Decimal("52")
_HOURS_PER_WEEK = Decimal("38")
_CENT = Decimal("0.01")

# Header aliases the common exports use (Deputy, Tanda, spreadsheets)
_NAME_KEYS = {"name", "full name", "fullname", "employee", "employee name",
              "staff", "staff name", "display name"}
_FIRST_KEYS = {"first name", "firstname", "given name", "first"}
_LAST_KEYS = {"last name", "surname", "lastname", "family name", "last"}
_EMAIL_KEYS = {"email", "e-mail", "email address", "e-mail address", "work email"}
_RATE_KEYS = {"rate", "pay rate", "hourly rate", "hourly", "base rate",
              "hourly base rate", "wage", "pay", "base pay rate", "base hourly rate",
              "ordinary rate", "ordinary hourly rate", "rate (hourly)",
              "pay rate (hourly)", "hourly pay rate", "base pay"}
_SALARY_KEYS = {"salary", "annual salary", "annual rate", "annual pay", "yearly salary",
                "weekly rate", "weekly wage", "weekly pay", "weekly salary"}
_RATE_UNIT_KEYS = {"pay unit", "rate unit", "pay rate unit", "rate type", "pay rate type"}
_POSITION_KEYS = {"role", "position", "job", "job title", "title", "team", "department"}
_SKILL_KEYS = {"skill", "skills", "area", "areas", "training", "training areas",
               "trained areas", "areas trained", "locations trained", "trained in"}
_TYPE_KEYS = {"employment type", "type", "employment", "contract", "employment basis",
              "employment status", "contract type", "employee type", "basis"}
_PHONE_KEYS = {"mobile", "phone", "mobile phone", "phone number", "contact number",
               "cell", "cell phone", "mobile number", "mobile no", "phone no",
               "contact phone"}
_LEVEL_KEYS = {"award level", "classification", "level", "pay level",
               "award classification", "classification level"}
_VISA_KEYS = {"visa", "visa status", "visa type", "work rights", "working rights"}
_VISA_EXPIRY_KEYS = {"visa expiry", "visa expiry date", "visa end", "visa end date"}

_FIELDS = [
    ("name", _NAME_KEYS), ("first_name", _FIRST_KEYS), ("last_name", _LAST_KEYS),
    ("email", _EMAIL_KEYS), ("rate", _RATE_KEYS), ("salary", _SALARY_KEYS),
    ("rate_unit", _RATE_UNIT_KEYS), ("type", _TYPE_KEYS), ("phone", _PHONE_KEYS),
    ("award_level", _LEVEL_KEYS), ("location", LOCATION_KEYS),
    ("visa", _VISA_KEYS), ("visa_expiry", _VISA_EXPIRY_KEYS),
    ("skills", _SKILL_KEYS), ("position", _POSITION_KEYS),
]
_MULTI = ("skills", "position")
_HEADER_HINT_KEYS = (_NAME_KEYS | _FIRST_KEYS | _LAST_KEYS | _EMAIL_KEYS | _RATE_KEYS
                     | _POSITION_KEYS | _SKILL_KEYS | _PHONE_KEYS)


def _shown(raw: str) -> str:
    raw = str(raw or "").strip()
    return raw if len(raw) <= 40 else raw[:40] + "…"


_EXCEL_SERIAL_RE = re.compile(r"\d{5}(?:\.0+)?")
_EXCEL_EPOCH = date(1899, 12, 30)


def _visa_expiry(v: str, order: Optional[str]):
    """-> (date | None, note | None). Reads the roster importer's date formats
    (a time after the date ignored) and Excel day numbers, with the column's
    day/month order (None when the column mixes both). Anything else stays
    blank and is flagged — a wrong visa expiry is worse than a blank one."""
    raw = str(v or "").strip()
    if not raw:
        return None, None
    if _EXCEL_SERIAL_RE.fullmatch(raw) and 20000 <= int(float(raw)) <= 80000:
        return _EXCEL_EPOCH + timedelta(days=int(float(raw))), None
    d = cell_date(raw, order or DAY_FIRST)
    if order is None and d != cell_date(raw, MONTH_FIRST):
        return None, (f"visa expiry '{_shown(raw)}' couldn't be read — this column has both "
                      "day/month and month/day dates; left blank, record it by hand")
    if d is None:
        return None, f"visa expiry '{_shown(raw)}' couldn't be read — left blank, record it by hand"
    return d, None


# --- Pay rates --------------------------------------------------------------

# '1.250,00' (dotted thousands, decimal comma) | '1,250.00' | '31,58' | '31.58',
# then an optional 'k' for thousands ('$78k').
_AMOUNT = (r"(\d{1,3}(?:\.\d{3})+,\d{1,2}(?!\d)|\d{1,3}(?:,\d{3})+(?:\.\d+)?"
           r"|\d+,\d{1,2}(?!\d)|\d+(?:\.\d+)?)(\s*[kK](?![A-Za-z]))?")
_DOLLAR_RE = re.compile(r"\$\s*" + _AMOUNT)
_NUMBER_RE = re.compile(_AMOUNT)
_ANNUAL_RE = re.compile(r"/\s*(?:yr|year|annum|a)\b|\bp\.?\s?a\b\.?|per\s+(?:annum|year)"
                        r"|\bannual|\bsalary|\byearly|\ba\s+year")
_WEEKLY_RE = re.compile(r"/\s*(?:wk|week|w)\b|\bp\.?\s?w\b\.?|per\s+week|\bweekly|\ba\s+week")
_HOURLY_RE = re.compile(r"/\s*(?:hr|hour|h)\b|\bp\.?\s?h\b\.?|per\s+hour|\bhourly|\ban\s+hour")


def _amount(m) -> Optional[Decimal]:
    token = m.group(1)
    if re.fullmatch(r"\d{1,3}(?:\.\d{3})+,\d{1,2}", token):      # '1.250,00'
        token = token.replace(".", "").replace(",", ".")
    elif re.fullmatch(r"\d+,\d{1,2}", token):                     # '31,58' — a decimal comma
        token = token.replace(",", ".")
    try:
        value = Decimal(token.replace(",", ""))
    except (InvalidOperation, ValueError):
        return None
    return value * 1000 if m.group(2) else value


_LEVEL_PREFIX_RE = re.compile(r"(?:\blevel|\blvl|\bgrade|\bl|\bg)\s*[-.:#]?\s*$", re.IGNORECASE)
_LEVEL_PREFIX_WINDOW = 16


def _first_amount(text: str) -> Optional[Decimal]:
    """The first money figure in a cell: a $-prefixed token wins ('Level 2 -
    $28.10' is 28.10, '$28.10 - $31.58' is 28.10); without one, the first
    figure with cents, else the first figure — never a classification number
    ('L2 28' is 28)."""
    s = str(text or "")
    if len(s) > _RATE_CELL_MAX:
        return None
    m = _DOLLAR_RE.search(s)
    if m:
        return _amount(m)
    first = None
    for m in _NUMBER_RE.finditer(s):
        if _LEVEL_PREFIX_RE.search(s, max(0, m.start() - _LEVEL_PREFIX_WINDOW), m.start()):
            continue
        if "." in m.group(1) or re.fullmatch(r"\d+,\d{1,2}", m.group(1)):
            return _amount(m)
        first = first or m
    return _amount(first) if first else None


def _unit_of(text: str) -> Optional[str]:
    s = str(text or "").lower()
    if _ANNUAL_RE.search(s):
        return "year"
    if _WEEKLY_RE.search(s):
        return "week"
    if _HOURLY_RE.search(s):
        return "hour"
    return None


def _money(d: Decimal) -> str:
    return f"${d:,.2f}"


def _parse_rate(cell: str, header_unit: Optional[str], unit_hint: str = ""):
    """-> (rate | None, flag | None, note | None).

    None rate means 'nothing usable' and the caller applies the default with
    the returned flag. A rate flagged rate_unclear is the figure as read,
    kept for review. An explicit unit in the cell (or a unit column) wins
    over the header's; a header unit only applies to figures too big to be
    hourly, so a mixed 'Salary' column of '$78,000' and '31.50' reads both,
    the small one flagged."""
    raw = str(cell or "").strip()
    if not raw:
        return None, "rate_defaulted", None
    if len(raw) > _RATE_CELL_MAX:
        return None, "rate_unclear", (f"pay rate cell is {len(raw):,} characters long — "
                                      "not a figure we can read")
    value = _first_amount(raw)
    if value is None or value <= 0:
        return None, "rate_unclear", f"pay rate '{raw}' couldn't be read"
    unit = _unit_of(raw) or _unit_of(unit_hint)
    misplaced = None
    if unit is None and header_unit:
        if value > _HOURLY_CEILING:
            unit = header_unit
        else:
            misplaced = header_unit
    if unit == "year":
        hourly = (value / _WEEKS_PER_YEAR / _HOURS_PER_WEEK).quantize(_CENT, rounding=ROUND_HALF_UP)
        how = "a year ÷ 52 weeks ÷ 38 hours"
    elif unit == "week":
        hourly = (value / _HOURS_PER_WEEK).quantize(_CENT, rounding=ROUND_HALF_UP)
        how = "a week ÷ 38 hours"
    else:
        if value > _HOURLY_CEILING:
            return None, "rate_unclear", (
                f"pay rate '{raw}' is over ${_HOURLY_CEILING}/h with no unit — it looks "
                f"like a salary or weekly figure")
        hourly = value.quantize(_CENT, rounding=ROUND_HALF_UP)
        if misplaced:
            return hourly, "rate_unclear", (
                f"'{raw}' is in a {misplaced}ly pay column but is too small to be a "
                f"{misplaced}'s pay — read as {_money(hourly)}/h; check it")
        return _plausible(raw, hourly, None, None)
    if hourly < Decimal("1"):
        return None, "rate_unclear", f"pay rate '{raw}' couldn't be read as {unit}ly pay"
    return _plausible(raw, hourly, "rate_converted", (
        f"'{raw}' read as {_money(value)} {how} = {_money(hourly)}/h — a planning "
        f"figure for labour costs only; confirm with your payroll advisor"))


def _plausible(raw: str, hourly: Decimal, flag, note):
    low, high = _PLAUSIBLE_HOURLY
    if low <= hourly <= high:
        return hourly, flag, note
    said = note or f"'{raw}' read as {_money(hourly)}/h"
    return hourly, "rate_unclear", (f"{said} — outside the usual {_money(low)} to "
                                    f"{_money(high)}/h; check it")


def _header_unit(header: str) -> Optional[str]:
    h = norm_header(header)
    if re.search(r"weekly|per week", h):
        return "week"
    if re.search(r"annual|salary|per annum|yearly", h):
        return "year"
    return None


# --- Other fields -------------------------------------------------------------

def _emp_type(v: str) -> Optional[EmploymentType]:
    s = re.sub(r"[^a-z]", "", str(v or "").lower())
    if not s:
        return None
    if "full" in s or s in {"ft", "fulltime"}:
        return EmploymentType.full_time
    if "part" in s or s in {"pt"}:
        return EmploymentType.part_time
    if "casual" in s or s in {"cas"}:
        return EmploymentType.casual
    return None


_ROMAN = {"i": 1, "ii": 2, "iii": 3, "iv": 4, "v": 5, "vi": 6}
_LEVEL_RE = re.compile(r"\b(?:level|lvl|grade|l|g)\s*[-.:#]?\s*([1-6]|iii|ii|iv|vi|v|i)\b")


def _award_level(v: str):
    """-> (AwardLevel | None, flag | None). Reads what the venue recorded;
    anything that needs interpreting is flagged for the advisor."""
    s = str(v or "").strip().lower()
    if not s:
        return None, "level_not_recorded"
    if len(s) > 60:
        return None, "level_unclear"
    if "intro" in s:
        return AwardLevel.level_1, "level_interpreted"
    m = _LEVEL_RE.search(s) or re.fullmatch(r"([1-6])", s)
    if not m:
        return None, "level_unclear"
    g = m.group(1)
    n = int(g) if g.isdigit() else _ROMAN[g]
    return AwardLevel(f"level_{n}"), None


_SKILL_SPLIT = re.compile(r"[,;|/\n]+")


def _skills(cells) -> list:
    out = []
    for cell in cells:
        for part in _SKILL_SPLIT.split(cell or ""):
            s = re.sub(r"\s+", " ", part).strip().lower()[:60]
            if s and s not in out:
                out.append(s)
    return out


def _phone(v: str):
    """-> (display number | None, flag | None). AU numbers are stored the way
    staff read them ('0412 345 678'); a foreign number keeps its E.164 form."""
    raw = str(v or "").strip()
    if not raw:
        return None, None
    if re.fullmatch(r"\d(?:\.\d+)?e\+?\d+", raw.lower()):
        return None, "phone_invalid"          # Excel scientific notation lost digits
    e164 = format_au_number(raw)
    if not e164:
        return None, "phone_invalid"
    if e164.startswith("+61") and len(e164) == 12:
        rest = e164[3:]
        if rest[0] in "45":
            return f"0{rest[:3]} {rest[3:6]} {rest[6:]}", None
        return f"0{rest[0]} {rest[1:5]} {rest[5:]}", None
    return e164, None


_EMAIL_RE = re.compile(r"[^@\s,;<>]+@[^@\s,;<>]+\.[a-z]{2,}")


def _email(v: str) -> Optional[str]:
    s = str(v or "").strip().lower()
    return s if _EMAIL_RE.fullmatch(s) else None


_NOTES = {
    "rate_defaulted": f"no pay rate in the file — set at {_money(DEFAULT_RATE)}/h; enter the rate they're paid",
    "type_defaulted": "no employment type recorded — set as casual",
    "level_not_recorded": "no award level in the file — shown as Level 2 until you record it (advisor to confirm)",
    "email_shared": "email is on more than one row — imported without it; their join code will add their own",
}


def _read_staff_paste(content: str, venue_name: str, location: Optional[str]) -> dict:
    """Every row of the paste read: name, email, pay, type, level, phone,
    areas and visa, plus the skips the file alone decides. Pure CPU, run off
    the event loop; nothing here touches the database. Raises HTTPException
    (422) or LocationChoiceNeeded."""
    try:
        rows = read_rows(content)
    except PasteUnreadable as e:
        raise HTTPException(status_code=422, detail=str(e))
    if not rows:
        raise HTTPException(status_code=422, detail="No rows found")

    # Header detection + column mapping
    header = rows[0][1]
    has_header = any(header_matches(norm_header(h), _HEADER_HINT_KEYS) for h in header)
    if has_header:
        bad_quote = unclosed_quote(*rows[0], len(header))
        if bad_quote:
            raise HTTPException(status_code=422, detail=f"Couldn't read the header row: {bad_quote}")
        col, column_map, unmapped = map_columns(header, _FIELDS, multi=_MULTI)
        data_rows = rows[1:]
    else:
        col = {f: ([] if f in _MULTI else None) for f, _ in _FIELDS}
        column_map, unmapped = {}, []
        data_rows = rows  # no header — infer per row below
    rate_col = col["rate"] if col["rate"] is not None else col["salary"]
    rate_unit_h = _header_unit(header[col["rate"]]) if col["rate"] is not None else None
    salary_unit_h = _header_unit(header[col["salary"]]) if col["salary"] is not None else None
    no_name_cols = col["name"] is None and col["first_name"] is None and col["last_name"] is None

    def get(cells, field):
        j = col[field]
        return cells[j] if j is not None and j < len(cells) else ""

    def get_all(cells, field):
        return [cells[j] for j in col[field] if j < len(cells)]

    def name_and_email(cells):
        name = build_name(get(cells, "name"), get(cells, "first_name"), get(cells, "last_name"))
        email_cell = get(cells, "email")
        if no_name_cols:
            non_email = [c for c in cells if "@" not in c]
            name = name or (non_email[0] if non_email else (cells[0] if cells else ""))
            email_cell = email_cell or next((c for c in cells if "@" in c), "")
        return re.sub(r"\s+", " ", name).strip(), email_cell

    data = [(line, [c.strip() for c in row]) for line, row in data_rows]

    # Several locations in one export: keep only this venue's rows
    location_info, kept_location = None, None
    if col["location"] is not None:
        active, kept_location, _ = resolve_location(
            [get(cells, "location") for _, cells in data], venue_name, location)
        if active:
            location_info = {"column": clean_header(header[col["location"]]),
                             "kept": kept_location, "skipped_other": 0}

    def ours(cells):
        return not kept_location or location_matches(get(cells, "location"), kept_location)

    # An email on rows for different people belongs to nobody in particular.
    # The same name and email on several rows (one row per area) is one
    # person, with the areas of every row.
    names_by_email, areas_by_email = {}, {}
    for line, cells in data:
        if unclosed_quote(line, cells, len(header)) or not ours(cells):
            continue
        name, email_cell = name_and_email(cells)
        email = _email(email_cell)
        if not (name and email):
            continue
        names_by_email.setdefault(email, set()).add(norm_name(name))
        trained, positions = areas_by_email.setdefault(email, ([], []))
        trained += [s for s in _skills(get_all(cells, "skills")) if s not in trained]
        positions += [s for s in _skills(get_all(cells, "position")) if s not in positions]

    visa_order = DAY_FIRST
    if col["visa_expiry"] is not None:
        try:
            visa_order = date_order((line, get(cells, "visa_expiry")) for line, cells in data)
        except MixedDateOrder:
            visa_order = None

    out, first_line = [], {}
    for line, cells in data:
        bad_quote = unclosed_quote(line, cells, len(header))
        if bad_quote:
            out.append({"line": line, "name": name_and_email(cells)[0], "email": None,
                        "skip": bad_quote})
            continue
        if not ours(cells):
            out.append({"line": line, "email": _email(get(cells, "email")),
                        "name": build_name(get(cells, "name"), get(cells, "first_name"),
                                           get(cells, "last_name")),
                        "skip": f"other location: {get(cells, 'location')}"})
            location_info["skipped_other"] += 1
            continue

        name, email_cell = name_and_email(cells)
        if get(cells, "rate"):
            rate_cell, cell_unit = get(cells, "rate"), rate_unit_h
        else:
            rate_cell, cell_unit = get(cells, "salary"), salary_unit_h
        if no_name_cols and rate_col is None:
            rate_cell = rate_cell or next((c for c in cells[1:] if _first_amount(c)), "")

        if not name or name.lower() in {"name", "employee", "staff"}:
            out.append({"line": line, "name": name, "email": None, "skip": "no name"})
            continue

        email = _email(email_cell)
        shared = bool(email) and len(names_by_email.get(email, ())) > 1
        if email and not shared and email in first_line:
            out.append({"line": line, "name": name, "email": email,
                        "skip": f"same person as line {first_line[email]} — one record, with "
                                "the areas from every row"})
            continue
        email_flags = []
        if email_cell.strip() and not email:
            email_flags.append(("email_invalid", f"'{_shown(email_cell)}' isn't an email — not "
                                                 "imported; their join code will add their own"))
        elif shared:
            email_flags.append(("email_shared", f"{email} is on rows for {len(names_by_email[email])} "
                                                "different people — imported without it; their "
                                                "join code will add their own"))
            email = None
        if email:
            first_line[email] = line

        if rate_col is None and has_header:
            rate = (None, "rate_defaulted", f"no pay-rate column we recognise — set at "
                                            f"{_money(DEFAULT_RATE)}/h; enter the rate they're paid")
        else:
            rate = _parse_rate(rate_cell, cell_unit, get(cells, "rate_unit"))
        if email in areas_by_email:
            trained, positions = areas_by_email[email]
        else:
            trained, positions = _skills(get_all(cells, "skills")), _skills(get_all(cells, "position"))
        out.append({
            "line": line, "name": name, "email": email, "email_flags": email_flags,
            "rate": rate, "rate_cell": rate_cell, "emp_type": _emp_type(get(cells, "type")),
            "level": _award_level(get(cells, "award_level")), "level_cell": get(cells, "award_level"),
            "phone": _phone(get(cells, "phone")), "phone_cell": get(cells, "phone"),
            "trained": trained, "skills": trained + [p for p in positions if p not in trained],
            "positions": positions, "visa": get(cells, "visa") or None,
            "visa_expiry": _visa_expiry(get(cells, "visa_expiry"), visa_order),
        })
    return {"rows": out, "column_map": column_map, "unmapped": unmapped, "location": location_info}


@router.post("/import-staff")
async def import_staff(body: StaffImportBody) -> dict:
    enforce_venue_manager(body.venue_id)
    db = get_db()
    venue = db.get_venue(body.venue_id)
    if not venue:
        raise HTTPException(status_code=404, detail="Venue not found")
    venue_state = getattr(venue, "state", State.nsw)

    if not body.content.strip():
        raise HTTPException(status_code=422, detail="Nothing to import")
    # Reading runs in a worker thread; every database call stays on the
    # event loop, which PostgresStore's one shared connection needs.
    try:
        paste = await asyncio.to_thread(_read_staff_paste, body.content,
                                        getattr(venue, "name", "") or "", body.location)
    except LocationChoiceNeeded as e:
        return location_choice_response(e)

    # Existing staff AT THIS VENUE (dedupe by email or name, case-insensitive)
    existing = db.get_employees(body.venue_id) or []
    by_email = {}
    for e in existing:
        em = (getattr(e, "email", "") or "").strip().lower()
        if em:
            by_email.setdefault(em, e)
    seen_names = {norm_name(getattr(e, "name", "")) for e in existing}

    created, skipped, updated, report = [], [], [], []
    now = datetime.utcnow()
    for row in paste["rows"]:
        line, name, email = row["line"], row["name"], row["email"]
        if row.get("skip"):
            skipped.append({"row": line, "name": name, "reason": row["skip"]})
            report.append({"line": line, "name": name, "email": email, "status": "skipped",
                           "reason": row["skip"], "flags": []})
            continue

        flags, notes = [], []

        def flag(code, note=None):
            flags.append(code)
            notes.append(note or _NOTES.get(code, code))

        for code, note in row["email_flags"]:
            flag(code, note)
        rate, rate_flag, rate_note = row["rate"]
        rate_cell, emp_type = row["rate_cell"], row["emp_type"]
        level, level_flag = row["level"]
        phone, phone_flag = row["phone"]
        trained, positions, skills = row["trained"], row["positions"], row["skills"]

        match = by_email.get(email) if email else None
        if match is not None:
            if not body.update_existing:
                reason = "already exists — tick update existing to refresh their details"
                skipped.append({"row": line, "name": name, "reason": reason})
                report.append({"line": line, "name": name, "email": email, "status": "skipped",
                               "reason": reason, "flags": []})
                continue
            changes = {}
            if name != match.name:
                changes["name"] = ("name", name)
            if (rate is not None and rate_flag != "rate_unclear"
                    and Decimal(str(match.hourly_base_rate)) != rate):
                changes["rate"] = ("hourly_base_rate", rate)
            if emp_type is not None and emp_type != match.employment_type:
                changes["type"] = ("employment_type", emp_type)
            if level is not None and level != match.award_level:
                changes["level"] = ("award_level", level)
            if phone and phone != (match.phone or None):
                changes["phone"] = ("phone", phone)
            # A training column states every area they're trained in; a
            # position or department alone only adds to what's recorded.
            current = list(match.skills or [])
            new_skills = skills if trained else current + [p for p in positions if p not in current]
            removed = [s for s in current if s not in new_skills]
            if new_skills != current:
                changes["skills"] = ("skills", new_skills)
            if rate_flag == "rate_converted" and "rate" in changes:
                flag("rate_converted", rate_note)
            elif rate_flag == "rate_unclear":
                flag("rate_unclear", f"{rate_note} — their current rate was kept")
            if level_flag in ("level_interpreted", "level_unclear"):
                flag(level_flag, _level_note(level_flag, row["level_cell"], updating=True))
            if phone_flag:
                flag(phone_flag, f"'{_shown(row['phone_cell'])}' isn't a phone number we can read "
                                 "— their number wasn't changed")
            if removed:
                flag("skills_removed", f"areas not in this file were removed: {', '.join(removed)}")
            if not changes:
                skipped.append({"row": line, "name": name, "reason": "already up to date"})
                report.append({"line": line, "name": name, "email": email, "status": "skipped",
                               "reason": "already up to date", "flags": flags, "notes": notes})
                continue
            try:
                upd = match.model_copy(deep=True)
                for attr, value in changes.values():
                    setattr(upd, attr, value)
                upd.updated_at = now
                db.save_employee(upd)
            except Exception as e:
                skipped.append({"row": line, "name": name, "reason": f"invalid: {e}"})
                report.append({"line": line, "name": name, "email": email, "status": "skipped",
                               "reason": f"invalid: {e}", "flags": flags, "notes": notes})
                continue
            by_email[email] = upd
            changed = list(changes)
            updated.append({"name": name, "email": email, "changed": changed})
            row_out = {"line": line, "name": name, "email": email, "status": "updated",
                       "changed": changed, "rate": str(upd.hourly_base_rate),
                       "flags": flags, "notes": notes}
            if "rate_converted" in flags:
                row_out["rate_original"] = rate_cell
            report.append(row_out)
            seen_names.add(norm_name(name))
            continue

        if not email and norm_name(name) in seen_names:
            skipped.append({"row": line, "name": name, "reason": "name already exists"})
            report.append({"line": line, "name": name, "email": None, "status": "skipped",
                           "reason": "name already exists", "flags": flags, "notes": notes})
            continue

        if rate is None:
            rate = DEFAULT_RATE
            if rate_flag == "rate_unclear":
                rate_note = f"{rate_note} — set at {_money(DEFAULT_RATE)}/h; enter their hourly rate"
        if rate_flag:
            flag(rate_flag, rate_note)
        if emp_type is None:
            flag("type_defaulted")
        if level_flag:
            flag(level_flag, _level_note(level_flag, row["level_cell"]))
        if phone_flag:
            flag(phone_flag, f"'{_shown(row['phone_cell'])}' isn't a phone number we can read — not imported")
        visa_expiry, visa_note = row["visa_expiry"]
        if visa_note:
            flag("visa_expiry_unreadable", visa_note)

        try:
            emp = Employee(
                id=f"emp-{uuid.uuid4().hex[:10]}",
                venue_id=body.venue_id,
                name=name,
                employment_type=emp_type or EmploymentType.casual,
                award_level=level or AwardLevel.level_2,
                state=venue_state,
                hourly_base_rate=rate,
                phone=phone,
                email=email,
                skills=skills,
                visa_status=row["visa"],
                visa_expiry=visa_expiry,
                created_at=now, updated_at=now,
            )
            db.save_employee(emp)
            created.append({"name": name, "email": email, "rate": str(rate)})
            row_out = {"line": line, "name": name, "email": email, "status": "created",
                       "rate": str(rate), "flags": flags, "notes": notes}
            if rate_flag == "rate_converted":
                row_out["rate_original"] = rate_cell
            report.append(row_out)
            if email:
                by_email[email] = emp
            seen_names.add(norm_name(name))
        except Exception as e:
            skipped.append({"row": line, "name": name, "reason": f"invalid: {e}"})
            report.append({"line": line, "name": name, "email": email, "status": "skipped",
                           "reason": f"invalid: {e}", "flags": flags, "notes": notes})

    if created or updated:
        # The employees list is cached for 2 minutes — a manager who imports
        # staff must see them immediately, not after the TTL.
        try:
            from rosteriq.middleware.cache import get_cache_manager
            await get_cache_manager().invalidate_all("employee_lists")
        except Exception:
            pass

    flags_summary = dict(Counter(
        f for r in report if r["status"] in ("created", "updated") for f in r["flags"]))
    logger.info(f"Staff import at {body.venue_id}: {len(created)} created, "
                f"{len(updated)} updated, {len(skipped)} skipped")
    return {
        "status": "imported",
        "created_count": len(created),
        "updated_count": len(updated),
        "skipped_count": len(skipped),
        "created": created,
        "updated": updated,
        "skipped": skipped,
        "rows": report,
        "flags_summary": flags_summary,
        "unmapped_columns": paste["unmapped"],
        "column_map": paste["column_map"],
        "location": paste["location"],
        "note": (f"Pay rates default to {_money(DEFAULT_RATE)} where the file had none or "
                 "couldn't be read, and salaries or weekly pay become an hourly planning "
                 "figure — check the flagged rows with your payroll advisor. RosterIQ "
                 "never pays anyone; payroll runs as normal."),
    }


def _level_note(code: str, value: str, updating: bool = False) -> str:
    value = _shown(value)
    if code == "level_interpreted":
        return f"'{value}' recorded as Level 1 (advisor to confirm)"
    if code == "level_unclear":
        tail = "their level wasn't changed" if updating else "shown as Level 2 until you record it"
        return f"'{value}' isn't an award level we can read — {tail} (advisor to confirm)"
    return _NOTES["level_not_recorded"]
