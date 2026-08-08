"""
Staff import from any program's export — the no-live-connection onboarding
path. A venue exports its staff list from Deputy / Tanda / a spreadsheet
(every one of them has an Export to CSV button) and pastes or uploads it here;
we ingest it without ever touching a live API, so nothing can fail mid-demo.

The parser is deliberately forgiving of real export files: it sniffs the
delimiter, detects and maps a header row by common column aliases, and where
there's no header it infers columns by content (an @ is an email, a number is
a pay rate). Every row is reported back as created or skipped-with-reason —
nothing is silently dropped.

Route:
    POST /api/setup/import-staff  { venue_id, content }
"""

import csv
import io
import logging
import re
import uuid
from datetime import datetime
from decimal import Decimal, InvalidOperation

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from rosteriq.database import get_db
from rosteriq.middleware.tenant import enforce_venue_access
from rosteriq.models import Employee, EmploymentType, AwardLevel, State

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/setup", tags=["setup"])


class StaffImportBody(BaseModel):
    venue_id: str
    content: str = Field(..., min_length=1, max_length=500_000,
                         description="Pasted CSV/TSV export of a staff list")


# Header aliases the common exports use (Deputy, Tanda, spreadsheets)
_NAME_KEYS = {"name", "full name", "fullname", "employee", "employee name",
              "staff", "staff name", "display name", "first name"}
_EMAIL_KEYS = {"email", "e-mail", "email address", "work email"}
_RATE_KEYS = {"rate", "pay rate", "hourly rate", "hourly", "base rate",
              "hourly base rate", "wage", "pay"}
_ROLE_KEYS = {"role", "position", "job", "job title", "title", "skill",
              "skills", "team", "department", "area"}
_TYPE_KEYS = {"employment type", "type", "employment", "contract"}
_LAST_KEYS = {"last name", "surname", "lastname"}


def _clean_rate(v: str):
    """'$31.50' / '31.5' / '' -> Decimal or None."""
    s = re.sub(r"[^\d.]", "", str(v or ""))
    if not s:
        return None
    try:
        d = Decimal(s)
        return d if d > 0 else None
    except (InvalidOperation, ValueError):
        return None


def _emp_type(v: str) -> EmploymentType:
    s = str(v or "").strip().lower()
    if "full" in s:
        return EmploymentType.full_time
    if "part" in s:
        return EmploymentType.part_time
    return EmploymentType.casual  # sensible default for hospitality


@router.post("/import-staff")
async def import_staff(body: StaffImportBody) -> dict:
    enforce_venue_access(body.venue_id)
    db = get_db()
    venue = db.get_venue(body.venue_id)
    if not venue:
        raise HTTPException(status_code=404, detail="Venue not found")
    venue_state = getattr(venue, "state", State.nsw)

    text = body.content.replace("\r\n", "\n").replace("\r", "\n").strip()
    if not text:
        raise HTTPException(status_code=422, detail="Nothing to import")

    # Sniff the delimiter (tab, comma, semicolon, or pipe)
    sample = text.split("\n", 1)[0]
    delim = "\t" if "\t" in sample else (
        ";" if sample.count(";") > sample.count(",") else (
            "|" if "|" in sample and "," not in sample else ","))
    rows = [r for r in csv.reader(io.StringIO(text), delimiter=delim) if any(c.strip() for c in r)]
    if not rows:
        raise HTTPException(status_code=422, detail="No rows found")

    # Header detection + column mapping
    def _norm(h):
        return str(h).strip().lower()
    header = rows[0]
    header_norm = [_norm(h) for h in header]
    has_header = any(h in (_NAME_KEYS | _EMAIL_KEYS | _RATE_KEYS | _ROLE_KEYS)
                     for h in header_norm)
    col = {"name": None, "email": None, "rate": None, "role": None,
           "type": None, "last": None}
    if has_header:
        for idx, h in enumerate(header_norm):
            if col["name"] is None and h in _NAME_KEYS:
                col["name"] = idx
            elif col["email"] is None and h in _EMAIL_KEYS:
                col["email"] = idx
            elif col["rate"] is None and h in _RATE_KEYS:
                col["rate"] = idx
            elif col["role"] is None and h in _ROLE_KEYS:
                col["role"] = idx
            elif col["type"] is None and h in _TYPE_KEYS:
                col["type"] = idx
            elif col["last"] is None and h in _LAST_KEYS:
                col["last"] = idx
        data_rows = rows[1:]
    else:
        data_rows = rows  # no header — infer per row below

    # Existing staff (dedupe by email or name, case-insensitive)
    existing = db.get_employees(body.venue_id) or []
    seen_emails = {(getattr(e, "email", "") or "").strip().lower()
                   for e in existing if getattr(e, "email", None)}
    seen_names = {getattr(e, "name", "").strip().lower() for e in existing}

    created, skipped = [], []
    now = datetime.utcnow()
    for i, row in enumerate(data_rows, start=(2 if has_header else 1)):
        cells = [c.strip() for c in row]
        # Resolve fields: use mapped columns, else infer by content
        name = cells[col["name"]] if col["name"] is not None and col["name"] < len(cells) else ""
        if col["last"] is not None and col["last"] < len(cells) and cells[col["last"]]:
            name = f"{name} {cells[col['last']]}".strip()
        email = cells[col["email"]] if col["email"] is not None and col["email"] < len(cells) else ""
        rate_cell = cells[col["rate"]] if col["rate"] is not None and col["rate"] < len(cells) else ""
        role = cells[col["role"]] if col["role"] is not None and col["role"] < len(cells) else ""
        type_cell = cells[col["type"]] if col["type"] is not None and col["type"] < len(cells) else ""

        if col["name"] is None:  # no header — infer from the row
            non_email = [c for c in cells if "@" not in c]
            name = name or (non_email[0] if non_email else (cells[0] if cells else ""))
            email = email or next((c for c in cells if "@" in c), "")
            rate_cell = rate_cell or next((c for c in cells[1:] if _clean_rate(c)), "")

        name = name.strip()
        if not name or name.lower() in {"name", "employee", "staff"}:
            skipped.append({"row": i, "reason": "no name"})
            continue
        email = email.strip()
        if email and email.lower() in seen_emails:
            skipped.append({"row": i, "name": name, "reason": "email already exists"})
            continue
        if not email and name.lower() in seen_names:
            skipped.append({"row": i, "name": name, "reason": "name already exists"})
            continue

        rate = _clean_rate(rate_cell) or Decimal("31.50")  # award-review default
        try:
            emp = Employee(
                id=f"emp-{uuid.uuid4().hex[:10]}",
                venue_id=body.venue_id,
                name=name,
                employment_type=_emp_type(type_cell),
                award_level=AwardLevel.level_2,
                state=venue_state,
                hourly_base_rate=rate,
                email=email or None,
                skills=[role] if role else [],
                created_at=now, updated_at=now,
            )
            db.save_employee(emp)
            created.append({"name": name, "email": email or None, "rate": str(rate)})
            if email:
                seen_emails.add(email.lower())
            seen_names.add(name.lower())
        except Exception as e:
            skipped.append({"row": i, "name": name, "reason": f"invalid: {e}"})

    logger.info(f"Staff import at {body.venue_id}: {len(created)} created, "
                f"{len(skipped)} skipped")
    return {
        "status": "imported",
        "created_count": len(created),
        "skipped_count": len(skipped),
        "created": created,
        "skipped": skipped,
        "note": "Pay rates default to $31.50 where the file had none — review "
                "them with your payroll advisor before running pays.",
    }
