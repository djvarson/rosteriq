"""
Managing the team after import: who is still working here, what a manager may
change on a staff record, and which people still need their join code.

* ``Employee.active`` — False once someone leaves. Inactive people are never
  rostered, offered cover or SOS, or shown on the kiosk board; their shifts,
  timesheets and pay history stay, so every store listing still returns them
  and anything that OFFERS work filters through ``active_only``.
* Hourly rates above ``MAX_HOURLY_RATE`` are refused: at that size the figure
  is almost always a weekly or annual salary typed into the hourly field, and
  storing it as $/h prices a roster in the millions.
* Rate changes made in RosterIQ are kept in ``Employee.rate_history`` with the
  date they take effect. Costing and payroll prep price every shift at the
  current rate; a backdated change is a note for the pay run to check.
"""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Iterable, Optional

MAX_HOURLY_RATE = Decimal("150")


def is_active(emp) -> bool:
    return getattr(emp, "active", True) is not False


def active_only(employees: Optional[Iterable]) -> list:
    return [e for e in (employees or []) if is_active(e)]


def _money(value) -> str:
    return f"${value:,.2f}"


def hourly_rate_problem(rate, pay_basis: Optional[str] = "hourly") -> Optional[str]:
    """Why this hourly rate can't be saved, or None when it can. A salaried
    person is costed from their weekly salary, so the ceiling is not theirs."""
    try:
        value = Decimal(str(rate))
    except (InvalidOperation, ValueError, TypeError):
        return "Enter the hourly rate as a number, e.g. 31.50"
    if value <= 0:
        return "The hourly rate must be more than $0"
    if value > MAX_HOURLY_RATE and str(pay_basis or "hourly").lower() != "salaried":
        return (f"{_money(value)} an hour is over {_money(MAX_HOURLY_RATE)} — that looks like "
                "a weekly or annual salary. Enter what they're paid per hour.")
    return None


def _email(value) -> str:
    return (value or "").strip().lower()


def is_anonymised(emp) -> bool:
    """Personal details already removed (services/privacy) — the record stays
    for history and is never edited back into a person."""
    return _email(getattr(emp, "email", None)).endswith("@deleted.local")


def email_holder(db, venue_id: str, email: str, except_id: Optional[str] = None):
    """Another staff record at this venue already carrying ``email`` (any
    letter case), active or not. Every login lookup resolves by email, so two
    records sharing one would hand someone the other person's shifts."""
    wanted = _email(email)
    if not wanted:
        return None
    for emp in db.get_employees(venue_id) or []:
        if emp.id != except_id and _email(getattr(emp, "email", None)) == wanted:
            return emp
    return None


def email_taken_message(holder, email: str) -> str:
    tail = (" (an inactive record — reactivate them instead of adding them again)"
            if not is_active(holder) else "")
    return (f"{holder.name}'s staff record here already uses {email}{tail}. Each person's "
            "email must be their own — it's how their login finds them.")


def rate_change_entry(previous_rate, new_rate, effective_from: date, changed_by: Optional[str],
                      now: Optional[datetime] = None) -> dict:
    return {
        "rate": str(Decimal(str(new_rate)).quantize(Decimal("0.01"))),
        "previous_rate": (str(Decimal(str(previous_rate)).quantize(Decimal("0.01")))
                          if previous_rate is not None else None),
        "effective_from": effective_from.isoformat(),
        "changed_at": (now or datetime.utcnow()).isoformat(timespec="seconds"),
        "changed_by": changed_by,
    }


def logins_by_email(db, venue_id: str) -> dict:
    """lower-case email -> the login records that hold this venue, dashboard
    or staff-only: the rule services/linking uses to tie a login to a staff
    record."""
    from rosteriq.services.venue_access import held_venue_ids

    out: dict = {}
    try:
        users = db.list_users() or []
    except Exception:
        return out
    for u in users:
        email = _email(u.get("email"))
        if email and u.get("id") and venue_id in held_venue_ids(u):
            out.setdefault(email, []).append(u)
    return out


def linked_logins(db, emp) -> list:
    """The login(s) tied to this staff record: same email (any letter case),
    holding its venue — provided no other record at the venue carries that
    email, since then the email proves nothing about whose login it is."""
    venue_id, email = getattr(emp, "venue_id", None), _email(getattr(emp, "email", None))
    if not venue_id or not email or is_anonymised(emp):
        return []
    sharing = [e for e in db.get_employees(venue_id) or [] if _email(getattr(e, "email", None)) == email]
    if [e.id for e in sharing] != [emp.id]:
        return []
    return logins_by_email(db, venue_id).get(email, [])


def join_code_rows(db, venue_id: str, origin: str) -> list:
    """Every active person at the venue with no linked login yet, with the
    code and sign-up link a manager hands them."""
    from rosteriq.services.linking import join_code

    linked = logins_by_email(db, venue_id)
    link = f"{origin.rstrip('/')}/register?join=1"
    rows = []
    for emp in active_only(db.get_employees(venue_id)):
        if _email(getattr(emp, "email", None)) in linked:
            continue
        rows.append({
            "employee_id": emp.id,
            "name": emp.name,
            "mobile": getattr(emp, "phone", None) or "",
            "email": getattr(emp, "email", None) or "",
            "join_code": join_code(emp.id),
            "link": link,
        })
    rows.sort(key=lambda r: r["name"].lower())
    return rows


def _csv_cell(value) -> str:
    text = str(value if value is not None else "")
    # A cell a spreadsheet would run as a formula is written as text.
    if text[:1] in ("=", "+", "-", "@", "\t", "\r"):
        text = "'" + text
    return text


def join_codes_csv(rows: list) -> str:
    import csv
    import io

    buf = io.StringIO()
    writer = csv.writer(buf, lineterminator="\n")
    writer.writerow(["Name", "Mobile", "Email", "Join code", "Sign-up link"])
    for r in rows:
        writer.writerow([_csv_cell(r[k]) for k in ("name", "mobile", "email", "join_code", "link")])
    return buf.getvalue()


def keep_record_state(employee, previous, changed_by: Optional[str] = None,
                      today: Optional[date] = None):
    """A whole-record upsert (POST /employees, an integration re-sync) must
    not quietly reactivate someone who left or drop their rate history just
    because the body didn't mention them. A rate that moved is recorded."""
    if previous is None:
        return employee
    sent = getattr(employee, "model_fields_set", set())
    if "active" not in sent and not is_active(previous):
        employee.active = False
    if "rate_history" not in sent:
        history = list(getattr(previous, "rate_history", None) or [])
        old, new = previous.hourly_base_rate, employee.hourly_base_rate
        if Decimal(str(old)) != Decimal(str(new)):
            history.append(rate_change_entry(old, new, today or date.today(), changed_by))
        employee.rate_history = history
    return employee


def retire_logins(db, venue_id: str, logins: list) -> dict:
    """After a leaver's details are removed, each login loses this venue,
    dashboard or staff-only, and keeps its access everywhere else. One that
    reaches no other venue is switched off (and its refresh tokens revoked).
    Owner accounts are never touched here."""
    from rosteriq.services.venue_access import drop_venue, held_venue_ids

    deactivated, venue_removed = [], []
    for user in logins:
        if user.get("role") == "owner" or user.get("is_owner"):
            continue
        if venue_id not in held_venue_ids(user):
            continue
        drop_venue(user, venue_id)
        rest = held_venue_ids(user)
        if not rest:
            user["is_active"] = False
        db.save_user(user)
        if not rest:
            try:
                db.revoke_refresh_tokens_for_user(user["id"])
            except Exception:  # noqa: BLE001 — the login is already off
                pass
            deactivated.append(user["id"])
        else:
            venue_removed.append(user["id"])
    return {"deactivated": deactivated, "venue_removed": venue_removed}
