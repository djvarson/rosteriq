"""
Account ↔ employee linking. The ONE place that answers "which employee record
is this login?" for the staff hub (/api/me/*), the legacy staff portal
(/api/staff/*), the time clock and the AI.

Rules
-----
* A login is linked to an employee by EMAIL, but only inside venues the user
  already holds (owners: every venue). The demo identities are only ever
  matched inside the demo venue.
* A self-registered user who holds NO venue yet is NOT auto-linked any more.
  Registration email is unverified, so "the manager typed this email on an
  employee record" is not proof the person logging in is that employee —
  anyone who knew a staff member's email could register as them and read
  their shifts and pay. Instead the user enters a JOIN CODE the manager hands
  them (staff page → "Join code"). The code proves the manager gave it to
  this person; entering it links the account and durably grants the venue.
* Nothing here tells an unlinked login whether (or where) a staff record
  carries its email — that would be a tenant-membership oracle for anyone
  who registers with someone else's address.
* Join codes are deterministic — HMAC(JWT_SECRET, employee_id) rendered as
  8 unambiguous base32 characters ("XXXX-XXXX") — so nothing new is stored
  and every worker/process agrees. Rotating JWT_SECRET rotates every code.
  Brute force: 32^8 ≈ 1e12 codes; attempts are also throttled per user, and
  wrong codes per IP so minting accounts buys no extra guesses.
"""

from __future__ import annotations

import hashlib
import hmac
import logging
import os
import time
from typing import List, Optional, Tuple

logger = logging.getLogger(__name__)

# Crockford base32: 32 symbols, no I/L/O/U; on input O→0 and I/L→1
_ALPHABET = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"
assert len(_ALPHABET) == 32 and len(set(_ALPHABET)) == 32
_CONFUSABLE = str.maketrans({"O": "0", "I": "1", "L": "1"})

MAX_CODE_ATTEMPTS = 5          # per user per window
CODE_ATTEMPT_WINDOW_S = 15 * 60
_attempts: dict = {}           # user_id -> [first_ts, count]
# Wrong codes per IP per window. Only failures count: a whole pub linking
# from one Wi-Fi on day one succeeds and never gets near it.
MAX_FAILED_CODES_PER_IP = 30
_ip_failures: dict = {}        # ip -> [first_ts, count]


def _secret() -> bytes:
    return (os.environ.get("JWT_SECRET") or "dev-only-join-code-secret").encode()


def join_code(employee_id: str) -> str:
    """The employee's join code (deterministic; shown to managers only)."""
    digest = hmac.new(_secret(), f"join:{employee_id}".encode(), hashlib.sha256).digest()
    # 40 bits → 8 base32 chars
    n = int.from_bytes(digest[:5], "big")
    chars = []
    for _ in range(8):
        chars.append(_ALPHABET[n & 31])
        n >>= 5
    code = "".join(chars)
    return f"{code[:4]}-{code[4:]}"


def normalise_code(raw: str) -> str:
    """Accept 'abcd-efgh', 'ABCDEFGH', ' abcd efgh ' … → 'ABCD-EFGH'."""
    s = "".join(ch for ch in (raw or "").upper() if ch.isalnum()).translate(_CONFUSABLE)
    return f"{s[:4]}-{s[4:8]}" if len(s) >= 8 else s


def code_matches(employee_id: str, raw: str) -> bool:
    return hmac.compare_digest(join_code(employee_id), normalise_code(raw))


def _email_of(emp) -> str:
    return (getattr(emp, "email", "") or "").strip().lower()


def find_linked_employee(db, user) -> Tuple[Optional[object], Optional[str]]:
    """
    (employee, venue_id) for this login, searching ONLY the venues the user
    holds (owners: all). Never grants anything. Returns (None, None) if there
    is no email match in scope.
    """
    email = (getattr(user, "email", "") or "").strip().lower()
    if not email:
        return None, None
    try:
        from rosteriq.services.demo import (
            DEMO_VENUE_ID, DEMO_USER_ID, DEMO_STAFF_USER_ID,
            DEMO_USER_EMAIL, DEMO_STAFF_EMAIL,
        )
        demo_ids = {DEMO_USER_ID, DEMO_STAFF_USER_ID}
        demo_emails = {DEMO_USER_EMAIL.strip().lower(), DEMO_STAFF_EMAIL.strip().lower()}
    except Exception:  # pragma: no cover — demo module always present
        DEMO_VENUE_ID, demo_ids, demo_emails = None, set(), set()

    if getattr(user, "user_id", None) in demo_ids or email in demo_emails:
        if DEMO_VENUE_ID:
            for emp in db.get_employees(DEMO_VENUE_ID) or []:
                if _email_of(emp) == email:
                    return emp, DEMO_VENUE_ID
        return None, None

    if getattr(user, "is_owner", False):
        venue_ids = [v.id for v in (db.list_venues() or [])]
    else:
        venue_ids = list(getattr(user, "venue_ids", None) or [])
    # A person who left one venue but still works at another (same email at
    # both) links to the record where they are active.
    fallback = (None, None)
    for vid in venue_ids:
        for emp in db.get_employees(vid) or []:
            if _email_of(emp) == email:
                if getattr(emp, "active", True) is not False:
                    return emp, vid
                if fallback[0] is None:
                    fallback = (emp, vid)
    return fallback


def _staff_venue_ids(db, user) -> list:
    """Venues a MANAGER login is linked to as staff only (services/venue_access.py)."""
    held = getattr(user, "staff_venue_ids", None)
    if held is None:
        try:
            held = (db.get_user_by_id(getattr(user, "user_id", None)) or {}).get("staff_venue_ids")
        except Exception:
            held = None
    return list(held or [])


def find_linked_employees(db, user) -> List[Tuple[object, str]]:
    """Every (employee, venue_id) this login is linked to, primary venue
    first: the email match in each venue the user holds, dashboard or
    staff-only. Same scope rules as find_linked_employee (owners: every
    venue; demo identities: the demo venue only). Records a manager has set
    inactive are left out unless that would leave nothing."""
    from rosteriq.services.demo import DEMO_VENUE_ID, is_demo_identity
    email = (getattr(user, "email", "") or "").strip().lower()
    if not email:
        return []
    if is_demo_identity(getattr(user, "user_id", None), email):
        emp, vid = find_linked_employee(db, user)
        return [(emp, vid)] if emp is not None else []
    if getattr(user, "is_owner", False):
        venue_ids = [v.id for v in (db.list_venues() or [])]
    else:
        venue_ids = list(getattr(user, "venue_ids", None) or [])
    for v in _staff_venue_ids(db, user):
        if v not in venue_ids:
            venue_ids.append(v)
    out = []
    for vid in venue_ids:
        if vid == DEMO_VENUE_ID:
            continue
        for emp in db.get_employees(vid) or []:
            if _email_of(emp) == email:
                out.append((emp, vid))
                break
    active = [(e, v) for e, v in out if getattr(e, "active", True)]
    return active or out


def _throttle(user_id: str) -> None:
    """Raise if this user has burned MAX_CODE_ATTEMPTS in the window."""
    from fastapi import HTTPException
    now = time.time()
    first, count = _attempts.get(user_id, (now, 0))
    if now - first > CODE_ATTEMPT_WINDOW_S:
        first, count = now, 0
    if count >= MAX_CODE_ATTEMPTS:
        raise HTTPException(
            status_code=429,
            detail="Too many join-code attempts. Try again in 15 minutes.",
        )
    _attempts[user_id] = [first, count + 1]


def _clear_throttle(user_id: str) -> None:
    _attempts.pop(user_id, None)


def _ip_window(ip: str, now: float):
    first, count = _ip_failures.get(ip, (now, 0))
    return (now, 0) if now - first > CODE_ATTEMPT_WINDOW_S else (first, count)


def _throttle_ip(ip: Optional[str]) -> None:
    """Raise if this IP has entered MAX_FAILED_CODES_PER_IP wrong codes in the window."""
    from fastapi import HTTPException
    if ip and _ip_window(ip, time.time())[1] >= MAX_FAILED_CODES_PER_IP:
        raise HTTPException(
            status_code=429,
            detail="Too many join-code attempts. Try again in 15 minutes.",
        )


def _record_ip_failure(ip: Optional[str]) -> None:
    if not ip:
        return
    now = time.time()
    if len(_ip_failures) > 10_000:
        for stale in [k for k, (first, _) in _ip_failures.items()
                      if now - first > CODE_ATTEMPT_WINDOW_S]:
            _ip_failures.pop(stale, None)
    first, count = _ip_window(ip, now)
    _ip_failures[ip] = [first, count + 1]


def link_with_code(db, user, raw_code: str,
                   client_ip: Optional[str] = None) -> Tuple[Optional[object], Optional[str]]:
    """
    Link this login to the employee whose join code was entered. The employee
    must exist, must not already belong to a venue the user holds (nothing to
    do) and the code must match. On success the venue is durably granted on
    the user record and an audit event is written. Raises HTTPException 400
    on a bad code, 429 when throttled.
    """
    from fastapi import HTTPException
    from rosteriq.services.events import audit, security

    uid = getattr(user, "user_id", None) or ""
    _throttle_ip(client_ip)
    _throttle(uid)
    code = normalise_code(raw_code)
    if len(code) != 9:
        raise HTTPException(status_code=400, detail="Join code must be 8 characters, e.g. ABCD-EFGH")

    # Candidate: any employee whose code matches. Codes are per-employee, so
    # this is a direct lookup over the (small) employee table.
    match, match_vid = None, None
    for venue in db.list_venues() or []:
        vid = getattr(venue, "id", None)
        if not vid:
            continue
        for emp in db.get_employees(vid) or []:
            if code_matches(emp.id, code):
                match, match_vid = emp, vid
                break
        if match is not None:
            break

    if match is None:
        _record_ip_failure(client_ip)
        security("link.code_rejected", venue_id=None, user_id=uid, outcome="denied")
        raise HTTPException(status_code=400, detail="That join code isn't valid. Check it with your manager.")

    # Demo-venue join codes are for show: redeeming one would stamp a real
    # login's email onto a record every Try Demo visitor can see.
    from rosteriq.services.demo import DEMO_VENUE_ID
    if match_vid == DEMO_VENUE_ID:
        raise HTTPException(status_code=400, detail="That's a demo join code — ask your manager for your own.")
    if getattr(match, "active", True) is False:
        security("link.code_inactive_record", venue_id=match_vid, user_id=uid, outcome="denied",
                 employee_id=match.id)
        raise HTTPException(status_code=400,
                            detail="That staff record isn't active any more. Ask your manager.")

    email = (getattr(user, "email", "") or "").strip().lower()
    emp_email = _email_of(match)
    if emp_email and email and emp_email != email:
        # The code is real but for someone else's record. Refuse: linking a
        # login to an employee with a different email would let a code that
        # leaked hijack a colleague's shifts and pay.
        security("link.code_email_mismatch", venue_id=match_vid, user_id=uid, outcome="denied",
                 employee_id=match.id)
        raise HTTPException(
            status_code=400,
            detail="This join code belongs to a staff record with a different email. "
                   "Ask your manager to put your login email on your staff profile.",
        )

    # Dashboard role is account-wide. A login that already manages some venue
    # must not arrive at another venue as a manager, so it is linked there as
    # STAFF ONLY (staff_venue_ids: its shifts reach /my, the dashboard stays
    # shut). Even that only inside one owner's business: a login that manages
    # a throwaway venue it created itself is refused, because no other manager
    # runs both the throwaway and the workplace.
    from rosteriq.services.venue_access import (
        held_venue_ids, is_manager_login, is_platform_owner, same_owner_runs,
    )
    rec_now = db.get_user_by_id(uid) or {}
    held_now = held_venue_ids(rec_now)
    staff_only = False
    if match_vid not in held_now and is_manager_login(rec_now):
        if is_platform_owner(rec_now) or not same_owner_runs(db, held_now + [match_vid],
                                                             exclude_user_id=uid):
            security("link.manager_account_refused", venue_id=match_vid, user_id=uid,
                     outcome="denied", employee_id=match.id)
            raise HTTPException(
                status_code=409,
                detail="This login manages a venue, so it can only be linked as staff at "
                       "another venue the same owner runs. Ask a manager of both venues to "
                       "add this one from Staff \u2192 Access, or ask RosterIQ support.",
            )
        staff_only = True

    # A record with no email yet: the code IS the proof — stamp the login
    # email on it so every later email lookup finds this employee. Never
    # stamp one another record here already carries: signup email is
    # unverified, so a hire could register as a colleague and then be the
    # login every email lookup (access changes, reset links) resolves to.
    if not emp_email and email:
        if any(_email_of(e) == email and e.id != match.id for e in (db.get_employees(match_vid) or [])):
            security("link.email_already_on_record", venue_id=match_vid, user_id=uid,
                     outcome="denied", employee_id=match.id)
            raise HTTPException(
                status_code=409,
                detail="Another staff record here already uses this login's email. "
                       "Ask your manager to check the staff list.",
            )
    if not emp_email and email:
        try:
            match.email = email
            db.save_employee(match)
        except Exception as e:  # noqa: BLE001
            logger.warning(f"join-code: could not stamp email on {match.id}: {e}")

    # Grant the venue durably on the user record
    try:
        rec = db.get_user_by_id(uid)
        field = "staff_venue_ids" if staff_only else "venue_ids"
        if rec is not None and match_vid not in held_venue_ids(rec):
            rec[field] = list(rec.get(field) or []) + [match_vid]
            db.save_user(rec)
    except Exception as e:  # noqa: BLE001
        logger.warning(f"join-code grant failed for {uid}: {e}")
        raise HTTPException(status_code=500, detail="Could not link your account. Try again.")
    try:
        # keep the in-flight request's context consistent
        if hasattr(user, field) and match_vid not in (getattr(user, field) or []):
            setattr(user, field, list(getattr(user, field) or []) + [match_vid])
    except Exception:
        pass
    _clear_throttle(uid)
    audit("user.venue_grant", match_vid, "user", uid, email=email, employee_id=match.id,
          reason="join_code", staff_only=staff_only or None)
    logger.info(f"Join code: {email} -> employee {match.id} at {match_vid}")
    return match, match_vid
