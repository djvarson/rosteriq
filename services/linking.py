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
* Join codes are deterministic — HMAC(JWT_SECRET, employee_id) rendered as
  8 unambiguous base32 characters ("XXXX-XXXX") — so nothing new is stored
  and every worker/process agrees. Rotating JWT_SECRET rotates every code.
  Brute force: 32^8 ≈ 1e12 codes; attempts are also throttled per user.
"""

from __future__ import annotations

import hashlib
import hmac
import logging
import os
import time
from typing import Optional, Tuple

logger = logging.getLogger(__name__)

# Crockford base32: 32 symbols, no I/L/O/U; on input O→0 and I/L→1
_ALPHABET = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"
assert len(_ALPHABET) == 32 and len(set(_ALPHABET)) == 32
_CONFUSABLE = str.maketrans({"O": "0", "I": "1", "L": "1"})

MAX_CODE_ATTEMPTS = 5          # per user per window
CODE_ATTEMPT_WINDOW_S = 15 * 60
_attempts: dict = {}           # user_id -> [first_ts, count]


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
    for vid in venue_ids:
        for emp in db.get_employees(vid) or []:
            if _email_of(emp) == email:
                return emp, vid
    return None, None


def find_pending_link(db, user) -> Tuple[Optional[object], Optional[str]]:
    """
    An employee record OUTSIDE the user's venues whose email matches — i.e.
    someone a manager has set up who has not yet entered their join code.
    Used only to show the "enter your join code" prompt; never to grant.
    """
    email = (getattr(user, "email", "") or "").strip().lower()
    if not email or getattr(user, "is_owner", False):
        return None, None
    held = set(getattr(user, "venue_ids", None) or [])
    for venue in db.list_venues() or []:
        vid = getattr(venue, "id", None)
        if not vid or vid in held:
            continue
        for emp in db.get_employees(vid) or []:
            if _email_of(emp) == email:
                return emp, vid
    return None, None


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


def link_with_code(db, user, raw_code: str) -> Tuple[Optional[object], Optional[str]]:
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
        security("link.code_rejected", venue_id=None, user_id=uid, outcome="denied")
        raise HTTPException(status_code=400, detail="That join code isn't valid. Check it with your manager.")

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

    # A record with no email yet: the code IS the proof — stamp the login
    # email on it so every later email lookup finds this employee.
    if not emp_email and email:
        try:
            match.email = email
            db.save_employee(match)
        except Exception as e:  # noqa: BLE001
            logger.warning(f"join-code: could not stamp email on {match.id}: {e}")

    # Grant the venue durably on the user record
    try:
        rec = db.get_user_by_id(uid)
        if rec is not None:
            vids = list(rec.get("venue_ids") or [])
            if match_vid not in vids:
                vids.append(match_vid)
                rec["venue_ids"] = vids
                db.save_user(rec)
    except Exception as e:  # noqa: BLE001
        logger.warning(f"join-code grant failed for {uid}: {e}")
        raise HTTPException(status_code=500, detail="Could not link your account. Try again.")
    try:
        # keep the in-flight request's context consistent
        if hasattr(user, "venue_ids") and match_vid not in (user.venue_ids or []):
            user.venue_ids = list(user.venue_ids or []) + [match_vid]
    except Exception:
        pass
    _clear_throttle(uid)
    audit("user.venue_grant", match_vid, "user", uid, email=email, employee_id=match.id,
          reason="join_code")
    logger.info(f"Join code: {email} -> employee {match.id} at {match_vid}")
    return match, match_vid
