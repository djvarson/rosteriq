"""
Staff portal — the staff-facing side of the Venue OS, answering the #1
industry complaint (schedule visibility: 41% of operators name it their staff's
top gripe) and the self-service basics staff expect: my roster, my hours, my
leave.

Identity model: a portal user is linked to an Employee record by EMAIL — the
manager puts the staff member's email on their employee record, the staff
member registers/logs in with that same email, and /api/me/* resolves the
match within the venues the user can access. No match = a clear explanation,
not an empty screen.

Staff routes:
    GET  /api/me/profile      -- who am I linked to (employee + venue)
    GET  /api/me/shifts       -- my upcoming shifts (next 14 days)
    GET  /api/me/timesheets   -- my worked hours (last 14 days)
    GET  /api/me/leave        -- my leave requests
    POST /api/me/leave        -- request leave / unavailability

Manager routes:
    GET  /api/leave           -- venue's leave requests (filter by status)
    POST /api/leave/{id}/decide -- approve / decline with optional note

Shift cover (phase 2 — "I can't work this shift"):
    POST /api/me/shifts/{shift_id}/cover -- put my shift up for cover
    GET  /api/me/cover                   -- cover board (up for grabs + mine)
    POST /api/me/cover/{id}/claim        -- claim a co-worker's shift
    POST /api/me/cover/{id}/cancel       -- requester withdraws an open/claimed cover
    GET  /api/cover                      -- manager: venue's cover requests
    POST /api/cover/{id}/decide          -- manager approve/decline; approval
                                            REASSIGNS THE SHIFT on the roster —
                                            the roster is the source of truth,
                                            never just a status flag.
"""

import logging
import uuid
from datetime import date, datetime, timedelta

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from pydantic import BaseModel, Field

from rosteriq.database import get_db
from rosteriq.services.clock import venue_today
from rosteriq.services.availability_rules import day_windows
from rosteriq.services.current_roster import live_rosters, live_shifts
from rosteriq.services.leave_shifts import (
    LEAVE_TYPE_PATTERN, LIVE_COVER, apply_approved_leave, cover_day, cover_holders, cover_stands,
    is_open, leave_type_label, opened_summary, shift_slot, standing_cover_ids,
    unsuperseded_rosters, withdraw_cover,
)
from rosteriq.services import roster_edit as edit
from rosteriq.middleware.tenant import scoped_employee_map
from typing import Optional, List
from rosteriq.middleware.auth import get_current_user, UserContext
from rosteriq.middleware.tenant import enforce_venue_access, enforce_venue_manager, enforce_venue_manager_any
from rosteriq.services.demo import (
    DEMO_USER_ID, DEMO_USER_EMAIL, DEMO_VENUE_ID,
    DEMO_STAFF_USER_ID, DEMO_STAFF_EMAIL,
)
from rosteriq.services.events import audit
from rosteriq.services.staff_records import is_active

logger = logging.getLogger(__name__)

# The public demo identities are a sandbox: they may only ever link to the
# demo venue, never auto-link into (or be granted) a real tenant.
_DEMO_USER_IDS = frozenset({DEMO_USER_ID, DEMO_STAFF_USER_ID})
_DEMO_EMAILS = frozenset(e.strip().lower() for e in (DEMO_USER_EMAIL, DEMO_STAFF_EMAIL))

router = APIRouter(tags=["staff-portal"])

_WEEKDAYS = ["monday", "tuesday", "wednesday", "thursday",
             "friday", "saturday", "sunday"]


class LeaveRequestBody(BaseModel):
    start_date: date
    end_date: date
    reason: str = Field(default="", max_length=300)
    leave_type: str = Field(default="other", pattern=LEAVE_TYPE_PATTERN)
    # Someone who works more than one venue: which venue(s) this leave is
    # for. Left out = every venue they work.
    venue_ids: Optional[List[str]] = Field(default=None, max_length=10)


class LeaveDecision(BaseModel):
    venue_id: str
    approve: bool
    note: str = Field(default="", max_length=300)
    leave_type: Optional[str] = Field(default=None, pattern=LEAVE_TYPE_PATTERN)
    paid_hours: Optional[float] = Field(default=None, ge=0, le=400)


class TimeRange(BaseModel):
    start: str = Field(..., description="HH:MM")
    end: str = Field(..., description="HH:MM")


class DayAvailability(BaseModel):
    status: str = Field(..., pattern=r"^(available|unavailable|partial)$")
    ranges: list[TimeRange] = Field(default_factory=list,
                                    description="For 'partial': [{start:'HH:MM', end:'HH:MM'}]")


class AvailabilityBody(BaseModel):
    # weekday name -> DayAvailability; only supplied days are changed. Typed so
    # a malformed shape (e.g. {"monday": "unavailable"}) is a clean 422 from
    # pydantic, not a 500 from the handler assuming a dict.
    days: dict[str, DayAvailability] = Field(
        ..., description="e.g. {'monday': {'status':'unavailable'}}")


def _parse_hhmm(v: str) -> int:
    """'HH:MM' -> MINUTES since midnight (not just the hour — so a window like
    17:30–17:45 is comparable). Raises 422 on garbage."""
    try:
        s = str(v)
        h, m = (s.split(":")[:2] if ":" in s else (s, "0"))
        hi, mi = int(h), int(m)
        if not (0 <= hi <= 23 and 0 <= mi <= 59):
            raise ValueError
        return hi * 60 + mi
    except Exception:
        raise HTTPException(status_code=422, detail=f"Bad time '{v}' — use HH:MM (00:00–23:59)")


class CoverRequestBody(BaseModel):
    reason: str = Field(default="", max_length=300)


class CoverDecision(BaseModel):
    venue_id: str
    approve: bool
    note: str = Field(default="", max_length=300)


def _linked_employee(db, user: UserContext):
    """(employee, venue_id) for this login — see services/linking.py.

    Email match INSIDE the venues the user holds only. Self-registered users
    who hold no venue link by entering the join code their manager gives them
    (POST /api/me/link); the old "search every venue and auto-grant on email
    match" is gone because registration email is unverified."""
    from rosteriq.services.linking import find_linked_employee
    return find_linked_employee(db, user)


def _linked_employees(db, user: UserContext, active_only: bool = False) -> list:
    """Every (employee, venue_id) this login is linked to, primary first —
    a person who works two venues has a record at each."""
    from rosteriq.services.linking import find_linked_employees
    return find_linked_employees(db, user, active_only=active_only)


_NO_LONGER_ACTIVE = "Your staff record here is no longer active — ask your manager"


def _on_team(links: list) -> list:
    """The links whose record is still active. A record set inactive (they
    left) keeps its own history (profile, shifts, hours, leave history) but
    no longer opens the venue's team surfaces."""
    return [(e, v) for e, v in links if is_active(e)]


def _team_links(db, user: UserContext) -> list:
    """Links for a write only a current team member makes (leave,
    availability, cover, acknowledgements). Not linked: 409. Left everywhere:
    403, the answer someone who left one of two venues gets there."""
    links = _linked_employees(db, user)
    if not links:
        raise HTTPException(status_code=409, detail=_no_link_response(user)["message"])
    links = _on_team(links)
    if not links:
        raise HTTPException(status_code=403, detail=_NO_LONGER_ACTIVE)
    return links


def _left_at(db, user: UserContext, venue_id: str) -> bool:
    """This login's staff record at ``venue_id`` has been set inactive."""
    from rosteriq.services.linking import linked_records
    return any(v == venue_id and not is_active(e) for e, v in linked_records(db, user))


def _linked_employee_at(db, user: UserContext, venue_id: str):
    """This login's ACTIVE staff record at ``venue_id``, or None."""
    for emp, vid in _linked_employees(db, user, active_only=True):
        if vid == venue_id:
            return emp
    return None


def _venue_names(db, venue_ids) -> dict:
    out = {}
    for vid in venue_ids:
        venue = db.get_venue(vid)
        out[vid] = getattr(venue, "name", vid) if venue else vid
    return out


def _no_link_response(user: UserContext, db=None) -> dict:
    """Not linked. The response is the SAME whether or not some venue's staff
    record carries this email: registration email is unverified, so any
    difference (a venue name, a pending flag) would tell whoever registered
    with a stranger's email which tenant employs that person. The join code
    is the only way to link, so link_pending is always True."""
    msg = ("No staff record matches your login email "
           f"({user.email}) yet. Ask your manager to add this email to your "
           "profile in Staff and give you your join code, then enter it to "
           "link your account.")
    return {"linked": False, "link_pending": True, "message": msg}


# ---------------------------------------------------------------------------
# Staff-facing
# ---------------------------------------------------------------------------

@router.get("/api/me/profile")
async def my_profile(user: UserContext = Depends(get_current_user)) -> dict:
    db = get_db()
    links = _linked_employees(db, user)
    if not links:
        return _no_link_response(user, db)
    emp, vid = links[0]
    names = _venue_names(db, [v for _, v in links])
    return {
        "linked": True,
        "employee_id": emp.id,
        "name": emp.name,
        "venue_id": vid,
        "venue_name": names[vid],
        "employment_type": str(emp.employment_type.value if hasattr(emp.employment_type, "value") else emp.employment_type),
        "skills": list(emp.skills or []),
        "venues": [{"venue_id": v, "venue_name": names[v], "employee_id": e.id} for e, v in links],
    }


class LinkBody(BaseModel):
    code: str = Field(..., min_length=4, max_length=16)


@router.post("/api/me/link")
async def link_my_account(body: LinkBody, request: Request,
                          user: UserContext = Depends(get_current_user)) -> dict:
    """Link this login to a staff record with the join code the manager gave
    you. Grants the venue durably; throttled per login (5 tries / 15 min)
    and per IP (wrong codes)."""
    from rosteriq.services.linking import link_with_code
    db = get_db()
    emp, vid = link_with_code(db, user, body.code,
                              client_ip=request.client.host if request.client else None)
    venue = db.get_venue(vid)
    return {
        "linked": True,
        "employee_id": emp.id,
        "name": emp.name,
        "venue_id": vid,
        "venue_name": getattr(venue, "name", vid) if venue else vid,
    }


@router.get("/api/employees/{employee_id}/join-code")
async def employee_join_code(employee_id: str, user: UserContext = Depends(get_current_user)) -> dict:
    """The join code a manager hands a staff member so they can link their
    login. A full manager of the employee's venue only, like the bulk sheet:
    the code binds a login to the record, so a department manager gets none
    (a foreign id is a 404)."""
    from rosteriq.middleware.tenant import load_employee_in_scope
    from rosteriq.services.linking import join_code
    if user.role not in ("manager", "owner") and not getattr(user, "is_owner", False):
        raise HTTPException(status_code=403, detail="Managers only")
    db = get_db()
    emp = load_employee_in_scope(db, employee_id)
    enforce_venue_manager(getattr(emp, "venue_id", None))
    return {
        "employee_id": emp.id,
        "name": emp.name,
        "email": getattr(emp, "email", None),
        "join_code": join_code(emp.id),
        "instructions": ("Ask them to sign up at /login with this email, open the "
                         "staff app and enter the join code when prompted."),
    }


_LEVEL_RANK = {"staff": 0, "department": 1, "full": 2}


def _lowers_access(user, account, lower_to) -> bool:
    """``lower_to`` = (venue_id, level) is below the login's current access
    at a venue the caller fully manages."""
    from rosteriq.services.venue_access import access_map, fully_manages
    if not lower_to:
        return False
    venue_id, level = lower_to
    current = (access_map(account).get(venue_id) or {}).get("level")
    return (fully_manages(user, venue_id) and current in _LEVEL_RANK and level in _LEVEL_RANK
            and _LEVEL_RANK[level] < _LEVEL_RANK[current])


def _account_management_guards(user, emp, db, lower_to=None):
    """Shared spine for the two login-management endpoints.

    Returns the target ACCOUNT after enforcing, in order:
    demo sandbox (neither demo identity may manage logins, and demo
    accounts may never be managed); manager/owner caller; a venue on the
    employee record (fail CLOSED — a venue-less record proves nothing);
    a linked login for the email (case-insensitive); the target account
    HOLDS this venue (an email typed onto an employee record is
    attacker-controlled — without this check any manager could mint
    takeovers for accounts platform-wide); the caller is a FULL manager of
    EVERY venue the target login reaches (resets and role flips are
    account-global, so one venue's manager must never control a login that
    reaches venues they don't run); the target is not the platform owner
    and did not create one of its venues (the venue's owner).

    ``lower_to`` = (venue_id, level) names a change that only LOWERS the
    login's access at that one venue. It gives the caller nothing anywhere
    else, so a full manager of that venue may make it whatever else the
    login reaches; only that venue's creator stays out of reach.
    """
    if user.user_id in _DEMO_USER_IDS or (user.email or "").strip().lower() in _DEMO_EMAILS:
        raise HTTPException(status_code=403, detail="The demo can't manage logins")
    if user.role not in ("manager", "owner") and not getattr(user, "is_owner", False):
        raise HTTPException(status_code=403, detail="Managers only")
    vid = getattr(emp, "venue_id", None)
    if not vid:
        raise HTTPException(status_code=409,
                            detail="This staff record has no venue — fix the record first")
    email_raw = (getattr(emp, "email", None) or "").strip()
    if not email_raw:
        raise HTTPException(status_code=409,
                            detail="This staff record has no email — add one, then use a join code first")
    # Registration stores what pydantic's EmailStr yields: raw local part,
    # lowercased domain. Staff records hold whatever the manager typed.
    variants = [email_raw, email_raw.lower()]
    if "@" in email_raw:
        local, _, domain = email_raw.rpartition("@")
        variants.insert(1, f"{local}@{domain.lower()}")
    account = None
    for candidate in dict.fromkeys(variants):
        account = db.get_user_by_email(candidate)
        if account:
            break
    if not account:
        raise HTTPException(status_code=409,
                            detail="No login exists for this email yet — hand them a join code instead")
    if (account.get("email") or "").strip().lower() in _DEMO_EMAILS:
        raise HTTPException(status_code=403, detail="Demo accounts can't be managed")
    from rosteriq.services.venue_access import (
        created_venue_ids, fully_manages, held_venue_ids, is_platform_owner,
    )
    held = held_venue_ids(account)
    if vid not in held:
        raise HTTPException(status_code=409,
                            detail="Their login isn't linked to this venue yet — join code first")
    # The account was found by the email TEXT on the record. It must be the
    # login actually linked to THIS record: exactly one staff record here
    # carries that email (it's this one), and no other login shares the email
    # in a different letter case.
    acct_email = (account.get("email") or "").strip().lower()
    same_email = [e for e in (db.get_employees(vid) or [])
                  if (getattr(e, "email", "") or "").strip().lower() == acct_email]
    if len(same_email) != 1 or same_email[0].id != getattr(emp, "id", None):
        raise HTTPException(status_code=409,
                            detail="More than one staff record here uses this email — "
                                   "fix the staff list before changing their login")
    twins = {u.get("id") for u in (db.get_user_by_email(v) for v in dict.fromkeys(variants)) if u}
    if len(twins) > 1:
        raise HTTPException(status_code=409,
                            detail="More than one login uses this email in different letter case — "
                                   "ask RosterIQ support to merge them")
    # Both a role flip AND a password reset act on the whole ACCOUNT, not
    # just this venue: resetting its password hands the caller a session that
    # reaches every venue the login does. So the caller must fully manage all
    # of them (an owner running both pubs may; one venue's manager may not).
    if not fully_manages(user, vid):
        raise HTTPException(status_code=403,
                            detail="Only a full manager of this venue can change staff logins")
    scope = [lower_to[0]] if _lowers_access(user, account, lower_to) else held
    if not all(fully_manages(user, v) for v in scope):
        raise HTTPException(status_code=409,
                            detail="Their login also reaches a venue you don't fully manage — "
                                   "changes to it need a manager of every venue it's linked to, "
                                   "or RosterIQ support")
    if account.get("role") == "owner" or account.get("is_owner"):
        raise HTTPException(status_code=403, detail="Owner accounts can't be changed here")
    if (not is_platform_owner(user) and account.get("id") != user.user_id
            and created_venue_ids(db, account.get("id"), scope)):
        raise HTTPException(status_code=403,
                            detail="This is the venue owner's login — only they (or RosterIQ "
                                   "support) can change it")
    return account


@router.post("/api/employees/{employee_id}/reset-link")
async def employee_reset_link(employee_id: str,
                              user: UserContext = Depends(get_current_user)) -> dict:
    """A one-hour, single-use password-reset link a manager hands (or texts)
    to a team member who is locked out. Mirrors the join-code philosophy:
    no email delivery to depend on — the manager IS the delivery channel.
    Manager/owner only, employee must be in the caller's venues, and the
    employee must already have a linked login (otherwise: join code)."""
    from rosteriq.middleware.tenant import load_employee_in_scope
    from rosteriq.routes.auth import auth_service, public_origin
    from rosteriq.services.events import security
    db = get_db()
    emp = load_employee_in_scope(db, employee_id)
    account = _account_management_guards(user, emp, db)
    email = account.get("email")
    token = auth_service.create_password_reset_token(email)
    if not token:
        raise HTTPException(status_code=500, detail="Could not create a reset link — try again")
    security("auth.reset_link_issued", outcome="ok",
             venue_id=getattr(emp, "venue_id", None),
             user_id=user.user_id, target=account.get("id"), email=email)
    return {
        "employee_id": emp.id,
        "name": emp.name,
        "email": email,
        "reset_url": f"{public_origin()}/reset-password?token={token}",
        "expires_in_minutes": 60,
        "instructions": "Send them this link however you like — it works once and dies in an hour.",
    }


class AccessRoleBody(BaseModel):
    role: str = Field(..., pattern="^(manager|staff)$")
    # Optional DEPARTMENT scope for a manager: the sections they may manage
    # (kitchen/bar/cellar/floor…). Empty/omitted = full venue manager. Ignored
    # when role == "staff".
    sections: Optional[List[str]] = None


@router.post("/api/employees/{employee_id}/access-role")
async def set_employee_access_role(employee_id: str, body: AccessRoleBody,
                                   user: UserContext = Depends(get_current_user)) -> dict:
    """Set a linked team member's dashboard access AT THIS RECORD'S VENUE:
    full manager, department manager (``sections``) or staff. The only
    role-granting path used to be first-venue bootstrap — a venue's 2IC
    could never approve leave.

    The dashboard role is one per login, so a login linked to other venues
    keeps its access there: made a manager here, their other staff venues
    become staff-only (their shifts still reach /my, the dashboard stays
    shut there) rather than silently becoming full manager venues.
    Guards: a full manager of every venue the login reaches (of this one
    alone when the change lowers their access here, unless they created
    it); never an owner account; never your own account (no self-lockouts)."""
    from rosteriq.middleware.tenant import load_employee_in_scope
    from rosteriq.services.events import audit
    from rosteriq.services.venue_access import (
        DEPARTMENT, FULL, STAFF, access_map, apply_access_map, visible_to,
    )
    db = get_db()
    emp = load_employee_in_scope(db, employee_id)
    vid = getattr(emp, "venue_id", None)
    # Normalise any requested department scope (free-string sections, like the
    # min_staff keys — strip/lowercase, drop blanks, dedupe preserving order).
    sections: List[str] = []
    seen = set()
    for s in (body.sections or []):
        n = str(s or "").strip().lower()
        if n and n not in seen:
            seen.add(n)
            sections.append(n)
    if body.role == "manager":
        level = DEPARTMENT if sections else FULL
    else:
        level = STAFF
    account = _account_management_guards(user, emp, db, lower_to=(vid, level))
    email = account.get("email")
    # Appointing managers / setting a department scope is a VENUE-WIDE user-
    # management action — a department manager (section-restricted) must not be
    # able to do it. enforce_venue_manager rejects section-restricted callers.
    enforce_venue_manager(vid)
    if account.get("id") == user.user_id:
        raise HTTPException(status_code=400, detail="You can't change your own access level")
    old = account.get("role")
    before = access_map(account)
    apply_access_map(account, {vid: {"level": level, "sections": sections}})
    db.save_user(account)
    # Only the venues the caller fully manages: lowering access here doesn't
    # need them to run the rest of the login.
    after = access_map(visible_to(user, account))
    audit("user.role_change", vid, "user", account["id"], email=email,
          old_role=old, new_role=account.get("role"), changed_by=user.user_id,
          sections=sections or None, reason="venue_manager_set",
          venue_access={v: a["level"] for v, a in after.items()})
    names = _venue_names(db, after.keys())
    if level == DEPARTMENT:
        msg = (f"{emp.name} is now a department manager for "
               f"{', '.join(sections)} at {names[vid]} — takes effect on their next sign-in.")
    elif level == FULL:
        msg = f"{emp.name} is now a manager at {names[vid]} — takes effect on their next sign-in."
    else:
        msg = f"{emp.name} is now staff at {names[vid]} — takes effect on their next sign-in."
    staff_elsewhere = [names[v] for v, a in after.items()
                       if v != vid and a["level"] == STAFF and before.get(v, {}).get("level") == STAFF
                       and account.get("role") == "manager"]
    if staff_elsewhere:
        msg += f" They stay staff-only at {', '.join(staff_elsewhere)}."
    return {"employee_id": emp.id, "name": emp.name, "email": email,
            "old_role": old, "role": body.role,
            "sections": sections, "message": msg,
            "venues": [{"venue_id": v, "venue_name": names[v], "level": a["level"],
                        "sections": a["sections"]} for v, a in after.items()]}


@router.get("/api/me/shifts")
async def my_shifts(user: UserContext = Depends(get_current_user)) -> dict:
    """My upcoming shifts — the schedule-visibility fix. Someone who works
    two venues sees both, each tagged with its venue."""
    db = get_db()
    links = _linked_employees(db, user)
    if not links:
        return _no_link_response(user, db)
    emp, vid = links[0]
    names = _venue_names(db, [v for _, v in links])
    today = venue_today(vid, db)
    horizon = today + timedelta(days=14)
    mine = []
    for link_emp, link_vid in links:
        start = venue_today(link_vid, db)
        try:
            for s in live_shifts(db, link_vid, start, start + timedelta(days=14), staff=True):
                if str(getattr(s, "employee_id", "")) != str(link_emp.id):
                    continue
                mine.append({
                    "id": s.id,
                    "date": s.date.isoformat(),
                    "day": s.date.strftime("%A"),
                    "start": s.start_time.strftime("%H:%M"),
                    "end": s.end_time.strftime("%H:%M"),
                    "role": s.role,
                    "break_minutes": getattr(s, "break_minutes", 0),
                    "venue_id": link_vid,
                    "venue_name": names[link_vid],
                })
        except Exception as e:
            logger.warning(f"my_shifts read failed for {link_emp.id}: {e}")
    mine.sort(key=lambda x: (x["date"], x["start"]))
    return {"linked": True, "employee_id": emp.id, "name": emp.name,
            "from": today.isoformat(), "to": horizon.isoformat(),
            "count": len(mine), "shifts": mine}


def _worked_minutes(t: dict):
    ci, co = t.get("clock_in"), t.get("clock_out")
    if not (ci and co):
        return None
    a = datetime.fromisoformat(ci) if isinstance(ci, str) else ci
    b = datetime.fromisoformat(co) if isinstance(co, str) else co
    if getattr(a, "tzinfo", None) is not None:
        a = a.replace(tzinfo=None)
    if getattr(b, "tzinfo", None) is not None:
        b = b.replace(tzinfo=None)
    return max(0, int((b - a).total_seconds() // 60) - int(t.get("break_minutes") or 0))


@router.get("/api/me/timesheets")
async def my_timesheets(user: UserContext = Depends(get_current_user)) -> dict:
    """My worked hours, last 14 days — pay transparency, every venue I work."""
    db = get_db()
    links = _linked_employees(db, user)
    if not links:
        return _no_link_response(user, db)
    names = _venue_names(db, [v for _, v in links])
    rows = []
    total = 0
    for emp, vid in links:
        end = venue_today(vid, db)
        start = end - timedelta(days=14)
        for t in db.get_timesheets(vid, start, end) or []:
            if t.get("employee_id") != emp.id:
                continue
            worked = _worked_minutes(t)
            if worked is not None:
                total += worked
            rows.append({
                "work_date": str(t.get("work_date")),
                "worked_minutes": worked,
                "status": t.get("status"),
                "venue_id": vid,
                "venue_name": names[vid],
            })
    rows.sort(key=lambda r: r["work_date"])
    return {"linked": True, "count": len(rows), "total_worked_minutes": total, "timesheets": rows}


@router.get("/api/me/leave")
async def my_leave(user: UserContext = Depends(get_current_user)) -> dict:
    db = get_db()
    links = _linked_employees(db, user)
    if not links:
        return _no_link_response(user, db)
    names = _venue_names(db, [v for _, v in links])
    requests = []
    for emp, vid in links:
        for r in db.list_leave_requests(vid) or []:
            if r.get("employee_id") != emp.id:
                continue
            requests.append({
                "id": r["id"], "start_date": str(r["start_date"]), "end_date": str(r["end_date"]),
                "reason": r.get("reason"), "status": r.get("status"),
                "leave_type": r.get("leave_type") or "other",
                "leave_type_label": leave_type_label(r.get("leave_type")),
                "decision_note": r.get("decision_note"),
                "venue_id": vid, "venue_name": names[vid],
            })
    return {"linked": True, "count": len(requests), "requests": requests}


def _as_date(v) -> date:
    return v if isinstance(v, date) else date.fromisoformat(str(v)[:10])


@router.post("/api/me/leave")
async def request_leave(body: LeaveRequestBody,
                        user: UserContext = Depends(get_current_user)) -> dict:
    """Request leave. Someone who works more than one venue picks which ones
    (``venue_ids``); left out, it goes to every venue they work."""
    db = get_db()
    links = _team_links(db, user)
    if body.end_date < body.start_date:
        raise HTTPException(status_code=422, detail="end_date must be on or after start_date")
    by_venue = {vid: emp for emp, vid in links}
    wanted = list(dict.fromkeys(body.venue_ids or by_venue.keys()))
    unknown = [v for v in wanted if v not in by_venue]
    if unknown or not wanted:
        raise HTTPException(status_code=422, detail="Pick the venue(s) you work at for this leave")
    names = _venue_names(db, wanted)
    for vid in wanted:
        if body.start_date < venue_today(vid, db):
            raise HTTPException(status_code=422, detail="Leave requests must be for future dates")
        emp = by_venue[vid]
        # One pending request per overlapping period keeps things sane
        for r in db.list_leave_requests(vid) or []:
            if (r.get("employee_id") == emp.id and r.get("status") == "pending"
                    and _as_date(r["start_date"]) <= body.end_date
                    and _as_date(r["end_date"]) >= body.start_date):
                where = f" at {names[vid]}" if len(by_venue) > 1 else ""
                raise HTTPException(status_code=409,
                                    detail=f"You already have a pending request overlapping those dates{where}")
    ids = []
    for vid in wanted:
        emp = by_venue[vid]
        req = {
            "id": f"lv-{uuid.uuid4().hex[:10]}",
            "venue_id": vid,
            "employee_id": emp.id,
            "start_date": body.start_date,
            "end_date": body.end_date,
            "reason": body.reason,
            "leave_type": body.leave_type,
            "status": "pending",
            "created_at": datetime.utcnow(),
        }
        db.save_leave_request(req)
        ids.append(req["id"])
        logger.info(f"Leave requested: {emp.name} {body.start_date}..{body.end_date} at {vid}")
    return {"status": "requested", "request_id": ids[0], "request_ids": ids,
            "venues": [names[v] for v in wanted]}


# ---------------------------------------------------------------------------
# Availability — staff set when they can/can't work; the roster generator
# already honours employee.availability, so this closes that loop from /my.
#   day absent from dict  -> available all day
#   day present, ranges [] -> unavailable all day
#   day present, ranges    -> available only in those windows
# Availability belongs to the person, not the venue: saving it writes every
# staff record the login is linked to.
# ---------------------------------------------------------------------------

@router.get("/api/me/availability")
async def my_availability(user: UserContext = Depends(get_current_user)) -> dict:
    db = get_db()
    links = _linked_employees(db, user)
    if not links:
        return _no_link_response(user, db)
    emp = links[0][0]
    avail = getattr(emp, "availability", {}) or {}
    days = []
    for d in _WEEKDAYS:
        windows = day_windows(avail, d)
        if windows is None:
            days.append({"day": d, "status": "available", "ranges": []})
        elif not windows:
            days.append({"day": d, "status": "unavailable", "ranges": []})
        else:
            days.append({"day": d, "status": "partial", "ranges": windows})
    return {"linked": True, "days": days}


@router.post("/api/me/availability")
async def set_my_availability(body: AvailabilityBody,
                              user: UserContext = Depends(get_current_user)) -> dict:
    db = get_db()
    links = _team_links(db, user)
    emp, vid = links[0]

    avail = dict(getattr(emp, "availability", {}) or {})
    for day, spec in (body.days or {}).items():
        d = str(day).strip().lower()
        if d not in _WEEKDAYS:
            raise HTTPException(status_code=422, detail=f"Unknown day '{day}'")
        status = spec.status.lower()  # pydantic already validated the enum
        if status == "available":
            avail.pop(d, None)
        elif status == "unavailable":
            avail[d] = []
        else:  # partial
            if not spec.ranges:
                raise HTTPException(status_code=422,
                                    detail=f"{d}: 'partial' needs at least one time range")
            clean = []
            for r in spec.ranges:
                s_min, e_min = _parse_hhmm(r.start.strip()), _parse_hhmm(r.end.strip())
                # Minute-precise: 17:30–17:45 is valid; 17:00–17:00 is not.
                if s_min >= e_min:
                    raise HTTPException(status_code=422,
                                        detail=f"{d}: range start {r.start.strip()} must be before end {r.end.strip()}")
                # Stored as HH:MM, whatever was typed ("9" -> "09:00").
                clean.append({"start": f"{s_min // 60:02d}:{s_min % 60:02d}",
                              "end": f"{e_min // 60:02d}:{e_min % 60:02d}"})
            avail[d] = clean

    now = datetime.utcnow()
    for link_emp, link_vid in links:
        link_emp.availability = dict(avail)
        link_emp.updated_at = now
        db.save_employee(link_emp)
        logger.info(f"Availability updated: {link_emp.name} at {link_vid}")
    return {"status": "saved", "days_set": list((body.days or {}).keys()),
            "venues": len(links)}


# ---------------------------------------------------------------------------
# Shift cover — staff side
# ---------------------------------------------------------------------------

def _find_my_shift(db, venue_id: str, employee_id: str, shift_id: str):
    """The named future shift, only if it belongs to this employee."""
    today = venue_today(venue_id, db)
    try:
        shifts = live_shifts(db, venue_id, today, today + timedelta(days=60), staff=True)
    except Exception:
        shifts = []
    for s in shifts:
        if getattr(s, "id", None) == shift_id:
            if str(getattr(s, "employee_id", "")) != str(employee_id):
                raise HTTPException(status_code=403, detail="You can only offer your own shifts for cover")
            return s
    raise HTTPException(status_code=404, detail="Shift not found in your upcoming roster")


def _cover_payload(c, emp_names: dict, staff: bool = False, standing=None) -> dict:
    if c.get("origin") == "open" or (staff and c.get("origin") == "leave"):
        # An open shift has nobody behind it to name; staff never see whose
        # leave left one open.
        c = dict(c, requested_by=None, reason="")
        emp_names = {**emp_names, None: "Open shift"}
    if standing is not None and c.get("status") in LIVE_COVER and c.get("id") not in standing:
        c = dict(c, status="withdrawn")
    return {
        "id": c["id"],
        "shift_id": c["shift_id"],
        "shift_date": str(c["shift_date"]),
        "shift_start": c.get("shift_start"),
        "shift_end": c.get("shift_end"),
        "role": c.get("role"),
        "requested_by": c["requested_by"],
        "requested_by_name": emp_names.get(c["requested_by"], c["requested_by"]),
        "reason": c.get("reason"),
        "claimed_by": c.get("claimed_by"),
        "claimed_by_name": emp_names.get(c.get("claimed_by"), c.get("claimed_by")) if c.get("claimed_by") else None,
        "status": c.get("status"),
        "origin": c.get("origin") or "staff",
        "decision_note": c.get("decision_note"),
    }


@router.post("/api/me/shifts/{shift_id}/cover")
async def offer_shift_for_cover(shift_id: str, body: CoverRequestBody,
                                user: UserContext = Depends(get_current_user)) -> dict:
    """Put one of my upcoming shifts up for a co-worker to cover."""
    db = get_db()
    links = _team_links(db, user)
    emp = vid = shift = None
    for link_emp, link_vid in links:
        try:
            shift = _find_my_shift(db, link_vid, link_emp.id, shift_id)
        except HTTPException as exc:
            if exc.status_code == 404:
                continue
            raise
        emp, vid = link_emp, link_vid
        break
    if shift is None:
        raise HTTPException(status_code=404, detail="Shift not found in your upcoming roster")
    for c in db.list_shift_covers(vid) or []:
        if c.get("shift_id") == shift_id and c.get("status") in ("open", "claimed"):
            raise HTTPException(status_code=409, detail="That shift is already up for cover")
    cover = {
        "id": f"cv-{uuid.uuid4().hex[:10]}",
        "venue_id": vid,
        "shift_id": shift_id,
        "shift_date": shift.date,
        "shift_start": shift.start_time.strftime("%H:%M"),
        "shift_end": shift.end_time.strftime("%H:%M"),
        "role": shift.role,
        "requested_by": emp.id,
        "reason": body.reason,
        "claimed_by": None,
        "status": "open",
        "created_at": datetime.utcnow(),
    }
    db.save_shift_cover(cover)
    logger.info(f"Cover requested: {emp.name} for shift {shift_id} at {vid}")
    return {"status": "open", "cover_id": cover["id"]}


@router.get("/api/me/cover")
async def cover_board(user: UserContext = Depends(get_current_user)) -> dict:
    """The cover board: shifts up for grabs at my venue(s), plus my own
    requests and anything I've claimed."""
    db = get_db()
    links = _linked_employees(db, user)
    if not links:
        return _no_link_response(user, db)
    links = _on_team(links)
    from rosteriq.services.cover_checks import can_take, on_leave_on
    names = _venue_names(db, [v for _, v in links])
    up_for_grabs, mine, claimed_by_me = [], [], []
    for emp, vid in links:
        emp_names = {e.id: e.name for e in (db.get_employees(vid) or [])}
        venue = db.get_venue(vid)
        covers = db.list_shift_covers(vid) or []
        standing = standing_cover_ids(db, vid, covers)
        leave = edit.approved_leave(db, vid)
        for c in covers:
            p = _cover_payload(c, emp_names, staff=True, standing=standing)
            p["venue_id"], p["venue_name"] = vid, names[vid]
            if c.get("requested_by") == emp.id:
                if c.get("origin") != "leave":
                    mine.append(p)
            elif c.get("claimed_by") == emp.id and c.get("status") in ("claimed", "approved"):
                claimed_by_me.append(p)
            elif (c.get("status") == "open" and c.get("id") in standing and is_active(emp)
                  and can_take(emp, c.get("role"), venue)
                  and not on_leave_on(db, vid, emp, cover_day(c), leave)):
                up_for_grabs.append(p)
    return {"linked": True, "up_for_grabs": up_for_grabs, "mine": mine,
            "claimed_by_me": claimed_by_me}


def _span_minutes(start, end) -> tuple:
    def m(t):
        if isinstance(t, str):
            hh, mm = t.strip()[:5].split(":")
            return int(hh) * 60 + int(mm)
        return t.hour * 60 + t.minute
    s, e = m(start), m(end)
    return s, (e if e > s else e + 24 * 60)      # an overnight shift runs past midnight


def _clashing_shift(db, venue_id: str, employee_id: str, cover: dict):
    """The employee's own shift at this venue that overlaps the cover's shift,
    if any — taking the cover would double-book them."""
    day = cover.get("shift_date")
    if not isinstance(day, date):
        day = date.fromisoformat(str(day)[:10])
    s, e = _span_minutes(cover.get("shift_start") or "00:00", cover.get("shift_end") or "23:59")
    for roster in live_rosters(db, venue_id, day, day, staff=True):
        if getattr(roster, "venue_id", None) != venue_id:
            continue
        for sh in roster.shifts or []:
            if sh.id == cover.get("shift_id") or sh.employee_id != employee_id or sh.date != day:
                continue
            if str(getattr(sh.status, "value", sh.status)) in ("cancelled", "no_show"):
                continue
            a, b = _span_minutes(sh.start_time, sh.end_time)
            if a < e and s < b:
                return sh
    return None


def _live_shift(db, venue_id: str, cover: dict):
    """The cover's shift, if it is still on the roster staff see for that
    day (a republish replaces the week's shifts)."""
    try:
        day = cover.get("shift_date")
        if not isinstance(day, date):
            day = date.fromisoformat(str(day)[:10])
    except ValueError:
        return None
    for sh in live_shifts(db, venue_id, day, day, staff=True):
        if sh.id == cover.get("shift_id"):
            return sh
    return None


def _clash_detail(sh, whose: str) -> str:
    return (f"{whose} already rostered {sh.start_time.strftime('%H:%M')}\u2013"
            f"{sh.end_time.strftime('%H:%M')} that day, which overlaps this shift")


def _cover_link(db, user: UserContext, cover_id: str):
    """(cover, my record at its venue) — or 404 when the cover isn't at a
    venue I work."""
    links = _team_links(db, user)
    cover = db.get_shift_cover(cover_id)
    emp = None
    if cover:
        emp = next((e for e, v in links if v == cover.get("venue_id")), None)
    if not cover or emp is None:
        raise HTTPException(status_code=404, detail="Cover request not found")
    return cover, emp, [v for _, v in links]


def _busy_elsewhere_detail(db, emp, cover: dict, scope=None, published_only=False) -> Optional[str]:
    """'on at Apothecary 16:00-23:00' when taking the cover would double-book
    this person at another venue, else None."""
    from rosteriq.services.person_identity import busy_elsewhere
    try:
        day = cover.get("shift_date")
        day = day if isinstance(day, date) else date.fromisoformat(str(day)[:10])
        hits = busy_elsewhere(db, emp, day, cover.get("shift_start"), cover.get("shift_end"),
                              scope=scope, published_only=published_only)
    except Exception as e:  # noqa: BLE001
        logger.warning(f"cross-venue check failed for cover {cover.get('id')}: {e}")
        return None
    if not hits:
        return None
    h = hits[0]
    return f"on at {h['venue_name']} {h['start']}-{h['end']} that day, which overlaps this shift"


@router.post("/api/me/cover/{cover_id}/claim")
async def claim_cover(cover_id: str, user: UserContext = Depends(get_current_user)) -> dict:
    """Claim a co-worker's open shift — goes to the manager for approval."""
    db = get_db()
    cover, emp, my_venues = _cover_link(db, user, cover_id)
    vid = cover.get("venue_id")
    if cover.get("requested_by") == emp.id:
        raise HTTPException(status_code=409, detail="You can't claim your own shift")
    if not is_active(emp):
        raise HTTPException(status_code=403,
                            detail="Your staff record here is no longer active — ask your manager")
    if cover.get("status") != "open":
        raise HTTPException(status_code=409, detail=f"This shift is already {cover.get('status')}")
    live = _live_shift(db, vid, cover)
    if live is None:
        raise HTTPException(status_code=409, detail="That shift is no longer on the roster \u2014 it may have been republished")
    if not cover_stands(cover, live):
        raise HTTPException(status_code=409, detail="That shift has changed on the roster since it "
                                                    "was put up for cover, so it's no longer up for grabs")
    from rosteriq.services.cover_checks import can_take, not_trained_message, on_leave_on
    if not can_take(emp, cover.get("role"), db.get_venue(vid)):
        raise HTTPException(status_code=409, detail=not_trained_message(cover.get("role")))
    if on_leave_on(db, vid, emp, cover_day(cover)):
        raise HTTPException(status_code=409, detail="You're on approved leave that day")
    clash = _clashing_shift(db, vid, emp.id, cover)
    if clash is not None:
        raise HTTPException(status_code=409, detail=_clash_detail(clash, "You're"))
    elsewhere = _busy_elsewhere_detail(db, emp, cover, scope=my_venues, published_only=True)
    if elsewhere:
        raise HTTPException(status_code=409, detail="You're " + elsewhere)
    if not db.claim_shift_cover(cover_id, emp.id):
        raise HTTPException(status_code=409, detail="Someone else has just claimed this shift")
    logger.info(f"Cover claimed: {emp.name} claims {cover_id} at {vid}")
    return {"status": "claimed", "cover_id": cover_id,
            "message": "Claimed — your manager still needs to approve the change"}


@router.post("/api/me/cover/{cover_id}/cancel")
async def cancel_cover(cover_id: str, user: UserContext = Depends(get_current_user)) -> dict:
    """Requester withdraws their cover request before it's decided."""
    db = get_db()
    cover, emp, _ = _cover_link(db, user, cover_id)
    vid = cover.get("venue_id")
    if cover.get("requested_by") != emp.id:
        raise HTTPException(status_code=403, detail="Only the requester can cancel this")
    if cover.get("status") not in ("open", "claimed"):
        raise HTTPException(status_code=409, detail=f"Already {cover.get('status')} — ask your manager")
    shift = _live_shift(db, vid, cover)
    if cover.get("origin") == "leave" or (shift is not None and is_open(shift)):
        raise HTTPException(status_code=409, detail="This shift was left open by approved leave "
                                                    "\u2014 it stays up for cover; ask your manager")
    cover["status"] = "cancelled"
    db.save_shift_cover(cover)
    return {"status": "cancelled", "cover_id": cover_id}


# ---------------------------------------------------------------------------
# Manager-facing
# ---------------------------------------------------------------------------

@router.get("/api/leave")
async def venue_leave(venue_id: str = Query(...), status: str = Query("")) -> dict:
    enforce_venue_manager_any(venue_id)
    db = get_db()
    emp_names = {e.id: e.name for e in (db.get_employees(venue_id) or [])}
    rows = db.list_leave_requests(venue_id) or []
    if status:
        rows = [r for r in rows if r.get("status") == status]
    return {"venue_id": venue_id, "count": len(rows), "requests": [{
        "id": r["id"],
        "employee_id": r["employee_id"],
        "employee_name": emp_names.get(r["employee_id"], r["employee_id"]),
        "start_date": str(r["start_date"]), "end_date": str(r["end_date"]),
        "reason": r.get("reason"), "status": r.get("status"),
        "leave_type": r.get("leave_type") or "other",
        "leave_type_label": leave_type_label(r.get("leave_type")),
        "paid_hours": r.get("paid_hours"),
        "entered_by_manager": bool(r.get("entered_by")),
        "opened_shifts": [o.get("label") for o in (r.get("opened_shifts") or [])],
        "decided_by": r.get("decided_by"), "decision_note": r.get("decision_note"),
    } for r in rows]}


@router.get("/api/availability")
async def team_availability(venue_id: str = Query(...)) -> dict:
    """Manager view: the whole team's weekly availability at a glance, so a
    manager knows who's off before building the roster. Same source the
    generator reads (employee.availability)."""
    enforce_venue_manager_any(venue_id)
    db = get_db()
    rows = []
    for emp in (db.get_employees(venue_id) or []):
        if not is_active(emp):
            continue
        avail = getattr(emp, "availability", {}) or {}
        days = {}
        constrained = []
        for d in _WEEKDAYS:
            windows = day_windows(avail, d)
            if windows is None:
                days[d] = "available"
            elif not windows:
                days[d] = "unavailable"
                constrained.append(d[:3])
            else:
                days[d] = "partial"
                constrained.append(d[:3])
        rows.append({
            "employee_id": emp.id,
            "name": emp.name,
            "days": days,
            "constrained_days": constrained,  # non-fully-available days
        })
    rows.sort(key=lambda r: (-len(r["constrained_days"]), r["name"]))
    return {"venue_id": venue_id, "count": len(rows), "staff": rows}


@router.get("/api/cover")
async def venue_covers(venue_id: str = Query(...), status: str = Query("")) -> dict:
    """Manager view of the venue's shift cover requests."""
    enforce_venue_manager_any(venue_id)
    db = get_db()
    emp_names = {e.id: e.name for e in (db.get_employees(venue_id) or [])}
    rows = db.list_shift_covers(venue_id) or []
    standing = standing_cover_ids(db, venue_id, rows)
    covers = [_cover_payload(c, emp_names, standing=standing) for c in rows]
    if status:
        covers = [p for p in covers if p["status"] == status]
    return {"venue_id": venue_id, "count": len(covers), "covers": covers}


def _reassign_shift(db, venue_id: str, cover: dict, new_employee_id: str) -> bool:
    """Move the cover's shift to its new owner, priced for them: on the roster
    staff see, and on any other roster of the week not yet superseded (a newer
    draft) where the same slot is still with whoever held it. Each roster is
    re-read and saved under its lock; RosterBusy when another save holds the
    lock of the roster staff see. Returns False, changing nothing, if the
    shift there has gone or changed since the cover was posted."""
    day = cover_day(cover)
    staff = live_rosters(db, venue_id, day, day, staff=True) if day else []
    if not staff:
        return False
    emp_map = scoped_employee_map(db, venue_id)
    state = edit.venue_state(db.get_venue(venue_id))

    def move(roster_id, pick):
        with edit.roster_lock(db, roster_id):
            roster = db.get_roster(roster_id)
            if roster is None or getattr(roster, "venue_id", None) != venue_id:
                return None
            roster = roster.model_copy(deep=True)
            target = pick(roster.shifts)
            if target is None:
                return None
            slot = shift_slot(target)
            target.employee_id = new_employee_id
            _reprice(db, venue_id, target, new_employee_id)
            edit.recost_total(roster, emp_map, state)
            db.save_roster(roster)
            return slot

    slot = move(staff[0].id, lambda shifts: next(
        (s for s in shifts if s.id == cover.get("shift_id") and cover_stands(cover, s)), None))
    if slot is None:
        return False
    # The requester's own copy of the slot first: an open copy may be someone
    # else's leave, and only stands in when the requester's leave opened theirs.
    holders = [h for h in (str(cover.get("requested_by") or ""), "") if h in cover_holders(cover)]

    def same_slot(shifts):
        return next((s for h in holders for s in shifts
                     if shift_slot(s) == slot and str(s.employee_id or "") == h), None)

    for r in unsuperseded_rosters(db, venue_id, day, day):
        if r.id == staff[0].id:
            continue
        try:
            move(r.id, same_slot)
        except edit.RosterBusy:
            logger.warning(f"Roster {r.id} busy: cover {cover.get('id')} not applied to it")
    return True


def _reprice(db, venue_id: str, shift, employee_id: str) -> None:
    try:
        from rosteriq.cost_calculator import calculate_shift_cost_breakdown
        emp = db.get_employee(employee_id)
        venue = db.get_venue(venue_id)
        if emp is not None and venue is not None:
            shift.cost = calculate_shift_cost_breakdown(emp, shift, venue.state).total_cost
    except Exception as e:  # the move matters more than the cached price
        logger.warning(f"Could not reprice shift {shift.id}: {e}")


@router.post("/api/cover/{cover_id}/decide")
async def decide_cover(cover_id: str, body: CoverDecision,
                       user: UserContext = Depends(get_current_user)) -> dict:
    """Approve (reassigns the shift on the roster) or decline a claimed cover."""
    enforce_venue_manager(body.venue_id)
    db = get_db()
    cover = db.get_shift_cover(cover_id)
    if not cover or cover.get("venue_id") != body.venue_id:
        raise HTTPException(status_code=404, detail="Cover request not found")
    if cover.get("status") == "open":
        raise HTTPException(status_code=409, detail="Nobody has claimed this shift yet")
    if cover.get("status") != "claimed":
        raise HTTPException(status_code=409, detail=f"Request is already {cover.get('status')}")

    claimant_id = cover.get("claimed_by")
    checks = None
    if body.approve:
        claimant = db.get_employee(claimant_id) if claimant_id else None
        if claimant is not None and not is_active(claimant):
            raise HTTPException(status_code=409, detail=f"{claimant.name} is no longer active "
                                "here — decline this request instead.")
        if not cover_stands(cover, _live_shift(db, body.venue_id, cover)):
            _withdraw_changed(db, cover, cover_id, claimant_id, user)
        clash = _clashing_shift(db, body.venue_id, claimant_id, cover)
        if clash is not None:
            raise HTTPException(status_code=409, detail=_clash_detail(
                clash, _emp_name(db, body.venue_id, claimant_id) + " is") + " \u2014 decline this request instead.")
        checks = _run_cover_checks(db, body.venue_id, cover)
        if checks["blocked"]:
            raise HTTPException(status_code=409, detail="; ".join(checks["block_reasons"])
                                + " \u2014 decline this request instead.")
        # The roster is the source of truth — a cover is only approved if the
        # shift genuinely moves. Fail loud if the roster changed underneath us.
        try:
            moved = _reassign_shift(db, body.venue_id, cover, claimant_id)
        except edit.RosterBusy:
            raise HTTPException(status_code=409, detail="Someone else is saving this roster "
                                                        "right now — try again in a moment.")
        if not moved:
            _withdraw_changed(db, cover, cover_id, claimant_id, user)
        cover["status"] = "approved"
    else:
        # Declined: shift stays with the requester; reopen for other claimants
        cover["status"] = "open"
        cover["claimed_by"] = None
    cover["decided_by"] = user.user_id
    cover["decided_at"] = datetime.utcnow()
    cover["decision_note"] = body.note
    if not body.approve:
        db.save_shift_cover(cover)
        _audit_cover_decision(db, cover, cover_id, "decline", claimant_id, body.note)
        return {"status": "reopened", "cover_id": cover_id}
    db.save_shift_cover(cover)
    _audit_cover_decision(db, cover, cover_id, "approve", claimant_id, body.note,
                          warnings=checks["warnings"])
    logger.info(f"Cover approved: shift {cover['shift_id']} -> {cover['claimed_by']} at {body.venue_id}")
    return {"status": "approved", "cover_id": cover_id, "checks": checks["checks"],
            "warnings": checks["warnings"]}


_CHANGED = ("That shift has changed on the roster since this cover was posted (it was "
            "filled, moved, retimed or removed), so the cover request has been withdrawn.")


def _withdraw_changed(db, cover: dict, cover_id: str, claimant_id, user: UserContext) -> None:
    withdraw_cover(db, cover, _CHANGED, user.user_id)
    _audit_cover_decision(db, cover, cover_id, "withdraw", claimant_id, _CHANGED)
    raise HTTPException(status_code=409, detail=_CHANGED)


def _run_cover_checks(db, venue_id: str, cover: dict) -> dict:
    from rosteriq.services.cover_checks import cover_checks
    claimant = db.get_employee(cover.get("claimed_by")) if cover.get("claimed_by") else None
    if claimant is None or getattr(claimant, "venue_id", None) != venue_id:
        raise HTTPException(status_code=409, detail="The person who claimed this shift is no "
                                                    "longer on this venue's staff \u2014 decline it instead.")
    return cover_checks(db, venue_id, cover, claimant, _live_shift(db, venue_id, cover))


def _emp_name(db, venue_id: str, employee_id) -> str:
    """Employee display name for the event log (id if not resolvable)."""
    try:
        emp = db.get_employee(employee_id) if employee_id else None
        return getattr(emp, "name", None) or str(employee_id)
    except Exception:
        return str(employee_id)


def _audit_cover_decision(db, cover: dict, cover_id: str, decision: str,
                          claimant_id, note: str, warnings=None) -> None:
    extra = {"warnings": warnings} if warnings else {}
    audit("cover.decide", cover.get("venue_id"), "shift_cover", cover_id,
          decision=decision, status=cover.get("status"),
          shift_id=cover.get("shift_id"), shift_date=str(cover.get("shift_date")),
          shift_start=cover.get("shift_start"), shift_end=cover.get("shift_end"),
          requested_by=cover.get("requested_by"),
          requested_by_name=_emp_name(db, cover.get("venue_id"), cover.get("requested_by")),
          claimed_by=claimant_id,
          claimed_by_name=_emp_name(db, cover.get("venue_id"), claimant_id),
          note=note, **extra)


@router.post("/api/leave/{req_id}/decide")
async def decide_leave(req_id: str, body: LeaveDecision,
                       user: UserContext = Depends(get_current_user)) -> dict:
    enforce_venue_manager(body.venue_id)
    db = get_db()
    req = db.get_leave_request(req_id)
    if not req or req.get("venue_id") != body.venue_id:
        raise HTTPException(status_code=404, detail="Leave request not found")
    if req.get("status") != "pending":
        raise HTTPException(status_code=409, detail=f"Request is already {req.get('status')}")
    req["status"] = "approved" if body.approve else "declined"
    req["decided_by"] = user.user_id
    req["decided_at"] = datetime.utcnow()
    req["decision_note"] = body.note
    if body.leave_type:
        req["leave_type"] = body.leave_type
    if body.paid_hours is not None:
        req["paid_hours"] = body.paid_hours
    opened = []
    if body.approve:
        emp = db.get_employee(req.get("employee_id"))
        if emp is not None and getattr(emp, "venue_id", None) == body.venue_id:
            try:
                opened = apply_approved_leave(db, body.venue_id, req, emp)
            except edit.RosterBusy:
                raise HTTPException(status_code=409, detail="Someone else is saving the roster "
                                                            "right now — try again in a moment.")
    db.save_leave_request(req)
    audit("leave.decide", body.venue_id, "leave_request", req_id,
          decision="approve" if body.approve else "decline", status=req["status"],
          employee_id=req.get("employee_id"),
          employee=_emp_name(db, body.venue_id, req.get("employee_id")),
          start_date=str(req.get("start_date")), end_date=str(req.get("end_date")),
          leave_type=req.get("leave_type") or "other", paid_hours=req.get("paid_hours"),
          opened_shifts=[o["label"] for o in opened],
          reason=req.get("reason"), note=body.note)
    return {"status": req["status"], "request_id": req_id,
            "opened_shifts": opened,
            "message": _opened_message(opened)}


def _opened_message(opened: list) -> str:
    if not opened:
        return "No upcoming shifts to cover."
    return opened_summary(opened)
