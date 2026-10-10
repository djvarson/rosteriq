"""
What a login may do at each venue it reaches.

A login's dashboard role is one field, but people who work two of an owner's
venues need different access at each (an assistant venue manager at The
Wembley Hotel who picks up shifts at Apothecary). The account record already
carries what that needs:

    venue_ids         venues the login can open in the dashboard. For a
                      manager login each is FULL (no section grant) or
                      DEPARTMENT (section_grants[venue] = the sections).
    staff_venue_ids   venues where a MANAGER login is linked to a staff
                      record only: their shifts, news and leave there reach
                      /my, but every venue-scoped dashboard gate refuses
                      them, because the venue is not in venue_ids.

A staff-role login holds all its venues in venue_ids, as staff.

access_map / apply_access_map translate between that storage and one access
level per venue, so callers never edit the three fields by hand.
"""

from __future__ import annotations

from typing import Dict, Iterable, List, Optional

FULL = "full"
DEPARTMENT = "department"
STAFF = "staff"
LEVELS = (FULL, DEPARTMENT, STAFF)

_DEFAULT_SECTIONS = ["kitchen", "bar", "floor"]


def _role(account) -> str:
    role = account.get("role") if isinstance(account, dict) else getattr(account, "role", None)
    return str(getattr(role, "value", role) or "")


def _get(account, key, default):
    if isinstance(account, dict):
        value = account.get(key)
    else:
        value = getattr(account, key, None)
    return default if value is None else value


def is_platform_owner(account) -> bool:
    return _role(account) == "owner" or bool(_get(account, "is_owner", False))


def is_manager_login(account) -> bool:
    return _role(account) in ("manager", "owner") or is_platform_owner(account)


def held_venue_ids(account) -> List[str]:
    """Every venue the login reaches, dashboard or staff-only, in order."""
    out = list(_get(account, "venue_ids", []) or [])
    for v in _get(account, "staff_venue_ids", []) or []:
        if v not in out:
            out.append(v)
    return out


def fully_manages(user, venue_id: Optional[str]) -> bool:
    """True when ``user`` (a UserContext, TenantContext or account dict) is a
    FULL manager of ``venue_id``: a platform owner, or a manager login that
    holds the venue with no department restriction there."""
    if not venue_id:
        return False
    if is_platform_owner(user):
        return True
    if _role(user) not in ("manager", "owner"):
        return False
    if venue_id not in (_get(user, "venue_ids", []) or []):
        return False
    return venue_id not in (_get(user, "section_grants", {}) or {})


def access_map(account) -> Dict[str, dict]:
    """venue_id -> {"level": full|department|staff, "sections": [...]}."""
    out: Dict[str, dict] = {}
    grants = _get(account, "section_grants", {}) or {}
    manager = is_manager_login(account)
    for v in _get(account, "venue_ids", []) or []:
        if not manager:
            out[v] = {"level": STAFF, "sections": []}
        elif v in grants:
            out[v] = {"level": DEPARTMENT, "sections": list(grants.get(v) or [])}
        else:
            out[v] = {"level": FULL, "sections": []}
    for v in _get(account, "staff_venue_ids", []) or []:
        out.setdefault(v, {"level": STAFF, "sections": []})
    return out


def visible_to(user, account: dict) -> dict:
    """A copy of ``account`` that reaches only the venues ``user`` fully
    manages (every venue, for the platform owner). Describe a login through
    this to a caller who may run only part of it."""
    seen = {v for v in held_venue_ids(account) if fully_manages(user, v)}
    view = dict(account)
    view["venue_ids"] = [v for v in _get(account, "venue_ids", []) or [] if v in seen]
    view["staff_venue_ids"] = [v for v in _get(account, "staff_venue_ids", []) or [] if v in seen]
    view["section_grants"] = {v: s for v, s in (_get(account, "section_grants", {}) or {}).items()
                              if v in seen}
    return view


def apply_access_map(account: dict, amap: Dict[str, dict]) -> dict:
    """Write one access level per venue back onto the account record.

    Any FULL or DEPARTMENT venue makes the login a manager, and its staff
    venues become staff-only (outside venue_ids, so no dashboard there).
    With no managed venue left it is a plain staff login holding them all.
    A DEPARTMENT level with no sections is refused rather than stored, since
    an empty grant would leave a manager login that manages nothing. Venues
    missing from ``amap`` keep their current level."""
    held = held_venue_ids(account)
    merged = access_map(account)
    merged.update(amap)
    order = held + [v for v in merged if v not in held]
    managed, staff_only, grants = [], [], {}
    for v in order:
        level = (merged.get(v) or {}).get("level")
        if level not in LEVELS:
            raise ValueError(f"Unknown access level {level!r}")
        if level == DEPARTMENT:
            sections = [s for s in (merged[v].get("sections") or []) if s]
            if not sections:
                raise ValueError("A department manager needs at least one section")
            grants[v] = sections
            managed.append(v)
        elif level == FULL:
            managed.append(v)
        else:
            staff_only.append(v)
    if managed:
        account["role"] = "manager"
        account["venue_ids"] = managed
        account["staff_venue_ids"] = staff_only
        account["section_grants"] = grants
    else:
        account["role"] = "staff"
        account["venue_ids"] = staff_only
        account["staff_venue_ids"] = []
        account["section_grants"] = {}
    return account


def drop_venue(account: dict, venue_id: str) -> dict:
    """Take ``venue_id`` off the login, dashboard or staff-only, leaving its
    access everywhere else as it was. A manager login left managing nothing
    becomes a staff login holding its staff venues (apply_access_map's
    rule)."""
    managed = [v for v in _get(account, "venue_ids", []) or [] if v != venue_id]
    staff_only = [v for v in _get(account, "staff_venue_ids", []) or []
                  if v != venue_id and v not in managed]
    grants = {v: s for v, s in (_get(account, "section_grants", {}) or {}).items() if v != venue_id}
    if _role(account) == "manager" and not managed:
        account["role"] = "staff"
        managed, staff_only, grants = staff_only, [], {}
    account["venue_ids"] = managed
    account["staff_venue_ids"] = staff_only
    account["section_grants"] = grants
    return account


def same_owner_runs(db, venue_ids: Iterable[str], exclude_user_id: Optional[str] = None) -> bool:
    """True when one venue-owning login (not the platform owner) is a FULL
    manager of every venue in ``venue_ids`` — the venues belong to one
    operator, e.g. Ben's two pubs."""
    wanted = [v for v in dict.fromkeys(venue_ids) if v]
    if not wanted:
        return False
    try:
        users = db.list_users() or []
    except Exception:
        return False
    for u in users:
        if not u or u.get("id") == exclude_user_id or not u.get("is_active", True):
            continue
        if is_platform_owner(u) or _role(u) != "manager":
            continue
        if all(fully_manages(u, v) for v in wanted):
            return True
    return False


def same_owner_venue_ids(db, venue_id: str, *, strict: bool = False) -> List[str]:
    """The other venues for which same_owner_runs(db, [venue_id, v]) holds:
    those a venue-owning login fully manages together with ``venue_id``.
    With ``strict``, logins that can't be read raise instead of reading as
    "no other venues"."""
    out: List[str] = []
    if not venue_id:
        return out
    try:
        users = db.list_users() or []
    except Exception:
        if strict:
            raise
        users = []
    for u in users:
        if not u or not u.get("is_active", True) or is_platform_owner(u) or _role(u) != "manager":
            continue
        if fully_manages(u, venue_id):
            out += [v for v in u.get("venue_ids") or [] if v != venue_id and fully_manages(u, v)]
    return list(dict.fromkeys(out))


def fully_managed_group(db, user, venue_id: str) -> List[str]:
    """``venue_id`` plus the other venues ``user`` fully manages alongside it,
    i.e. one owner's business as this caller sees it. For the platform owner
    (who manages everything) the group is the venues some venue-owning login
    fully manages together with ``venue_id``, never every tenant at once."""
    out = [venue_id]
    if not is_platform_owner(user):
        if fully_manages(user, venue_id):
            out += [v for v in _get(user, "venue_ids", []) or []
                    if v != venue_id and fully_manages(user, v)]
        return list(dict.fromkeys(out))
    return out + same_owner_venue_ids(db, venue_id)


def created_venue_ids(db, user_id: str, venue_ids: Iterable[str]) -> List[str]:
    """Which of ``venue_ids`` this login created (the first-venue bootstrap
    audit event, which is never pruned)."""
    out = []
    page = 500
    for v in venue_ids:
        offset = 0
        found = False
        while not found:
            try:
                rows = db.list_events(venue_id=v, category="audit", action_prefix="user.venue_grant",
                                      limit=page, offset=offset) or []
            except Exception:
                rows = []
            for r in rows:
                details = r.get("details") or {}
                if (str(r.get("resource_id")) == str(user_id)
                        and details.get("reason") == "first_venue_bootstrap"):
                    found = True
                    break
            if len(rows) < page:
                break
            offset += page
        if found:
            out.append(v)
    return out


def _signup_creators(db, venue_id: str, strict: bool) -> Optional[set]:
    """The logins that created ``venue_id`` through self-serve signup (the
    first-venue bootstrap audit event); empty for a venue RosterIQ support
    set up, None when the audit log can't be read."""
    out: set = set()
    page, offset = 500, 0
    while True:
        try:
            rows = db.list_events(venue_id=venue_id, category="audit", action_prefix="user.venue_grant",
                                  limit=page, offset=offset) or []
        except Exception:
            if strict:
                raise
            return None
        out |= {str(r.get("resource_id")) for r in rows
                if (r.get("details") or {}).get("reason") == "first_venue_bootstrap"}
        if len(rows) < page:
            return out
        offset += page


def separate_signups(db, venue_a: str, venue_b: str, *, strict: bool = False) -> bool:
    """True when different logins signed ``venue_a`` and ``venue_b`` up
    themselves, or that can't be read. Two signups are two businesses,
    whoever manages both: a manager's own venue isn't their employer's, and
    a login that holds several clients' venues doesn't join them."""
    a = _signup_creators(db, venue_a, strict)
    b = _signup_creators(db, venue_b, strict)
    if a is None or b is None:
        return True
    return bool(a) and bool(b) and not (a & b)


def venue_names(db, venue_ids: Iterable[str]) -> List[str]:
    names = []
    for v in venue_ids:
        try:
            venue = db.get_venue(v)
        except Exception:
            venue = None
        names.append(getattr(venue, "name", None) or v)
    return names


def section_options(venue) -> List[str]:
    """The sections a department manager can be given at ``venue``."""
    try:
        from rosteriq.services.venue_areas import area_names
        names = area_names(venue)
    except Exception:
        names = []
    if not names:
        names = list((getattr(venue, "min_staff", None) or {}).keys())
    return names or list(_DEFAULT_SECTIONS)


def role_label(db, account) -> str:
    """How the dashboard names this login: 'Owner (The Wembley Hotel,
    Apothecary)' for a login that created one of its venues and fully manages
    all of them, otherwise Manager / Department manager / Staff."""
    if not account:
        return ""
    if is_platform_owner(account):
        return "RosterIQ support"
    held = held_venue_ids(account)
    names = venue_names(db, held)
    where = f" ({', '.join(names)})" if names else ""
    if not is_manager_login(account):
        return "Staff" + where
    amap = access_map(account)
    levels = {a["level"] for a in amap.values()}
    if levels == {FULL} and created_venue_ids(db, account.get("id"), held):
        return "Owner" + where
    if FULL in levels:
        managed = [n for v, n in zip(held, names) if amap.get(v, {}).get("level") == FULL]
        return "Manager (" + ", ".join(managed) + ")"
    if DEPARTMENT in levels:
        dept = [n for v, n in zip(held, names) if amap.get(v, {}).get("level") == DEPARTMENT]
        return "Department manager (" + ", ".join(dept) + ")"
    return "Staff" + where
