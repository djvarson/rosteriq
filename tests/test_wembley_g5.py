"""
Wembley pilot: managers, leavers and staff-only links.

* An owner or full manager of a venue can always lower a login's access AT
  THAT VENUE, and then remove the person's details there, even when the login
  also reaches a venue the caller doesn't run. Reset links and raising access
  still need a manager of every venue the login reaches, and nobody lowers a
  venue's creator at the venue they created.
* A manager someone else appointed can't add venues of their own (that is
  how a manager used to get out of the owner's reach); the owner still can.
* A leaver (record set inactive) loses the venue's team surfaces: feed,
  notices, procedures, staff list, leave, availability, cover. Their own
  history stays theirs, the login is never deleted, and reactivating them
  brings everything back. Left at every venue = the same answer as left at
  one.
* Join codes come from a full manager of the employee's venue only, like the
  bulk sheet. A department manager gets none, in or out of their section.
* A manager login linked as staff-only at another venue counts as linked
  there: removing their details at that venue scrubs their name from its feed
  and takes the venue off the login. Removing a manager's only dashboard
  venue leaves them a staff login at their other venues, not switched off.
"""

import uuid
from datetime import datetime, timedelta
from types import SimpleNamespace

from fastapi.testclient import TestClient

from rosteriq.api import app
from rosteriq.database import get_db
from rosteriq.services.clock import venue_today

PW = "Passw0rd!234"
NO_LONGER = "no longer active"


def _login(c, email):
    c.post("/api/auth/register", json={"email": email, "password": PW, "name": email.split("@")[0]})
    r = c.post("/api/auth/login", json={"email": email, "password": PW})
    assert r.status_code == 200, r.text
    return {"Authorization": f"Bearer {r.json()['access_token']}"}


def _venue(c, h, vid, name):
    return c.post("/venues", json={"id": vid, "name": name, "state": "wa", "max_labour_pct": 30,
                                   "tanda_org_id": "", "timezone": "Australia/Perth",
                                   "created_at": "2026-07-01T00:00:00"}, headers=h)


def _emp(c, h, vid, eid, name, email=None, skills=("bar",)):
    body = {"id": eid, "venue_id": vid, "name": name, "employment_type": "casual",
            "award_level": "level_2", "state": "wa", "hourly_base_rate": "30.00",
            "skills": list(skills), "created_at": "2026-07-01T00:00:00",
            "updated_at": "2026-07-01T00:00:00"}
    if email:
        body["email"] = email
    r = c.post("/employees", json=body, headers=h)
    assert r.status_code == 200, r.text
    return eid


def _link(c, mgr_h, eid, staff_h):
    code = c.get(f"/api/employees/{eid}/join-code", headers=mgr_h).json()["join_code"]
    return c.post("/api/me/link", json={"code": code}, headers=staff_h)


def _world():
    """Ben created The Wembley and Apothecary (his first and second venue)."""
    c = TestClient(app)
    tag = uuid.uuid4().hex[:6]
    _login(c, f"g5boot_{tag}@x.com")          # the platform owner is someone else
    ben_email = f"ben_{tag}@x.com"
    ben = _login(c, ben_email)
    vw, va = f"wem-{tag}", f"apo-{tag}"
    assert _venue(c, ben, vw, "The Wembley Hotel").status_code == 200
    assert _venue(c, ben, va, "Apothecary").status_code == 200
    return SimpleNamespace(c=c, tag=tag, ben=ben, ben_email=ben_email, vw=vw, va=va)


def _person(w, vid, name, venues=(), skills=("bar",)):
    """A staff member with a record at ``vid`` (and ``venues``), linked to
    their own login with the join codes."""
    email = f"{name.lower()}_{w.tag}@x.com"
    ids = {}
    for v in [vid] + list(venues):
        ids[v] = _emp(w.c, w.ben, v, f"{v}-{name.lower()}", name, email=email, skills=skills)
    h = _login(w.c, email)
    for eid in ids.values():
        r = _link(w.c, w.ben, eid, h)
        assert r.status_code == 200, r.text
    return SimpleNamespace(h=h, email=email, ids=ids, id=ids[vid])


def _set_role(w, eid, role="manager", **body):
    r = w.c.post(f"/api/employees/{eid}/access-role", json={"role": role, **body}, headers=w.ben)
    assert r.status_code == 200, r.text
    return r.json()


def _platform_owner(w):
    email = f"support_{w.tag}@x.com"
    w.c.post("/api/auth/register", json={"email": email, "password": PW, "name": "Support"})
    db = get_db()
    rec = db.get_user_by_email(email)
    rec["role"] = "owner"
    db.save_user(rec)
    return _login(w.c, email)


def _gave_himself_a_venue(w, email, vid):
    """The state a manager reached by creating a venue of their own before
    /venues refused it: the venue is on their login and they created it."""
    from rosteriq.models import State, VenueConfig
    from rosteriq.services.events import audit
    db = get_db()
    db.save_venue(VenueConfig(id=vid, name="Andy's Side Bar", tanda_org_id="", state=State.wa,
                              timezone="Australia/Perth", min_staff={}, max_labour_pct=30.0,
                              created_at=datetime(2026, 7, 1)))
    acct = db.get_user_by_email(email)
    acct["venue_ids"] = list(acct["venue_ids"]) + [vid]
    db.save_user(acct)
    audit("user.venue_grant", vid, "user", acct["id"], email=email,
          reason="first_venue_bootstrap", venue_ids=acct["venue_ids"])


def _detail(r):
    body = r.json()
    return body.get("detail") or (body.get("error") or {}).get("message") or ""


# --- [1] lowering access at one venue -----------------------------------------------

def test_owner_demotes_and_removes_a_manager_who_also_reaches_his_own_venue():
    w = _world()
    c, db = w.c, get_db()
    andy = _person(w, w.vw, "Andy")
    _set_role(w, andy.id)
    vx = f"side-{w.tag}"
    _gave_himself_a_venue(w, andy.email, vx)
    assert c.get("/api/leave", params={"venue_id": w.vw}, headers=andy.h).status_code == 200

    # Account-wide changes still need a manager of every venue the login reaches.
    r = c.post(f"/api/employees/{andy.id}/reset-link", headers=w.ben)
    assert r.status_code == 409 and "RosterIQ support" in _detail(r)
    # Lowering him at the Wembley gives Ben nothing at Andy's venue: allowed.
    r = c.post(f"/api/employees/{andy.id}/access-role", json={"role": "staff"}, headers=w.ben)
    assert r.status_code == 200, r.text
    acct = db.get_user_by_email(andy.email)
    assert acct["role"] == "manager" and acct["venue_ids"] == [vx]
    assert acct["staff_venue_ids"] == [w.vw] and acct["is_active"] is True
    # His manager access at the Wembley is gone at once; his own venue stays his.
    assert c.get("/api/leave", params={"venue_id": w.vw}, headers=andy.h).status_code == 403
    assert c.get("/employees", params={"venue_id": w.vw}, headers=andy.h).status_code == 403
    assert c.get("/api/leave", params={"venue_id": vx}, headers=andy.h).status_code == 200
    # Raising him again is account-wide: refused.
    r = c.post(f"/api/employees/{andy.id}/access-role", json={"role": "manager"}, headers=w.ben)
    assert r.status_code == 409
    r = c.post(f"/api/employees/{andy.id}/access", json={"venue_id": w.vw, "level": "full"},
               headers=w.ben)
    assert r.status_code == 409

    # Remove details at the Wembley: the venue comes off his login, nothing else.
    r = c.post(f"/api/privacy/anonymise/employee/{andy.id}", headers=w.ben)
    assert r.status_code == 200, r.text
    assert r.json()["logins_venue_removed"] == 1 and r.json()["logins_deactivated"] == 0
    acct = db.get_user_by_email(andy.email)
    assert acct["venue_ids"] == [vx] and acct["staff_venue_ids"] == [] and acct["is_active"] is True
    assert c.get(f"/api/feed/posts?venue_id={w.vw}", headers=andy.h).status_code == 403


def test_lowering_at_one_venue_through_the_access_list_and_the_creator_is_safe():
    w = _world()
    c, db = w.c, get_db()
    andy = _person(w, w.vw, "Andy")
    _set_role(w, andy.id)
    vx = f"side-{w.tag}"
    _gave_himself_a_venue(w, andy.email, vx)
    # Full -> department at the Wembley is lower: allowed.
    r = c.post(f"/api/employees/{andy.id}/access", headers=w.ben,
               json={"venue_id": w.vw, "level": "department", "sections": ["Bar"]})
    assert r.status_code == 200, r.text
    acct = db.get_user_by_email(andy.email)
    assert acct["section_grants"] == {w.vw: ["bar"]} and acct["venue_ids"] == [w.vw, vx]
    # Changing which department is not lower: account-wide rules.
    r = c.post(f"/api/employees/{andy.id}/access", headers=w.ben,
               json={"venue_id": w.vw, "level": "department", "sections": ["kitchen"]})
    assert r.status_code == 409
    # Nor at a venue Ben doesn't run.
    r = c.post(f"/api/employees/{andy.id}/access", headers=w.ben,
               json={"venue_id": vx, "level": "staff"})
    assert r.status_code in (403, 409)
    assert db.get_user_by_email(andy.email)["venue_ids"] == [w.vw, vx]

    # A second full manager of both pubs can't lower Ben, who created them.
    sam = _person(w, w.vw, "Sam")
    _set_role(w, sam.id)
    r = c.post(f"/api/employees/{sam.id}/access", json={"venue_id": w.va, "level": "full"},
               headers=w.ben)
    assert r.status_code == 200, r.text
    ben_rec = _emp(c, w.ben, w.vw, f"{w.vw}-ben", "Ben", email=w.ben_email)
    for path, body in ((f"/api/employees/{ben_rec}/access-role", {"role": "staff"}),
                       (f"/api/employees/{ben_rec}/access", {"venue_id": w.vw, "level": "staff"})):
        r = c.post(path, json=body, headers=sam.h)
        assert r.status_code == 403 and "RosterIQ support" in _detail(r), r.text
    ben_acct = db.get_user_by_email(w.ben_email)
    assert ben_acct["role"] == "manager" and set(ben_acct["venue_ids"]) == {w.vw, w.va}


def test_an_appointed_manager_cannot_add_a_venue_but_the_owner_can():
    w = _world()
    c, db = w.c, get_db()
    andy = _person(w, w.vw, "Andy")
    _set_role(w, andy.id)
    kim = _person(w, w.vw, "Kim", skills=("kitchen",))
    _set_role(w, kim.id, sections=["kitchen"])
    for who, h in (("andy", andy.h), ("kim", kim.h)):
        vid = f"{who}-own-{w.tag}"
        r = _venue(c, h, vid, "My Own Bar")
        assert r.status_code == 403, r.text
        assert db.get_venue(vid) is None
    assert db.get_user_by_email(andy.email)["venue_ids"] == [w.vw]
    # The owner adds a third pub; someone new signs up and creates their first.
    assert _venue(c, w.ben, f"third-{w.tag}", "The Third").status_code == 200
    newbie = _login(c, f"newbie_{w.tag}@x.com")
    assert _venue(c, newbie, f"newbie-{w.tag}", "Fresh Start").status_code == 200
    assert db.get_user_by_email(f"newbie_{w.tag}@x.com")["role"] == "manager"


# --- [2]/[30] leavers ------------------------------------------------------------------

def _seed_team_content(w, vid, title):
    c = w.c
    r = c.post("/api/announcements", json={"venue_id": vid, "title": title, "body": "In at 4"},
               headers=w.ben)
    assert r.status_code == 200, r.text
    r = c.post("/api/feed/posts", json={"venue_id": vid, "body": f"{title} feed"}, headers=w.ben)
    assert r.status_code == 200, r.text
    r = c.post("/api/sops/documents", json={"venue_id": vid, "title": f"{title} procedure",
                                            "body": "Do it right"}, headers=w.ben)
    assert r.status_code == 200, r.text


def _leave_body(vid, days=30):
    start = (venue_today(vid, get_db()) + timedelta(days=days)).isoformat()
    return {"start_date": start, "end_date": start, "reason": "x", "leave_type": "annual"}


def test_a_leaver_loses_the_team_surfaces_and_keeps_their_own_history():
    w = _world()
    c, db = w.c, get_db()
    sam = _person(w, w.vw, "Sam")
    amy = _person(w, w.vw, "Amy")
    _seed_team_content(w, w.vw, "Stocktake")
    ann_id = c.get("/api/me/announcements", headers=sam.h).json()["announcements"][0]["id"]
    r = c.post(f"/employees/{sam.id}/deactivate", headers=w.ben)
    assert r.status_code == 200, r.text
    _seed_team_content(w, w.vw, "New safe code")

    # Venue-wide reads and writes are closed to him.
    assert c.get("/api/me/announcements", headers=sam.h).json()["announcements"] == []
    assert c.get("/api/me/sops", headers=sam.h).json()["documents"] == []
    assert c.get("/api/me/cover", headers=sam.h).json()["up_for_grabs"] == []
    for method, path, body in (
        ("GET", f"/api/announcements?venue_id={w.vw}", None),
        ("GET", f"/api/feed/posts?venue_id={w.vw}", None),
        ("POST", "/api/feed/posts", {"venue_id": w.vw, "body": "posted after I was let go"}),
        ("GET", f"/employees?venue_id={w.vw}", None),
        ("GET", f"/employees/{amy.id}", None),
        ("POST", "/api/me/leave", _leave_body(w.vw)),
        ("POST", "/api/me/availability", {"days": {"monday": {"status": "unavailable"}}}),
        ("POST", f"/api/me/announcements/{ann_id}/read", None),
    ):
        r = c.request(method, path, json=body, headers=sam.h)
        assert r.status_code == 403 and NO_LONGER in _detail(r), (method, path, r.status_code, r.text)
    listing = c.get("/employees", headers=sam.h).json()["items"]
    assert [e["id"] for e in listing] == [sam.id]
    assert all("posted after" not in p["body"] for p in db.list_feed_posts(w.vw) or [])

    # His own history stays his, and the login is untouched.
    prof = c.get("/api/me/profile", headers=sam.h).json()
    assert prof["linked"] is True and prof["employee_id"] == sam.id
    for path in ("/api/me/shifts", "/api/me/timesheets", "/api/me/leave"):
        assert c.get(path, headers=sam.h).json()["linked"] is True
    assert c.get(f"/employees/{sam.id}", headers=sam.h).status_code == 200
    acct = db.get_user_by_email(sam.email)
    assert acct["is_active"] is True and acct["venue_ids"] == [w.vw]

    # A colleague still has it all; reactivating Sam needs no other step.
    assert c.get(f"/api/feed/posts?venue_id={w.vw}", headers=amy.h).status_code == 200
    assert c.post(f"/employees/{sam.id}/reactivate", headers=w.ben).status_code == 200
    assert c.get(f"/api/feed/posts?venue_id={w.vw}", headers=sam.h).status_code == 200
    titles = [a["title"] for a in c.get("/api/me/announcements", headers=sam.h).json()["announcements"]]
    assert "New safe code" in titles
    assert c.get(f"/employees?venue_id={w.vw}", headers=sam.h).status_code == 200
    assert c.post("/api/me/leave", json=_leave_body(w.vw), headers=sam.h).status_code == 200


def test_left_at_both_venues_is_the_same_answer_as_left_at_one():
    w = _world()
    c = w.c
    meca = _person(w, w.vw, "Meca", venues=[w.va])
    _seed_team_content(w, w.vw, "Wembley notice")
    _seed_team_content(w, w.va, "Apothecary notice")

    assert c.post(f"/employees/{meca.ids[w.vw]}/deactivate", headers=w.ben).status_code == 200
    titles = [a["title"] for a in c.get("/api/me/announcements", headers=meca.h).json()["announcements"]]
    assert titles == ["Apothecary notice"]
    r = c.get(f"/api/feed/posts?venue_id={w.vw}", headers=meca.h)
    assert r.status_code == 403 and NO_LONGER in _detail(r)
    assert c.get(f"/api/feed/posts?venue_id={w.va}", headers=meca.h).status_code == 200

    assert c.post(f"/employees/{meca.ids[w.va]}/deactivate", headers=w.ben).status_code == 200
    assert c.get("/api/me/announcements", headers=meca.h).json()["announcements"] == []
    assert c.get("/api/me/sops", headers=meca.h).json()["documents"] == []
    for vid in (w.vw, w.va):
        r = c.get(f"/api/feed/posts?venue_id={vid}", headers=meca.h)
        assert r.status_code == 403 and NO_LONGER in _detail(r), r.text
        r = c.post("/api/feed/posts", json={"venue_id": vid, "body": "still here"}, headers=meca.h)
        assert r.status_code == 403 and NO_LONGER in _detail(r), r.text
    r = c.post("/api/me/leave", json=_leave_body(w.vw), headers=meca.h)
    assert r.status_code == 403 and NO_LONGER in _detail(r)
    assert c.get("/api/me/profile", headers=meca.h).json()["linked"] is True


# --- [3] join codes ----------------------------------------------------------------------

def test_join_codes_need_a_full_manager_of_the_employees_venue():
    w = _world()
    c = w.c
    kim = _person(w, w.vw, "Kim", skills=("kitchen",))
    _set_role(w, kim.id, sections=["kitchen"])
    bar_hire = _emp(c, w.ben, w.vw, f"{w.vw}-newbie", "New Bie", skills=["front bar"])
    cook = _emp(c, w.ben, w.vw, f"{w.vw}-cook", "Cook Hire", skills=["kitchen"])
    for eid in (bar_hire, cook):
        assert c.get(f"/api/employees/{eid}/join-code", headers=kim.h).status_code == 403
        r = c.get(f"/api/employees/{eid}/join-code", headers=w.ben)
        assert r.status_code == 200 and r.json()["join_code"]
    staff = _person(w, w.vw, "Sid")
    assert c.get(f"/api/employees/{bar_hire}/join-code", headers=staff.h).status_code == 403
    other = _login(c, f"zoe_{w.tag}@x.com")
    assert _venue(c, other, f"zoe-{w.tag}", "Zoe's").status_code == 200
    assert c.get(f"/api/employees/{bar_hire}/join-code", headers=other).status_code == 404
    support = _platform_owner(w)
    assert c.get(f"/api/employees/{bar_hire}/join-code", headers=support).status_code == 200


# --- [28]/[42]/[29] staff-only links -------------------------------------------------------

def test_removing_a_staff_only_link_scrubs_the_name_and_drops_the_venue():
    from rosteriq.services.notification_hub import NotificationHub
    w = _world()
    c, db = w.c, get_db()
    riley = _person(w, w.vw, "Riley", venues=[w.va])
    _set_role(w, riley.id)                    # manager at the Wembley, staff-only at Apothecary
    acct = db.get_user_by_email(riley.email)
    assert acct["venue_ids"] == [w.vw] and acct["staff_venue_ids"] == [w.va]
    post = c.post("/api/feed/posts", json={"venue_id": w.va, "body": "Swap anyone?"}, headers=riley.h)
    assert post.status_code == 200 and post.json()["author_role"] == "staff", post.text
    ben_post = c.post("/api/feed/posts", json={"venue_id": w.va, "body": "Rota up"}, headers=w.ben)
    r = c.post(f"/api/feed/posts/{ben_post.json()['id']}/comments", json={"body": "Ta"},
               headers=riley.h)
    assert r.status_code == 200, r.text

    # She is linked at Apothecary: not on its join-code sheet, and its notices reach her.
    sheet = c.get(f"/api/venues/{w.va}/join-codes", headers=w.ben).json()["people"]
    assert riley.ids[w.va] not in {p["employee_id"] for p in sheet}
    assert riley.email in NotificationHub()._linked_logins(w.va)

    # Her Wembley record: she manages there, so it's Access first.
    r = c.post(f"/api/privacy/anonymise/employee/{riley.ids[w.vw]}", headers=w.ben)
    assert r.status_code == 409
    # Her Apothecary record: removed, name scrubbed, venue off the login.
    assert c.post(f"/employees/{riley.ids[w.va]}/deactivate", headers=w.ben).status_code == 200
    r = c.post(f"/api/privacy/anonymise/employee/{riley.ids[w.va]}", headers=w.ben)
    assert r.status_code == 200, r.text
    assert r.json()["names_rewritten"]["feed_posts"] == 1
    assert r.json()["logins_venue_removed"] == 1 and r.json()["logins_deactivated"] == 0
    names = [p["author_name"] for p in db.list_feed_posts(w.va)]
    names += [cm["author_name"] for p in db.list_feed_posts(w.va) for cm in p.get("comments") or []]
    assert not any("Riley" in n for n in names), names
    acct = db.get_user_by_email(riley.email)
    assert acct["role"] == "manager" and acct["venue_ids"] == [w.vw]
    assert acct["staff_venue_ids"] == [] and acct["is_active"] is True


def test_removing_a_managers_only_dashboard_venue_keeps_their_staff_venue():
    w = _world()
    c, db = w.c, get_db()
    pat = _person(w, w.va, "Pat", venues=[w.vw])
    _set_role(w, pat.id)                      # manager at Apothecary, staff-only at the Wembley
    acct = db.get_user_by_email(pat.email)
    assert acct["venue_ids"] == [w.va] and acct["staff_venue_ids"] == [w.vw]
    assert c.post(f"/api/privacy/anonymise/employee/{pat.id}", headers=w.ben).status_code == 409
    support = _platform_owner(w)
    r = c.post(f"/api/privacy/anonymise/employee/{pat.id}", headers=support)
    assert r.status_code == 200, r.text
    assert r.json()["logins_deactivated"] == 0 and r.json()["logins_venue_removed"] == 1
    acct = db.get_user_by_email(pat.email)
    assert acct["is_active"] is True and acct["role"] == "staff"
    assert acct["venue_ids"] == [w.vw] and acct["staff_venue_ids"] == []
    assert acct["section_grants"] == {}
    assert c.get("/api/me/profile", headers=pat.h).json()["venue_id"] == w.vw
    assert c.post("/api/auth/login", json={"email": pat.email, "password": PW}).status_code == 200


class _Users:
    def __init__(self):
        self.saved, self.revoked = [], []

    def save_user(self, u):
        self.saved.append(dict(u))

    def revoke_refresh_tokens_for_user(self, uid):
        self.revoked.append(uid)


def test_retire_logins_keeps_every_other_venue_at_its_level():
    from rosteriq.services.staff_records import retire_logins
    db = _Users()
    dept = {"id": "u1", "role": "manager", "venue_ids": ["a", "b"], "staff_venue_ids": ["c"],
            "section_grants": {"a": ["bar"], "b": ["kitchen"]}, "is_active": True}
    only_staff_left = {"id": "u2", "role": "manager", "venue_ids": ["a"], "staff_venue_ids": ["c"],
                       "section_grants": {"a": ["bar"]}, "is_active": True}
    staff_only_here = {"id": "u3", "role": "manager", "venue_ids": ["b"], "staff_venue_ids": ["a"],
                       "section_grants": {}, "is_active": True}
    last = {"id": "u4", "role": "manager", "venue_ids": [], "staff_venue_ids": ["a"],
            "section_grants": {}, "is_active": True}
    out = retire_logins(db, "a", [dept, only_staff_left, staff_only_here, last])
    assert out == {"deactivated": ["u4"], "venue_removed": ["u1", "u2", "u3"]}
    assert dept["role"] == "manager" and dept["venue_ids"] == ["b"]
    assert dept["staff_venue_ids"] == ["c"] and dept["section_grants"] == {"b": ["kitchen"]}
    assert only_staff_left["role"] == "staff" and only_staff_left["venue_ids"] == ["c"]
    assert only_staff_left["staff_venue_ids"] == [] and only_staff_left["section_grants"] == {}
    assert staff_only_here["venue_ids"] == ["b"] and staff_only_here["staff_venue_ids"] == []
    assert last["is_active"] is False and db.revoked == ["u4"]
