"""
Two venues, one owner, shared people.

Ben owns The Wembley Hotel and Apothecary. Meca (a casual duty manager) and
the assistant venue managers work at both, with a staff record at each.

Pins:
* person identity: the same person is the same email across venues, looked
  up only inside the venues the caller may see; busy_elsewhere finds their
  shift at the other venue (overnight shifts included)
* Ben (a full manager of both) can set access, issue reset links and give
  access to the other venue for a two-venue login; one venue's manager
  still can't, and the copy says "RosterIQ support"; nobody but RosterIQ
  support resets the owner's login
* a manager login can link its staff record at a venue the same owner runs
  (staff-only there: shifts on /my, no dashboard); a throwaway venue's
  manager still can't
* Ben's login is labelled "Owner (The Wembley Hotel, Apothecary)"
* /my merges shifts, news, procedures, hours, leave and cover across both
  venues with a venue tag; availability writes both records; leave asks
  which venue(s); a one-venue person sees exactly what they did before
* generating a roster treats the other venue's shifts as booked (day, rest
  gap); the conflict scan and publish warn "Meca is on at Apothecary"
* CrossVenueSync groups the two records as one person
* payroll prep flags (never computes) combined weekly hours past the
  threshold across the venues the caller fully manages
"""

import uuid
from datetime import date, datetime, time as dtime, timedelta
from types import SimpleNamespace

from fastapi.testclient import TestClient

from rosteriq.api import app
from rosteriq.database import get_db
from rosteriq.middleware.tenant import TenantContext, _tenant_context_var
from rosteriq.models import Roster, Shift, ShiftStatus
from rosteriq.services.clock import venue_today

PW = "Passw0rd!234"


def _login(c, email):
    c.post("/api/auth/register", json={"email": email, "password": PW, "name": email.split("@")[0]})
    r = c.post("/api/auth/login", json={"email": email, "password": PW})
    assert r.status_code == 200, r.text
    return {"Authorization": f"Bearer {r.json()['access_token']}"}


def _venue(c, h, vid, name):
    r = c.post("/venues", json={"id": vid, "name": name, "state": "wa", "max_labour_pct": 30,
                                "tanda_org_id": "", "created_at": "2026-07-01T00:00:00"}, headers=h)
    assert r.status_code in (200, 201), r.text


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
    """Ben runs both pubs; Meca has a record at each (the Apothecary one typed
    in capitals) and has linked her login to both with the two join codes."""
    c = TestClient(app)
    tag = uuid.uuid4().hex[:6]
    _login(c, f"b6boot_{tag}@x.com")          # the platform owner is someone else
    ben_email = f"ben_{tag}@x.com"
    ben = _login(c, ben_email)
    vw, va = f"wem-{tag}", f"apo-{tag}"
    _venue(c, ben, vw, "The Wembley Hotel")
    _venue(c, ben, va, "Apothecary")
    meca_email = f"meca_{tag}@x.com"
    mw = _emp(c, ben, vw, f"{vw}-meca", "Meca Rossi", email=meca_email)
    ma = _emp(c, ben, va, f"{va}-meca", "Meca Rossi", email=meca_email.upper())
    meca = _login(c, meca_email)
    assert _link(c, ben, mw, meca).status_code == 200
    assert _link(c, ben, ma, meca).status_code == 200
    return SimpleNamespace(c=c, tag=tag, ben=ben, ben_email=ben_email, vw=vw, va=va,
                           meca=meca, meca_email=meca_email, mw=mw, ma=ma)


def _person(w, vid, name, *, venues=None):
    """A staff member with a record at ``vid`` (and at ``venues`` too),
    linked to their own login."""
    email = f"{name.lower()}_{w.tag}@x.com"
    ids = {}
    for v in [vid] + list(venues or []):
        ids[v] = _emp(w.c, w.ben, v, f"{v}-{name.lower()}", name, email=email)
    h = _login(w.c, email)
    for eid in ids.values():
        assert _link(w.c, w.ben, eid, h).status_code == 200
    return SimpleNamespace(h=h, email=email, ids=ids, id=ids[vid])


def _promote(w, eid, **body):
    r = w.c.post(f"/api/employees/{eid}/access-role", json={"role": "manager", **body}, headers=w.ben)
    assert r.status_code == 200, r.text
    return r.json()


def _shift(sid, emp, day, start, end, role="bar"):
    return Shift(id=sid, employee_id=emp, date=day, start_time=start, end_time=end,
                 break_minutes=0, status=ShiftStatus.scheduled, role=role)


def _roster(vid, week_start, shifts, state=None, rid=None):
    db = get_db()
    rid = rid or f"{vid}-r{uuid.uuid4().hex[:6]}"
    db.save_roster(Roster(id=rid, venue_id=vid, week_start=week_start,
                          week_end=week_start + timedelta(days=6), shifts=shifts,
                          total_cost=None, created_at=datetime.utcnow()))
    if state:
        db.update_roster_state(rid, state, "seed", "test")
    return rid


def _monday(d):
    return d - timedelta(days=d.weekday())


class _As:
    """Run service code as a given caller (the tenant context a request has)."""

    def __init__(self, venue_ids, role="manager", is_owner=False):
        self.ctx = TenantContext(user_id="t", venue_ids=list(venue_ids), is_owner=is_owner, role=role)

    def __enter__(self):
        self.token = _tenant_context_var.set(self.ctx)

    def __exit__(self, *exc):
        _tenant_context_var.reset(self.token)


# --- person identity ------------------------------------------------------------

def test_same_person_is_the_same_email_inside_the_callers_venues_only():
    from rosteriq.services.person_identity import (
        busy_elsewhere, busy_elsewhere_many, same_person_records,
    )
    w = _world()
    db = get_db()
    # an unrelated pub elsewhere on the platform has a record with her email
    other_h = _login(w.c, f"other_{w.tag}@x.com")
    vo = f"oth-{w.tag}"
    _venue(w.c, other_h, vo, "Unrelated Pub")
    mo = _emp(w.c, other_h, vo, f"{vo}-meca", "Meca R", email=w.meca_email)

    meca_w = db.get_employee(w.mw)
    with _As([w.vw, w.va]):
        assert [(e.id, v) for e, v in same_person_records(db, meca_w)] == [(w.ma, w.va)]
    with _As([vo]):
        assert same_person_records(db, db.get_employee(mo)) == []
    with _As([w.vw]):                         # Wembley's AVM can't see Apothecary
        assert same_person_records(db, meca_w) == []

    # Friday night at Apothecary runs past midnight into Saturday
    fri = _monday(venue_today(w.va, db)) + timedelta(days=11)
    _roster(w.va, _monday(fri), [_shift(f"{w.va}-s1", w.ma, fri, dtime(22, 0), dtime(2, 0))],
            state="published")
    with _As([w.vw, w.va]):
        hits = busy_elsewhere(db, meca_w, fri + timedelta(days=1), "01:00", "05:00")
        assert [(h["venue_name"], h["start"], h["end"]) for h in hits] == [("Apothecary", "22:00", "02:00")]
        assert busy_elsewhere(db, meca_w, fri, "10:00", "16:00") == []
        assert busy_elsewhere(db, meca_w, fri, "13:00", "21:30") == []
        assert busy_elsewhere(db, meca_w, fri, "13:00", "21:30", gap_minutes=60) != []
        many = busy_elsewhere_many(db, w.vw, db.get_employees(w.vw), fri + timedelta(days=1),
                                   "01:00", "05:00")
        assert list(many) == [w.mw] and many[w.mw][0]["venue_name"] == "Apothecary"
    with _As([w.vw]):
        assert busy_elsewhere(db, meca_w, fri + timedelta(days=1), "01:00", "05:00") == []


# --- logins across two venues ----------------------------------------------------

def test_owner_of_both_venues_manages_a_two_venue_login():
    w = _world()
    c, db = w.c, get_db()
    # Make manager: at Wembley only. Apothecary stays staff-only, never a
    # silent full-manager venue.
    body = _promote(w, w.mw)
    assert "staff-only at Apothecary" in body["message"]
    acct = db.get_user_by_email(w.meca_email)
    assert acct["role"] == "manager" and acct["venue_ids"] == [w.vw]
    assert acct["staff_venue_ids"] == [w.va] and acct["section_grants"] == {}
    # reset link for the two-venue login
    r = c.post(f"/api/employees/{w.ma}/reset-link", headers=w.ben)
    assert r.status_code == 200, r.text
    # also give access to Apothecary — as the bar's department manager there
    r = c.get(f"/api/employees/{w.mw}/access", headers=w.ben)
    assert r.status_code == 200, r.text
    assert [v["venue_id"] for v in r.json()["venues"]] == [w.vw, w.va]
    r = c.post(f"/api/employees/{w.mw}/access", headers=w.ben,
               json={"venue_id": w.va, "level": "department", "sections": ["Bar"]})
    assert r.status_code == 200, r.text
    acct = db.get_user_by_email(w.meca_email)
    assert acct["venue_ids"] == [w.vw, w.va] and acct["staff_venue_ids"] == []
    assert acct["section_grants"] == {w.va: ["bar"]}
    # and back to staff at both: one staff login holding both venues
    for vid in (w.vw, w.va):
        r = c.post(f"/api/employees/{w.mw}/access", headers=w.ben,
                   json={"venue_id": vid, "level": "staff"})
        assert r.status_code == 200, r.text
    acct = db.get_user_by_email(w.meca_email)
    assert acct["role"] == "staff" and set(acct["venue_ids"]) == {w.vw, w.va}


def test_one_venues_manager_still_cannot_control_a_two_venue_login():
    w = _world()
    c = w.c
    sam = _person(w, w.vw, "Sam")
    _promote(w, sam.id)                       # full manager of Wembley only
    for path in (f"/api/employees/{w.mw}/reset-link", f"/api/employees/{w.mw}/access-role"):
        r = c.post(path, json={"role": "manager"}, headers=sam.h)
        assert r.status_code == 409, r.text
        assert "RosterIQ support" in r.text and "platform owner" not in r.text
    r = c.post(f"/api/employees/{w.mw}/access", json={"venue_id": w.vw, "level": "full"}, headers=sam.h)
    assert r.status_code == 409, r.text
    # nor give anyone access to a venue they don't run
    kay = _person(w, w.vw, "Kay")
    r = c.post(f"/api/employees/{kay.id}/access", json={"venue_id": w.va, "level": "full"},
               headers=sam.h)
    assert r.status_code == 403, r.text
    # a department manager never controls logins at all
    _promote(w, kay.id, sections=["bar"])
    lou = _person(w, w.vw, "Lou")
    assert c.post(f"/api/employees/{lou.id}/reset-link", headers=kay.h).status_code == 403

    # Ben gives Sam Apothecary too: now Sam runs both and may help Meca...
    r = c.post(f"/api/employees/{sam.id}/access", json={"venue_id": w.va, "level": "full"},
               headers=w.ben)
    assert r.status_code == 200, r.text
    assert "can now open Apothecary as a manager" in r.json()["message"]
    assert c.post(f"/api/employees/{w.mw}/reset-link", headers=sam.h).status_code == 200
    # ...but never take over the owner's own login
    ben_rec = _emp(c, w.ben, w.vw, f"{w.vw}-ben", "Ben", email=w.ben_email)
    r = c.post(f"/api/employees/{ben_rec}/reset-link", headers=sam.h)
    assert r.status_code == 403, r.text
    assert "RosterIQ support" in r.text


def test_manager_links_their_staff_record_at_the_owners_other_venue():
    w = _world()
    c, db = w.c, get_db()
    riley = _person(w, w.vw, "Riley")
    _promote(w, riley.id)
    ra = _emp(c, w.ben, w.va, f"{w.va}-riley", "Riley", email=riley.email)
    r = _link(c, w.ben, ra, riley.h)
    assert r.status_code == 200, r.text
    acct = db.get_user_by_email(riley.email)
    assert acct["venue_ids"] == [w.vw] and acct["staff_venue_ids"] == [w.va]
    # her Apothecary shift reaches her phone; Apothecary's dashboard stays shut
    day = venue_today(w.va, db) + timedelta(days=2)
    _roster(w.va, _monday(day), [_shift(f"{w.va}-rs", ra, day, dtime(10, 0), dtime(16, 0))],
            state="published")
    shifts = c.get("/api/me/shifts", headers=riley.h).json()["shifts"]
    assert [(s["venue_name"], s["date"]) for s in shifts] == [("Apothecary", day.isoformat())]
    assert c.get("/api/leave", params={"venue_id": w.va}, headers=riley.h).status_code == 403
    assert c.get("/api/leave", params={"venue_id": w.vw}, headers=riley.h).status_code == 200

    # a login that manages a throwaway venue it made itself is still refused
    zed_email = f"zed_{w.tag}@x.com"
    zed = _login(c, zed_email)
    _venue(c, zed, f"zed-{w.tag}", "Zed's")
    zw = _emp(c, w.ben, w.vw, f"{w.vw}-zed", "Zed", email=zed_email)
    r = _link(c, w.ben, zw, _login(c, zed_email))
    assert r.status_code == 409, r.text
    assert "RosterIQ support" in r.text
    assert w.vw not in (db.get_user_by_email(zed_email).get("staff_venue_ids") or [])


def test_the_owner_is_labelled_owner_of_both_venues():
    w = _world()
    r = w.c.get("/api/me/access", headers=w.ben)
    assert r.status_code == 200, r.text
    assert r.json()["label"] == "Owner (The Wembley Hotel, Apothecary)"
    _promote(w, w.mw)
    assert w.c.get("/api/me/access", headers=w.meca).json()["label"] == "Manager (The Wembley Hotel)"
    assert w.c.get(f"/api/employees/{w.mw}/access", headers=w.ben).json()["label"] == \
        "Manager (The Wembley Hotel)"


def test_demo_cannot_use_the_access_endpoints():
    c = TestClient(app)
    demo_h = {"Authorization": f"Bearer {c.post('/api/auth/demo').json()['access_token']}"}
    db = get_db()
    emp = next(iter(db.get_employees("demo-venue-001") or []), None)
    if emp is not None:
        r = c.post(f"/api/employees/{emp.id}/access", json={"venue_id": "demo-venue-001", "level": "full"},
                   headers=demo_h)
        assert r.status_code == 403, r.text
    assert c.get("/api/me/access", headers=demo_h).json()["label"] == "Demo"


# --- /my across venues ------------------------------------------------------------

def test_my_merges_both_venues_with_a_venue_tag():
    w = _world()
    c, db = w.c, get_db()
    today = venue_today(w.vw, db)
    d2, d4 = today + timedelta(days=2), today + timedelta(days=4)
    sam = _person(w, w.va, "Sam")
    _roster(w.vw, _monday(d2), [_shift(f"{w.vw}-m2", w.mw, d2, dtime(17, 0), dtime(23, 0)),
                                _shift(f"{w.vw}-m4", w.mw, d4, dtime(12, 0), dtime(20, 0))],
            state="published")
    if _monday(d4) != _monday(d2):
        _roster(w.vw, _monday(d4), [_shift(f"{w.vw}-m4", w.mw, d4, dtime(12, 0), dtime(20, 0))],
                state="published")
    a_shifts = [_shift(f"{w.va}-m3", w.ma, today + timedelta(days=3), dtime(10, 0), dtime(18, 0)),
                _shift(f"{w.va}-s4", sam.id, d4, dtime(10, 0), dtime(18, 0))]
    for monday in sorted({_monday(s.date) for s in a_shifts}):
        _roster(w.va, monday, [s for s in a_shifts if _monday(s.date) == monday], state="published")
    now = datetime.utcnow()
    for vid, name in ((w.vw, "Derby Saturday"), (w.va, "New cocktail list")):
        db.save_announcement({"id": f"ann-{vid}", "venue_id": vid, "title": name, "body": "x",
                              "audience": [], "pinned": False, "created_at": now, "read_by": []})
        db.save_sop_document({"id": f"sop-{vid}", "venue_id": vid, "title": f"Closing {name}",
                              "category": "sop", "body": "x", "applies_to": [], "version": 1,
                              "requires_ack": True, "active": True, "created_at": now})
    for vid, eid in ((w.vw, w.mw), (w.va, w.ma)):
        db.save_timesheet({"id": f"ts-{vid}", "venue_id": vid, "employee_id": eid,
                           "work_date": today - timedelta(days=2),
                           "clock_in": now - timedelta(days=2, hours=8),
                           "clock_out": now - timedelta(days=2), "break_minutes": 30,
                           "status": "approved", "created_at": now})
    db.save_shift_cover({"id": f"cv-{w.tag}", "venue_id": w.va, "shift_id": f"{w.va}-s4",
                         "shift_date": d4, "shift_start": "10:00", "shift_end": "18:00",
                         "role": "bar", "requested_by": sam.id, "reason": "", "status": "open",
                         "created_at": now})

    prof = c.get("/api/me/profile", headers=w.meca).json()
    assert [v["venue_name"] for v in prof["venues"]] == ["The Wembley Hotel", "Apothecary"]
    shifts = c.get("/api/me/shifts", headers=w.meca).json()["shifts"]
    assert [(s["date"], s["venue_name"]) for s in shifts] == [
        (d2.isoformat(), "The Wembley Hotel"), ((today + timedelta(days=3)).isoformat(), "Apothecary"),
        (d4.isoformat(), "The Wembley Hotel")]
    news = c.get("/api/me/announcements", headers=w.meca).json()
    assert {(a["title"], a["venue_name"]) for a in news["announcements"]} == {
        ("Derby Saturday", "The Wembley Hotel"), ("New cocktail list", "Apothecary")}
    assert news["unread"] == 2
    assert c.post(f"/api/me/announcements/ann-{w.va}/read", headers=w.meca).status_code == 200
    sops = c.get("/api/me/sops", headers=w.meca).json()
    assert sops["outstanding"] == 2 and {d["venue_name"] for d in sops["documents"]} == {
        "The Wembley Hotel", "Apothecary"}
    assert c.post(f"/api/me/sops/sop-{w.va}/acknowledge", headers=w.meca).status_code == 200
    hours = c.get("/api/me/timesheets", headers=w.meca).json()
    assert hours["count"] == 2 and hours["total_worked_minutes"] == 2 * (8 * 60 - 30)

    board = c.get("/api/me/cover", headers=w.meca).json()
    assert [(g["id"], g["venue_name"]) for g in board["up_for_grabs"]] == [(f"cv-{w.tag}", "Apothecary")]
    # taking Sam's Apothecary shift would double-book her: she's on at Wembley then
    r = c.post(f"/api/me/cover/cv-{w.tag}/claim", headers=w.meca)
    assert r.status_code == 409, r.text
    assert "on at The Wembley Hotel 12:00-20:00" in r.text

    # availability is the person's: Sunday off reaches both venues
    r = c.post("/api/me/availability", headers=w.meca,
               json={"days": {"sunday": {"status": "unavailable"}}})
    assert r.status_code == 200, r.text
    assert db.get_employee(w.mw).availability.get("sunday") == []
    assert db.get_employee(w.ma).availability.get("sunday") == []

    # leave asks which venue(s); left out, it goes to both
    start = today + timedelta(days=20)
    r = c.post("/api/me/leave", headers=w.meca, json={
        "start_date": start.isoformat(), "end_date": start.isoformat(), "venue_ids": [w.va]})
    assert r.status_code == 200 and r.json()["venues"] == ["Apothecary"], r.text
    assert [x["employee_id"] for x in db.list_leave_requests(w.va)] == [w.ma]
    assert db.list_leave_requests(w.vw) == []
    later = start + timedelta(days=7)
    r = c.post("/api/me/leave", headers=w.meca, json={
        "start_date": later.isoformat(), "end_date": later.isoformat()})
    assert r.status_code == 200 and len(r.json()["request_ids"]) == 2, r.text
    assert len(db.list_leave_requests(w.vw)) == 1
    r = c.post("/api/me/leave", headers=w.meca, json={
        "start_date": later.isoformat(), "end_date": later.isoformat(), "venue_ids": ["elsewhere"]})
    assert r.status_code == 422
    mine = c.get("/api/me/leave", headers=w.meca).json()["requests"]
    assert sorted(r["venue_name"] for r in mine) == ["Apothecary", "Apothecary", "The Wembley Hotel"]


def test_one_venue_staff_see_what_they_always_saw():
    w = _world()
    c, db = w.c, get_db()
    tay = _person(w, w.vw, "Tayla")
    day = venue_today(w.vw, db) + timedelta(days=1)
    _roster(w.vw, _monday(day), [_shift(f"{w.vw}-t1", tay.id, day, dtime(11, 0), dtime(17, 0))],
            state="published")
    prof = c.get("/api/me/profile", headers=tay.h).json()
    assert prof["venue_id"] == w.vw and prof["employee_id"] == tay.id and len(prof["venues"]) == 1
    body = c.get("/api/me/shifts", headers=tay.h).json()
    assert body["employee_id"] == tay.id and [s["id"] for s in body["shifts"]] == [f"{w.vw}-t1"]
    start = day + timedelta(days=10)
    r = c.post("/api/me/leave", headers=tay.h, json={
        "start_date": start.isoformat(), "end_date": start.isoformat()})
    assert r.status_code == 200 and r.json()["request_id"] == r.json()["request_ids"][0]
    assert [x["employee_id"] for x in db.list_leave_requests(w.vw)] == [tay.id]


# --- no double-booking ------------------------------------------------------------

def test_generator_keeps_people_off_where_they_are_booked_at_the_other_venue():
    w = _world()
    c, db = w.c, get_db()
    _person(w, w.vw, "Tayla")
    ws = _monday(venue_today(w.vw, db)) + timedelta(days=7)
    thu, fri, sat = ws + timedelta(days=3), ws + timedelta(days=4), ws + timedelta(days=5)
    _roster(w.va, ws, [_shift(f"{w.va}-b{i}", w.ma, d, dtime(16, 0), dtime(23, 0))
                       for i, d in enumerate((thu, fri, sat))], state="published")
    r = c.post("/rosters/generate", json={"venue_id": w.vw, "week_start": ws.isoformat()},
               headers=w.ben)
    assert r.status_code == 200, r.text
    out = r.json()
    meca_days = {s["date"] for s in out["shifts"] if s["employee_id"] == w.mw}
    assert meca_days, "Meca should still work her free days at Wembley"
    assert not meca_days & {thu.isoformat(), fri.isoformat(), sat.isoformat()}
    assert any("Meca Rossi is on at Apothecary 16:00-23:00" in m for m in out["booked_elsewhere"])


def test_generator_keeps_the_rest_gap_before_a_shift_booked_elsewhere():
    from rosteriq.roster_optimiser import _is_employee_available
    w = _world()
    db = get_db()
    meca = db.get_employee(w.mw)
    tue = _monday(venue_today(w.vw, db)) + timedelta(days=8)
    early = _shift("x1", w.mw, tue + timedelta(days=1), dtime(6, 0), dtime(14, 0))
    ok, why = _is_employee_available(meca, tue, 14, 22, [early], 0)
    assert not ok and "rest" in why
    later = _shift("x2", w.mw, tue + timedelta(days=1), dtime(10, 0), dtime(16, 0))
    assert _is_employee_available(meca, tue, 14, 22, [later], 0)[0]


def test_conflict_scan_and_publish_warn_about_the_other_venue():
    w = _world()
    c, db = w.c, get_db()
    ws = _monday(venue_today(w.vw, db)) + timedelta(days=7)
    fri = ws + timedelta(days=4)
    rid = _roster(w.vw, ws, [_shift(f"{w.vw}-f", w.mw, fri, dtime(16, 0), dtime(23, 0))])
    _roster(w.va, ws, [_shift(f"{w.va}-f", w.ma, fri, dtime(18, 0), dtime(22, 0))], state="published")

    r = c.get(f"/api/v1/rosters/{rid}/conflicts", headers=w.ben)
    assert r.status_code == 200, r.text
    rows = [x for x in r.json()["conflicts"] if x["conflict_type"] == "cross_venue"]
    assert [x["message"] for x in rows] == [
        f"Meca Rossi is on at Apothecary 18:00-22:00 on {fri.strftime('%a')} {fri.day} {fri.strftime('%b')}"]
    assert rows[0]["severity"] == "warning"

    # Wembley's AVM can't see Apothecary, so nothing about it reaches them
    sam = _person(w, w.vw, "Sam")
    _promote(w, sam.id)
    r = c.get(f"/api/v1/rosters/{rid}/conflicts", headers=sam.h)
    assert r.status_code == 200 and "Apothecary" not in r.text, r.text

    r = c.post(f"/api/v1/rosters/{rid}/publish", json={"skip_approval": True}, headers=w.ben)
    assert r.status_code == 200, r.text
    assert any("Meca Rossi is on at Apothecary 18:00-22:00" in m
               for m in r.json()["cross_venue_warnings"])


def test_cross_venue_sync_treats_the_two_records_as_one_person():
    w = _world()
    c, db = w.c, get_db()
    ws = _monday(venue_today(w.vw, db))
    fri = ws + timedelta(days=4)
    _roster(w.vw, ws, [_shift(f"{w.vw}-cv", w.mw, fri, dtime(16, 0), dtime(23, 0))], state="published")
    _roster(w.va, ws, [_shift(f"{w.va}-cv", w.ma, fri, dtime(18, 0), dtime(22, 0))], state="published")
    ids = f"{w.vw},{w.va}"
    shared = c.get("/api/v1/multi-venue/shared-employees", params={"venue_ids": ids}, headers=w.ben)
    assert shared.status_code == 200, shared.text
    assert [set(row["venues"]) for row in shared.json()] == [{w.vw, w.va}]
    conflicts = c.get("/api/v1/multi-venue/conflicts", headers=w.ben, params={
        "venue_ids": ids, "start_date": ws.isoformat(), "end_date": (ws + timedelta(days=6)).isoformat()})
    assert conflicts.status_code == 200 and len(conflicts.json()) == 1, conflicts.text


# --- payroll ------------------------------------------------------------------------

def test_payroll_prep_flags_combined_hours_across_the_owners_venues():
    w = _world()
    c, db = w.c, get_db()
    ws = _monday(venue_today(w.vw, db)) - timedelta(days=7)
    for vid, eid, days in ((w.vw, w.mw, range(4)), (w.va, w.ma, range(4, 7))):
        for i in days:
            d = ws + timedelta(days=i)
            start = datetime.combine(d, dtime(2, 0))       # 10:00 Perth, naive UTC
            db.save_timesheet({"id": f"ts-{vid}-{i}", "venue_id": vid, "employee_id": eid,
                               "work_date": d, "clock_in": start, "clock_out": start + timedelta(hours=8),
                               "break_minutes": 0, "status": "approved", "created_at": start})
    period = {"venue_id": w.vw, "period_start": ws.isoformat(),
              "period_end": (ws + timedelta(days=6)).isoformat(), "state": "wa"}
    r = c.post("/api/payroll/prepare-actuals", json=period, headers=w.ben)
    assert r.status_code == 200, r.text
    flags = r.json()["cross_venue_hours"]
    assert len(flags) == 1
    assert flags[0].startswith("Meca Rossi: 56h across both venues")
    assert "The Wembley Hotel 32h" in flags[0] and "Apothecary 24h" in flags[0]
    assert flags[0].endswith("check overtime with your advisor")

    # Wembley's AVM runs Wembley's pay prep without seeing Apothecary's hours
    sam = _person(w, w.vw, "Sam")
    _promote(w, sam.id)
    r = c.post("/api/payroll/prepare-actuals", json=period, headers=sam.h)
    assert r.status_code == 200, r.text
    assert r.json()["cross_venue_hours"] == []
