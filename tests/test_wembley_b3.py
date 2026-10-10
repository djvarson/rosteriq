"""
Wembley pilot (b3): managers fix the roster by hand, and copy last week.

* POST/PUT/DELETE /rosters/{id}/shifts add, move, reassign, retime, re-area
  and remove one shift. The shift is recosted and so is the roster total.
* Hard stops: overlapping the same person's own shift (409), rostering an
  inactive person (409), a day outside the roster's week or someone from
  another venue (400). Everything else a manager may override comes back as
  a warning: availability, approved leave, recorded visa cap, max hours,
  area skill, busy at a sister venue, award compliance.
* A department manager edits only shifts in their own areas, and cannot copy
  a week (venue-wide).
* Editing a published roster keeps it published; staff /my shows the change
  at once; each change is audited (who, before, after) and listed as
  "changed since publish" until the manager notifies staff or republishes.
* POST /rosters/{id}/copy-to clones a week into another as a new draft,
  leaving open the shifts of people now inactive, on approved leave or
  unavailable, and says which it left open.
"""

import sys
import types
import uuid
from datetime import date, datetime, time, timedelta
from decimal import Decimal

from fastapi.testclient import TestClient

from rosteriq.api import app
from rosteriq.database import get_db
from rosteriq.cost_calculator import calculate_roster_cost
from rosteriq.models import Employee, Roster, Shift, ShiftStatus, State

PW = "Passw0rd!234"
nc = TestClient(app, raise_server_exceptions=False)

AREAS = [
    {"name": "front bar", "min_by_day": {"fri": 2, "sat": 3}},
    {"name": "sports bar"},
    {"name": "kitchen"},
    {"name": "duty manager", "requires_skill": True},
]


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _login(c, email):
    c.post("/api/auth/register", json={"email": email, "password": PW, "name": "U"})
    tok = c.post("/api/auth/login", json={"email": email, "password": PW}).json()["access_token"]
    return {"Authorization": f"Bearer {tok}"}


def _set(email, role, venue_ids, grants=None):
    db = get_db()
    rec = db.get_user_by_email(email)
    rec["role"] = role
    rec["venue_ids"] = list(venue_ids)
    rec["section_grants"] = grants or {}
    db.save_user(rec)
    return rec


def _manager_with_venue(c):
    email = f"wm{uuid.uuid4().hex[:8]}@x.com"
    h = _login(c, email)
    _set(email, "staff", [])
    vid = f"wembley-{uuid.uuid4().hex[:6]}"
    r = c.post("/venues", json={"id": vid, "name": "The Wembley Hotel", "state": "wa",
                                "max_labour_pct": 30, "tanda_org_id": "",
                                "timezone": "Australia/Perth",
                                "created_at": "2026-10-01T00:00:00"}, headers=h)
    assert r.status_code == 200, r.text
    db = get_db()
    db.save_venue(db.get_venue(vid).model_copy(update={"areas": AREAS}))
    return h, vid, db.get_user_by_email(email)["id"]


def _user(c, vid, role="staff", grants=None, email=None):
    email = email or f"ws{uuid.uuid4().hex[:8]}@x.com"
    h = _login(c, email)
    _set(email, role, [vid], grants)
    return h, email


def _emp(vid, key, name, **kw):
    fields = dict(employment_type="casual", award_level="level_2", state="wa",
                  hourly_base_rate="31.50", skills=["front bar"],
                  created_at=datetime(2026, 10, 1), updated_at=datetime(2026, 10, 1))
    fields.update(kw)
    emp = Employee(id=f"{vid}-{key}", venue_id=vid, name=name, **fields)
    get_db().save_employee(emp)
    return emp


def _next_monday():
    today = date.today()
    return today - timedelta(days=today.weekday()) + timedelta(days=7)


def _shift(emp_id, day, start, end, role="front bar", brk=30, sid=None):
    return Shift(id=sid or f"s-{uuid.uuid4().hex[:8]}", employee_id=emp_id, date=day,
                 start_time=start, end_time=end, break_minutes=brk,
                 status=ShiftStatus.scheduled, role=role, cost=Decimal("100"))


def _roster(vid, monday, shifts, rid=None, state=None):
    r = Roster(id=rid or f"r-{uuid.uuid4().hex[:8]}", venue_id=vid, week_start=monday,
               week_end=monday + timedelta(days=6), shifts=shifts,
               total_cost=Decimal(100 * len(shifts)), created_at=datetime.utcnow())
    db = get_db()
    db.save_roster(r)
    if state:
        db.update_roster_state(r.id, state, "test", "system")
    return r


def _deactivate(monkeypatch, emp_id):
    db = get_db()
    emp = db.get_employee(emp_id)
    emp.active = False
    db.save_employee(emp)


def _codes(resp):
    return {w["code"] for w in resp.json()["warnings"]}


def _base():
    c = TestClient(app)
    mgr, vid, mgr_id = _manager_with_venue(c)
    monday = _next_monday()
    amy = _emp(vid, "amy", "Amy Barker", skills=["front bar", "sports bar"])
    ben = _emp(vid, "ben", "Ben Cook", skills=["kitchen", "front bar"])
    wed = monday + timedelta(days=2)
    r = _roster(vid, monday, [_shift(amy.id, wed, time(17), time(23), sid=f"{vid}-s1")])
    return c, mgr, vid, mgr_id, monday, amy, ben, r


# ---------------------------------------------------------------------------
# add, move, reassign, delete
# ---------------------------------------------------------------------------

def test_manager_adds_moves_reassigns_and_deletes_shifts():
    c, mgr, vid, _, monday, amy, ben, r = _base()
    db = get_db()
    thu = monday + timedelta(days=3)

    add = c.post(f"/rosters/{r.id}/shifts", json={
        "employee_id": ben.id, "date": thu.isoformat(), "start_time": "11:00",
        "end_time": "17:00", "break_minutes": 30, "role": "Kitchen"}, headers=mgr)
    assert add.status_code == 200, add.text
    body = add.json()
    new_id = body["shift"]["id"]
    assert body["shift"]["role"] == "kitchen"
    assert Decimal(body["shift"]["cost"]) > 0
    saved = db.get_roster(r.id)
    assert {s.id for s in saved.shifts} == {f"{vid}-s1", new_id}
    # the seeded total is replaced by award costing of every shift in the week
    assert saved.total_cost == calculate_roster_cost(saved, {amy.id: amy, ben.id: ben}, State.wa)
    assert saved.total_cost != r.total_cost
    assert body["warnings"] == []
    assert body["state"] == "draft" and body["changed_since_publish"] is False

    fri = monday + timedelta(days=4)
    move = c.put(f"/rosters/{r.id}/shifts/{vid}-s1", json={
        "date": fri.isoformat(), "start_time": "18:00", "end_time": "23:30", "role": "sports bar"},
        headers=mgr)
    assert move.status_code == 200, move.text
    moved = next(s for s in db.get_roster(r.id).shifts if s.id == f"{vid}-s1")
    assert (moved.date, moved.start_time, moved.end_time, moved.role) == (fri, time(18), time(23, 30), "sports bar")
    assert moved.employee_id == amy.id and moved.break_minutes == 30

    reassign = c.put(f"/rosters/{r.id}/shifts/{vid}-s1", json={"employee_id": ben.id, "role": "front bar"},
                     headers=mgr)
    assert reassign.status_code == 200, reassign.text
    assert next(s for s in db.get_roster(r.id).shifts if s.id == f"{vid}-s1").employee_id == ben.id

    before_total = db.get_roster(r.id).total_cost
    gone = c.delete(f"/rosters/{r.id}/shifts/{new_id}", headers=mgr)
    assert gone.status_code == 200, gone.text
    left = db.get_roster(r.id)
    assert [s.id for s in left.shifts] == [f"{vid}-s1"]
    assert left.total_cost < before_total
    assert Decimal(gone.json()["total_cost"]) == left.total_cost


def test_hard_rules_refuse_the_edit_and_save_nothing(monkeypatch):
    c, mgr, vid, _, monday, amy, ben, r = _base()
    db = get_db()
    wed = monday + timedelta(days=2)

    clash = c.post(f"/rosters/{r.id}/shifts", json={
        "employee_id": amy.id, "date": wed.isoformat(), "start_time": "21:00",
        "end_time": "23:59", "role": "sports bar"}, headers=mgr)
    assert clash.status_code == 409
    assert "already rostered" in clash.json()["error"]["message"]

    # an overnight close runs into the next day's shift
    ok = c.post(f"/rosters/{r.id}/shifts", json={
        "employee_id": ben.id, "date": wed.isoformat(), "start_time": "18:00",
        "end_time": "02:00", "break_minutes": 30, "role": "front bar"}, headers=mgr)
    assert ok.status_code == 200, ok.text
    overnight = c.post(f"/rosters/{r.id}/shifts", json={
        "employee_id": ben.id, "date": (wed + timedelta(days=1)).isoformat(), "start_time": "01:00",
        "end_time": "05:00", "role": "front bar"}, headers=mgr)
    assert overnight.status_code == 409

    assert c.post(f"/rosters/{r.id}/shifts", json={
        "employee_id": ben.id, "date": wed.isoformat(), "start_time": "10:00",
        "end_time": "11:00", "break_minutes": 60, "role": "front bar"}, headers=mgr).status_code == 400
    assert c.post(f"/rosters/{r.id}/shifts", json={
        "employee_id": ben.id, "date": (monday + timedelta(days=7)).isoformat(), "start_time": "10:00",
        "end_time": "15:00", "role": "front bar"}, headers=mgr).status_code == 400

    other_vid = f"apothecary-{uuid.uuid4().hex[:6]}"
    stranger = _emp(other_vid, "zed", "Zed Other")
    assert c.post(f"/rosters/{r.id}/shifts", json={
        "employee_id": stranger.id, "date": wed.isoformat(), "start_time": "10:00",
        "end_time": "15:00", "role": "front bar"}, headers=mgr).status_code == 400

    _deactivate(monkeypatch, ben.id)
    inactive = c.post(f"/rosters/{r.id}/shifts", json={
        "employee_id": ben.id, "date": (monday + timedelta(days=5)).isoformat(), "start_time": "10:00",
        "end_time": "15:00", "role": "front bar"}, headers=mgr)
    assert inactive.status_code == 409
    assert "inactive" in inactive.json()["error"]["message"]
    assert c.put(f"/rosters/{r.id}/shifts/{vid}-s1", json={"employee_id": ben.id}, headers=mgr).status_code == 409

    assert len(db.get_roster(r.id).shifts) == 2   # the seeded shift and Ben's overnight close


def test_overridable_problems_come_back_as_warnings(monkeypatch):
    c, mgr, vid, _, monday, amy, ben, r = _base()
    db = get_db()
    sat = monday + timedelta(days=5)
    fri = monday + timedelta(days=4)
    cat = _emp(vid, "cat", "Cat Junior", availability={"saturday": []}, max_hours_per_week=10,
               visa_status="student", visa_work_limit_fortnight=8,
               visa_expiry=monday + timedelta(days=4))
    db.save_leave_request({"id": f"lv-{uuid.uuid4().hex[:6]}", "venue_id": vid, "employee_id": cat.id,
                           "start_date": sat, "end_date": sat, "reason": "Footy",
                           "status": "approved", "created_at": datetime.utcnow()})

    fake = types.ModuleType("rosteriq.services.person_identity")
    fake.busy_elsewhere = lambda db_, emp, day, start, end: (
        [{"venue_id": "apo", "venue_name": "Apothecary", "start": "12:00", "end": "16:00"}]
        if emp.id == cat.id else [])
    import rosteriq.services as services_pkg
    monkeypatch.setitem(sys.modules, "rosteriq.services.person_identity", fake)
    monkeypatch.setattr(services_pkg, "person_identity", fake, raising=False)

    resp = c.post(f"/rosters/{r.id}/shifts", json={
        "employee_id": cat.id, "date": sat.isoformat(), "start_time": "10:00",
        "end_time": "22:00", "break_minutes": 0, "role": "kitchen"}, headers=mgr)
    assert resp.status_code == 200, resp.text
    codes = _codes(resp)
    assert {"availability", "leave", "max_hours", "visa_cap", "visa_expiry", "skill",
            "busy_elsewhere", "compliance"} <= codes
    msgs = " ".join(w["message"] for w in resp.json()["warnings"])
    assert "Apothecary" in msgs and "8h" in msgs
    assert any(s.employee_id == cat.id for s in db.get_roster(r.id).shifts)   # saved anyway

    # a venue area that requires the skill is a warning too, and an unknown area is named
    dm = c.post(f"/rosters/{r.id}/shifts", json={
        "employee_id": amy.id, "date": fri.isoformat(), "start_time": "17:00",
        "end_time": "23:00", "break_minutes": 30, "role": "duty manager"}, headers=mgr)
    assert dm.status_code == 200 and "skill" in _codes(dm)
    odd = c.post(f"/rosters/{r.id}/shifts", json={
        "employee_id": ben.id, "date": fri.isoformat(), "start_time": "10:00",
        "end_time": "15:00", "break_minutes": 30, "role": "car park"}, headers=mgr)
    assert odd.status_code == 200 and "area" in _codes(odd)

    # less than the minimum rest before the person's next shift
    rest = c.post(f"/rosters/{r.id}/shifts", json={
        "employee_id": amy.id, "date": (monday + timedelta(days=1)).isoformat(), "start_time": "16:00",
        "end_time": "23:30", "break_minutes": 30, "role": "front bar"}, headers=mgr)
    assert rest.status_code == 200
    assert any("rest" in w["message"] for w in rest.json()["warnings"]) is False
    early = c.post(f"/rosters/{r.id}/shifts", json={
        "employee_id": amy.id, "date": (monday + timedelta(days=3)).isoformat(), "start_time": "06:00",
        "end_time": "11:00", "break_minutes": 30, "role": "front bar"}, headers=mgr)
    assert early.status_code == 200
    assert any("rest" in w["message"] for w in early.json()["warnings"])


# ---------------------------------------------------------------------------
# who may edit
# ---------------------------------------------------------------------------

def test_department_manager_edits_only_their_own_areas():
    c, mgr, vid, _, monday, amy, ben, r = _base()
    db = get_db()
    thu = monday + timedelta(days=3)
    kitchen = c.post(f"/rosters/{r.id}/shifts", json={
        "employee_id": ben.id, "date": thu.isoformat(), "start_time": "11:00",
        "end_time": "17:00", "break_minutes": 30, "role": "kitchen"}, headers=mgr).json()["shift"]["id"]

    bar_mgr, _ = _user(c, vid, role="manager", grants={vid: ["front bar", "sports bar", "bottle shop"]})
    fri = (monday + timedelta(days=4)).isoformat()

    mine = c.post(f"/rosters/{r.id}/shifts", json={
        "employee_id": amy.id, "date": fri, "start_time": "17:00", "end_time": "23:00",
        "break_minutes": 30, "role": "Sports Bar"}, headers=bar_mgr)
    assert mine.status_code == 200, mine.text
    assert c.put(f"/rosters/{r.id}/shifts/{vid}-s1", json={"start_time": "18:00"},
                 headers=bar_mgr).status_code == 200

    assert c.post(f"/rosters/{r.id}/shifts", json={
        "employee_id": ben.id, "date": fri, "start_time": "10:00", "end_time": "15:00",
        "role": "kitchen"}, headers=bar_mgr).status_code == 403
    assert c.put(f"/rosters/{r.id}/shifts/{kitchen}", json={"start_time": "12:00"},
                 headers=bar_mgr).status_code == 403
    assert c.put(f"/rosters/{r.id}/shifts/{vid}-s1", json={"role": "kitchen"},
                 headers=bar_mgr).status_code == 403
    assert c.delete(f"/rosters/{r.id}/shifts/{kitchen}", headers=bar_mgr).status_code == 403
    assert c.post(f"/rosters/{r.id}/copy-to", headers=bar_mgr).status_code == 403
    assert kitchen in {s.id for s in db.get_roster(r.id).shifts}
    assert next(s for s in db.get_roster(r.id).shifts if s.id == f"{vid}-s1").role == "front bar"

    assert c.delete(f"/rosters/{r.id}/shifts/{mine.json()['shift']['id']}", headers=bar_mgr).status_code == 200

    staff, _ = _user(c, vid)
    assert c.post(f"/rosters/{r.id}/shifts", json={
        "employee_id": amy.id, "date": fri, "start_time": "10:00", "end_time": "15:00",
        "role": "front bar"}, headers=staff).status_code == 403
    assert c.delete(f"/rosters/{r.id}/shifts/{vid}-s1", headers=staff).status_code == 403
    assert c.get(f"/rosters/{r.id}/changes", headers=staff).status_code == 403

    other_mgr, _, _ = _manager_with_venue(c)
    assert c.delete(f"/rosters/{r.id}/shifts/{vid}-s1", headers=other_mgr).status_code == 404
    assert c.post(f"/rosters/{r.id}/copy-to", headers=other_mgr).status_code == 404
    assert len(db.get_roster(r.id).shifts) == 2


def test_superseded_roster_cannot_be_edited():
    c, mgr, vid, _, monday, amy, ben, r = _base()
    get_db().update_roster_state(r.id, "superseded", "replaced", "system")
    resp = c.put(f"/rosters/{r.id}/shifts/{vid}-s1", json={"start_time": "18:00"}, headers=mgr)
    assert resp.status_code == 409


# ---------------------------------------------------------------------------
# a published roster
# ---------------------------------------------------------------------------

def test_editing_a_published_roster_keeps_it_live_and_tracks_changes():
    c, mgr, vid, mgr_id, monday, amy, ben, r = _base()
    db = get_db()
    db.update_roster_state(r.id, "published", "Published", "system")
    staff, staff_email = _user(c, vid)
    db.save_employee(db.get_employee(amy.id).model_copy(update={"email": staff_email}))

    def my_times():
        return [(s["date"], s["start"]) for s in c.get("/api/me/shifts", headers=staff).json()["shifts"]]

    wed = (monday + timedelta(days=2)).isoformat()
    assert my_times() == [(wed, "17:00")]

    resp = c.put(f"/rosters/{r.id}/shifts/{vid}-s1", json={"start_time": "18:00"}, headers=mgr)
    assert resp.status_code == 200, resp.text
    assert resp.json()["state"] == "published"
    assert resp.json()["changed_since_publish"] is True
    assert db.get_roster_state(r.id) == "published"
    assert my_times() == [(wed, "18:00")]

    rows = [e for e in db.list_audit_logs(vid, limit=500) if e["action"] == "roster.shift_update"]
    assert len(rows) == 1
    assert rows[0]["user_id"] == mgr_id and rows[0]["resource_id"] == r.id
    assert rows[0]["details"]["before"]["start"] == "17:00"
    assert rows[0]["details"]["after"]["start"] == "18:00"

    changes = c.get(f"/rosters/{r.id}/changes", headers=mgr).json()
    assert changes["changed_since_publish"] is True
    assert changes["pending_count"] == 1 and changes["affected_staff"] == 1

    # a no-op save is not a change
    same = c.put(f"/rosters/{r.id}/shifts/{vid}-s1", json={"start_time": "18:00"}, headers=mgr)
    assert same.json()["action"] == "unchanged" and same.json()["pending_changes"] == 1

    sent = c.post(f"/rosters/{r.id}/notify-changes", headers=mgr)
    assert sent.status_code == 200, sent.text
    assert sent.json()["notified_changes"] == 1 and sent.json()["staff_changed"] == 1
    assert "delivered" in sent.json()
    assert c.get(f"/rosters/{r.id}/changes", headers=mgr).json()["changed_since_publish"] is False

    # a new change is pending again; recalling and publishing afresh tells everyone
    thu = (monday + timedelta(days=3)).isoformat()
    added = c.post(f"/rosters/{r.id}/shifts", json={
        "employee_id": ben.id, "date": thu, "start_time": "11:00", "end_time": "17:00",
        "break_minutes": 30, "role": "kitchen"}, headers=mgr).json()
    assert added["changed_since_publish"] is True
    db.update_roster_state(r.id, "draft", "Recalled", "system")
    assert c.get(f"/rosters/{r.id}/changes", headers=mgr).json()["changed_since_publish"] is False
    db.update_roster_state(r.id, "published", "Published again", "system")
    assert c.get(f"/rosters/{r.id}/changes", headers=mgr).json()["changed_since_publish"] is False


def test_department_manager_notifies_only_their_own_changes():
    c, mgr, vid, _, monday, amy, ben, r = _base()
    db = get_db()
    db.update_roster_state(r.id, "published", "Published", "system")
    thu = (monday + timedelta(days=3)).isoformat()
    c.post(f"/rosters/{r.id}/shifts", json={"employee_id": ben.id, "date": thu, "start_time": "11:00",
                                            "end_time": "17:00", "break_minutes": 30, "role": "kitchen"},
           headers=mgr)
    c.put(f"/rosters/{r.id}/shifts/{vid}-s1", json={"end_time": "22:00"}, headers=mgr)
    bar_mgr, _ = _user(c, vid, role="manager", grants={vid: ["front bar"]})
    sent = c.post(f"/rosters/{r.id}/notify-changes", headers=bar_mgr)
    assert sent.status_code == 200, sent.text
    assert sent.json()["notified_changes"] == 1
    left = c.get(f"/rosters/{r.id}/changes", headers=mgr).json()
    assert left["pending_count"] == 1 and left["pending"][0]["after"]["role"] == "kitchen"


def test_imported_roster_is_staff_visible_so_edits_are_tracked():
    c = TestClient(app)
    mgr, vid, _ = _manager_with_venue(c)
    monday = _next_monday()
    amy = _emp(vid, "amy", "Amy Barker")
    r = _roster(vid, monday, [_shift(amy.id, monday + timedelta(days=2), time(17), time(23), sid=f"{vid}-s1")],
                rid=f"imported-{vid}-{monday.isoformat()}")
    resp = c.delete(f"/rosters/{r.id}/shifts/{vid}-s1", headers=mgr)
    assert resp.status_code == 200
    assert resp.json()["changed_since_publish"] is True
    sent = c.post(f"/rosters/{r.id}/notify-changes", headers=mgr).json()
    assert sent["staff_cancelled"] == 1 and sent["staff_changed"] == 0


# ---------------------------------------------------------------------------
# copy a week forward
# ---------------------------------------------------------------------------

def test_copy_last_week_into_a_new_draft_and_report_who_was_left_off(monkeypatch):
    c = TestClient(app)
    mgr, vid, _ = _manager_with_venue(c)
    db = get_db()
    this_monday = _next_monday() - timedelta(days=7)
    target = this_monday + timedelta(days=7)
    amy = _emp(vid, "amy", "Amy Barker")
    ben = _emp(vid, "ben", "Ben Leave")
    cat = _emp(vid, "cat", "Cat Busy", availability={"thursday": []})
    dan = _emp(vid, "dan", "Dan Gone")
    db.save_leave_request({"id": f"lv-{uuid.uuid4().hex[:6]}", "venue_id": vid, "employee_id": ben.id,
                           "start_date": target + timedelta(days=1), "end_date": target + timedelta(days=2),
                           "reason": "Holiday", "status": "approved", "created_at": datetime.utcnow()})
    source = _roster(vid, this_monday, [
        _shift(amy.id, this_monday + timedelta(days=4), time(17), time(23)),
        _shift(amy.id, this_monday + timedelta(days=5), time(16), time(1)),
        _shift(ben.id, this_monday + timedelta(days=1), time(11), time(17), role="kitchen"),
        _shift(ben.id, this_monday + timedelta(days=6), time(11), time(17), role="kitchen"),
        _shift(cat.id, this_monday + timedelta(days=3), time(17), time(23)),
        _shift(dan.id, this_monday + timedelta(days=4), time(17), time(23)),
    ], state="published")
    old_draft = _roster(vid, target, [_shift(amy.id, target, time(9), time(15))])
    _deactivate(monkeypatch, dan.id)

    resp = c.post(f"/rosters/{source.id}/copy-to?week_start={target.isoformat()}", headers=mgr)
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["week_start"] == target.isoformat()
    assert body["copied"] == 3
    assert body["skipped"] == []
    reasons = {(s["name"], s["reason"]) for s in body["left_open"]}
    assert reasons == {("Ben Leave", "on approved leave"), ("Cat Busy", "unavailable"), ("Dan Gone", "inactive")}
    assert body["superseded"] == [old_draft.id]

    new = db.get_roster(body["roster_id"])
    assert new.week_start == target and db.get_roster_state(new.id) == "draft"
    assert sorted((s.employee_id, s.date, s.start_time) for s in new.shifts) == [
        ("", target + timedelta(days=1), time(11)),
        ("", target + timedelta(days=3), time(17)),
        ("", target + timedelta(days=4), time(17)),
        (amy.id, target + timedelta(days=4), time(17)),
        (amy.id, target + timedelta(days=5), time(16)),
        (ben.id, target + timedelta(days=6), time(11)),
    ]
    assert not {s.id for s in new.shifts} & {s.id for s in source.shifts}
    assert all(s.cost and s.cost > 0 for s in new.shifts if s.employee_id)
    assert new.total_cost and new.total_cost > 0
    assert len(db.get_roster(source.id).shifts) == 6           # the source week is untouched
    assert db.get_roster_state(old_draft.id) == "superseded"

    # the copy is a draft: staff don't see it until it's published
    staff, staff_email = _user(c, vid)
    db.save_employee(db.get_employee(amy.id).model_copy(update={"email": staff_email}))
    mine = c.get("/api/me/shifts", headers=staff).json()["shifts"]
    assert all(s["date"] < target.isoformat() for s in mine)


def test_copy_defaults_to_next_week_and_never_overwrites_a_live_week():
    c = TestClient(app)
    mgr, vid, _ = _manager_with_venue(c)
    db = get_db()
    monday = _next_monday()
    amy = _emp(vid, "amy", "Amy Barker")
    source = _roster(vid, monday, [_shift(amy.id, monday, time(17), time(23))])

    assert c.post(f"/rosters/{source.id}/copy-to?week_start={(monday + timedelta(days=3)).isoformat()}",
                  headers=mgr).status_code == 400

    nxt = c.post(f"/rosters/{source.id}/copy-to", headers=mgr)
    assert nxt.status_code == 200, nxt.text
    assert nxt.json()["week_start"] == (monday + timedelta(days=7)).isoformat()

    # a week staff can already see is edited in place, not replaced
    db.update_roster_state(nxt.json()["roster_id"], "published", "Published", "system")
    again = c.post(f"/rosters/{source.id}/copy-to", headers=mgr)
    assert again.status_code == 409


# ---------------------------------------------------------------------------
# demo and storage
# ---------------------------------------------------------------------------

def test_demo_reset_clears_the_demo_venues_change_records():
    from rosteriq.services.demo import DEMO_VENUE_ID
    from rosteriq.services.demo_reset import PG_RULES, reset_demo_venue
    db = get_db()
    db.save_to_key("roster_changes:demo-r", {"venue_id": DEMO_VENUE_ID, "roster_id": "demo-r", "changes": []})
    db.save_to_key("roster_changes:real-r", {"venue_id": "real", "roster_id": "real-r", "changes": []})
    reset_demo_venue(db, force=True)
    assert db.load_from_key("roster_changes:demo-r") is None
    assert db.load_from_key("roster_changes:real-r") is not None
    assert any(label == "roster change records" for label, _, _ in PG_RULES)


def test_roster_lock_takes_a_cross_worker_lock_on_postgres():
    from rosteriq.services import roster_edit

    calls = []

    class _Cur:
        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def execute(self, sql, params):
            calls.append((sql.split("(")[0].split()[-1], params[1]))

        def fetchone(self):
            return {"got": True}

    class PostgresStore:
        def _cursor(self):
            return _Cur()

    with roster_edit.roster_lock(PostgresStore(), "r-1"):
        assert calls == [("pg_try_advisory_lock", "r-1")]
    assert calls[-1] == ("pg_advisory_unlock", "r-1")


# ---------------------------------------------------------------------------
# the Roster page
# ---------------------------------------------------------------------------

from rosteriq.tests.test_wembley_a6 import _dash, _fns, _run, needs_node  # noqa: E402

GRID_STUBS = r"""
global.state = { currentVenue: 'wem', isDemo: false, currentRosterId: null, currentRoster: null,
  employees: [{ id: 'e1', name: '<img src=x onerror=alert(1)>' }, { id: 'e2', name: 'Ben' }],
  venues: [{ id: 'wem', name: 'The Wembley Hotel', areas: [{ name: 'front bar' }] }] };
global.rosterSection = '';
global.rosterSetSection = function () {};
global.rosterWeekOffset = 0;
global.renderRosterChanges = function (info) { log.push({ banner: info }); };
global.fetchAPI = async function (url, opts) {
  calls.push({ url: url, method: opts && opts.method });
  return global.__resp[url.split('?')[0]] || {};
};
global.__resp = {};
"""

ROSTER = {
    "id": "r1", "venue_id": "wem", "week_start": "2026-10-12", "total_cost": "812.40",
    "shifts": [
        {"id": "s1", "employee_id": "e1", "date": "2026-10-14", "start_time": "11:00:00",
         "end_time": "15:00:00", "role": "front bar\"><script>x</script>", "break_minutes": 0},
        {"id": "s2", "employee_id": "e1", "date": "2026-10-14", "start_time": "18:00:00",
         "end_time": "23:00:00", "role": "front bar", "break_minutes": 30},
        {"id": "s'3", "employee_id": "e2", "date": "2026-10-16", "start_time": "17:00:00",
         "end_time": "23:00:00", "role": "kitchen", "break_minutes": 30},
    ],
}


@needs_node
def test_grid_renders_split_shifts_as_editable_and_escapes_everything():
    import json
    js = _fns(_dash(), "rosterWeekDays", "escAttr", "localIso", "rosterPeople", "renderRosterTable")
    data = _run(GRID_STUBS + js + r"""
function renderRosterDayView() {}   // the Day view has its own tests
async function main() {
  const tbl = el('roster-table');
  let foot = null;
  tbl.querySelector = function (sel) { return sel === 'tfoot' ? foot : null; };
  tbl.appendChild = function (c) { foot = c; return c; };
  renderRosterTable(""" + json.dumps(ROSTER) + r""");
  console.log(JSON.stringify({ body: el('roster-body').innerHTML, foot: foot ? foot.innerHTML : '',
                               id: state.currentRosterId, cost: el('roster-total-cost').textContent }));
}
""")
    body = data["body"]
    assert data["id"] == "r1" and data["cost"] == "812"
    assert "<img" not in body and "<script>" not in body
    assert "&lt;img src=x onerror=alert(1)&gt;" in body
    assert body.count('class="shift-cell shift-optimal roster-shift"') == 3
    assert 'data-shift-id="s1"' in body and 'data-shift-id="s2"' in body
    assert 'data-shift-id="s&#39;3"' in body
    assert body.count('class="roster-add"') == 14          # one per person per day
    assert 'data-day="2026-10-18"' in body                   # Sunday of the week
    assert data["foot"].count('class="roster-add"') == 7     # add-anyone row


@needs_node
def test_copy_last_week_posts_the_previous_weeks_roster_into_the_week_on_screen():
    js = _fns(_dash(), "localIso", "getWeekMonday", "rosterWeekDays", "rosterDayLabel", "copyLastWeek")
    data = _run(GRID_STUBS + r"""
global.renderRosterTable = function (r) { log.push({ render: r.id }); };
global.renderCopyReport = function (r) { log.push({ report: r.copied }); };
""" + js + r"""
async function main() {
  const target = localIso(getWeekMonday(0));
  const source = localIso(getWeekMonday(-1));
  __resp['/rosters'] = { items: [{ id: 'src-1', venue_id: 'wem' }] };
  __resp['/rosters/src-1/copy-to'] = { roster_id: 'new-1', roster: { id: 'new-1' }, copied: 12, skipped: [] };
  await copyLastWeek();
  console.log(JSON.stringify({ calls: calls, log: log, confirms: confirms, target: target, source: source,
                               id: state.currentRosterId }));
}
""")
    assert data["calls"][0]["url"].endswith("&week_start=" + data["source"] + "&current=true")
    assert data["calls"][1] == {"url": "/rosters/src-1/copy-to?week_start=" + data["target"], "method": "POST"}
    assert len(data["confirms"]) == 1
    assert data["id"] == "new-1"
    assert {"render": "new-1"} in data["log"] and {"report": 12} in data["log"]
