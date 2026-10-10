"""
Leave and shift cover through the roster's life at the Wembley.

[6]  Approved leave and an approved cover reach a newer, unpublished draft of
     the week as well as the roster staff see, and publishing a roster that
     puts someone on a day of their approved leave is refused before staff
     see it.
[9]  A shift left open while its roster isn't on staff phones goes on the
     cover board when that roster is published (so does any other open
     shift), and the leave decision says that instead of "posted for cover".
[14] Leave opening a shift and an approved cover keep the roster's stored
     total cost current.
[15]/[37] Both save the roster under its lock, from a copy read inside it.
[7]/[41] A cover whose shift was filled, retimed or removed since it was
     posted is never approved: approval is refused and the cover withdrawn,
     and the board, the manager's list and the briefing stop counting it.
[17] The paid-leave default for a shift that had already started is the
     rostered hours less what the person punched that day.
[24] The paid-leave default counts a day once when the imported roster and a
     newer draft share the week.
"""

import uuid
from datetime import datetime, time as dtime, timedelta
from decimal import Decimal

from rosteriq.cost_calculator import calculate_roster_cost
from rosteriq.database import get_db
from rosteriq.models import Roster, State
from rosteriq.services import roster_edit as edit
from rosteriq.services.clock import venue_today
from rosteriq.tests.test_wembley_b7 import (
    _detail, _monday, _next_week, _publish, _roster_shift, _shift, _world,
)


def _draft(vid, monday, shifts, rid=None):
    """An unpublished roster for the week, saved after any roster already there."""
    rid = rid or f"g3d-{uuid.uuid4().hex[:6]}"
    get_db().save_roster(Roster(id=rid, venue_id=vid, week_start=monday,
                                week_end=monday + timedelta(days=6), shifts=shifts,
                                total_cost=None,
                                created_at=datetime.utcnow() + timedelta(minutes=5)))
    return rid


def _publish_api(c, mgr, rid):
    r = c.post(f"/api/v1/rosters/{rid}/publish", json={"skip_approval": True}, headers=mgr)
    assert r.status_code == 200, r.text
    return r.json()


def _ask_leave(c, mgr, staff_hdr, vid, day, leave_type="annual"):
    r = c.post("/api/me/leave", json={"start_date": day.isoformat(), "end_date": day.isoformat(),
                                      "leave_type": leave_type}, headers=staff_hdr)
    assert r.status_code == 200, r.text
    dec = c.post(f"/api/leave/{r.json()['request_id']}/decide",
                 json={"venue_id": vid, "approve": True}, headers=mgr)
    assert dec.status_code == 200, dec.text
    return dec.json()


def _record_leave(c, mgr, vid, emp_id, first, last=None, leave_type="annual", **extra):
    r = c.post("/api/leave", json={"venue_id": vid, "employee_id": emp_id,
                                   "start_date": first.isoformat(),
                                   "end_date": (last or first).isoformat(),
                                   "leave_type": leave_type, **extra}, headers=mgr)
    assert r.status_code == 200, r.text
    return r.json()


def _covers(c, mgr, vid):
    return {x["shift_id"]: x for x in c.get(f"/api/cover?venue_id={vid}", headers=mgr).json()["covers"]}


def _grabs(c, hdr):
    return sorted(x["shift_id"] for x in c.get("/api/me/cover", headers=hdr).json()["up_for_grabs"])


# ---------------------------------------------------------------------------
# [6] A newer draft of the week, and the publish check
# ---------------------------------------------------------------------------

def test_approved_leave_also_opens_the_shift_on_a_newer_draft_and_publishing_it_keeps_it_open():
    c, mgr, vid, ids, staff, _ = _world()
    nm = _next_week(vid)
    wed = nm + timedelta(days=2)
    r1 = _publish(vid, nm, [_shift("jo-wed", ids["jo"], wed, dtime(10), dtime(17), "Front Bar")])
    r2 = _draft(vid, nm, [_shift("jo-wed-2", ids["jo"], wed, dtime(10), dtime(17), "Front Bar"),
                          _shift("sam-thu-2", ids["sam"], nm + timedelta(days=3), dtime(10),
                                 dtime(17), "Front Bar")])

    _ask_leave(c, mgr, staff["jo"], vid, wed)
    assert _roster_shift(r1, "jo-wed").employee_id == ""
    assert _roster_shift(r2, "jo-wed-2").employee_id == ""

    pub = _publish_api(c, mgr, r2)
    assert pub["success"] is True, pub
    assert c.get("/api/me/shifts", headers=staff["jo"]).json()["shifts"] == []
    # The old roster's cover went with it; the published shift is on the board
    covers = _covers(c, mgr, vid)
    assert covers["jo-wed"]["status"] == "withdrawn"
    assert (covers["jo-wed-2"]["status"], covers["jo-wed-2"]["origin"]) == ("open", "leave")
    assert _grabs(c, staff["pat"]) == ["jo-wed-2"]


def test_publishing_a_roster_that_puts_someone_on_their_approved_leave_is_refused():
    c, mgr, vid, ids, staff, _ = _world()
    db = get_db()
    yday = venue_today(vid, db) - timedelta(days=1)
    # A shift already worked on a leave day is history: it never blocks
    worked = _draft(vid, _monday(yday), [_shift("jo-yday", ids["jo"], yday, dtime(0), dtime(1),
                                                "Duty Manager")])
    _record_leave(c, mgr, vid, ids["jo"], yday, leave_type="personal", paid_hours=1)
    assert _roster_shift(worked, "jo-yday").employee_id == ids["jo"]
    assert _publish_api(c, mgr, worked)["success"] is True

    nm = _next_week(vid)
    thu = nm + timedelta(days=3)
    _record_leave(c, mgr, vid, ids["jo"], thu, paid_hours=7)
    rid = _draft(vid, nm, [_shift("jo-thu", ids["jo"], thu, dtime(10), dtime(17), "Front Bar")])
    pub = _publish_api(c, mgr, rid)
    assert pub["success"] is False and pub["state"] == "failed", pub
    assert "Jo Taylor" in pub["message"] and "approved leave" in pub["message"]
    assert "Thu 10am-5pm Front Bar" in pub["message"]
    assert [s["date"] for s in c.get("/api/me/shifts", headers=staff["jo"]).json()["shifts"]] == []


def test_leave_while_a_roster_is_mid_save(monkeypatch):
    from rosteriq.services import leave_shifts
    c, mgr, vid, ids, staff, _ = _world()
    nm = _next_week(vid)
    wed, thu = nm + timedelta(days=2), nm + timedelta(days=3)
    r1 = _publish(vid, nm, [_shift("jo-wed", ids["jo"], wed, dtime(10), dtime(17), "Front Bar"),
                            _shift("sam-thu", ids["sam"], thu, dtime(10), dtime(17), "Front Bar")])
    r2 = _draft(vid, nm, [_shift("jo-wed-2", ids["jo"], wed, dtime(10), dtime(17), "Front Bar")])
    busy = set()
    real = leave_shifts.roster_lock

    def lock(db, roster_id, *a, **k):
        if roster_id in busy:
            raise edit.RosterBusy()
        return real(db, roster_id, *a, **k)

    monkeypatch.setattr(leave_shifts, "roster_lock", lock)

    # The roster staff see is mid-save: try again, nothing changed
    busy.add(r1)
    r = c.post("/api/leave", json={"venue_id": vid, "employee_id": ids["sam"],
                                   "start_date": thu.isoformat(), "end_date": thu.isoformat(),
                                   "leave_type": "annual"}, headers=mgr)
    assert r.status_code == 409 and "try again" in _detail(r)
    assert _roster_shift(r1, "sam-thu").employee_id == ids["sam"]

    # Only the newer draft is mid-save: the leave still lands where staff look,
    # and publishing the draft is refused while it rosters Jo on her leave
    busy.clear()
    busy.add(r2)
    _ask_leave(c, mgr, staff["jo"], vid, wed)
    assert _roster_shift(r1, "jo-wed").employee_id == ""
    assert _roster_shift(r2, "jo-wed-2").employee_id == ids["jo"]
    pub = _publish_api(c, mgr, r2)
    assert pub["success"] is False and "Jo Taylor is on approved leave" in pub["message"]


def test_an_approved_cover_also_moves_the_shift_on_a_newer_draft():
    c, mgr, vid, ids, staff, _ = _world()
    nm = _next_week(vid)
    fri = nm + timedelta(days=4)
    r1 = _publish(vid, nm, [_shift("jo-fri", ids["jo"], fri, dtime(16), dtime(23), "Duty Manager")])
    cover_id = c.post("/api/me/shifts/jo-fri/cover", json={}, headers=staff["jo"]).json()["cover_id"]
    r2 = _draft(vid, nm, [_shift("jo-fri-2", ids["jo"], fri, dtime(16), dtime(23), "Duty Manager"),
                          _shift("jo-sat-2", ids["jo"], fri + timedelta(days=1), dtime(16),
                                 dtime(23), "Duty Manager")])
    assert c.post(f"/api/me/cover/{cover_id}/claim", headers=staff["meca"]).status_code == 200
    r = c.post(f"/api/cover/{cover_id}/decide", json={"venue_id": vid, "approve": True}, headers=mgr)
    assert r.status_code == 200, r.text
    assert _roster_shift(r1, "jo-fri").employee_id == ids["meca"]
    assert _roster_shift(r2, "jo-fri-2").employee_id == ids["meca"]
    assert _roster_shift(r2, "jo-sat-2").employee_id == ids["jo"]


# ---------------------------------------------------------------------------
# [9] Open shifts reach the cover board when their roster is published
# ---------------------------------------------------------------------------

def test_shifts_left_open_on_a_draft_go_on_the_cover_board_when_it_is_published():
    c, mgr, vid, ids, staff, _ = _world()
    nm = _next_week(vid)
    fri, sat, sun = nm + timedelta(days=4), nm + timedelta(days=5), nm + timedelta(days=6)
    rid = _draft(vid, nm, [
        _shift("jo-sat", ids["jo"], sat, dtime(16), dtime(23), "Duty Manager"),
        _shift("sam-fri", ids["sam"], fri, dtime(17), dtime(23), "Front Bar"),
        _shift("open-sun", "", sun, dtime(10), dtime(15), "Front Bar"),
    ])

    dec = _ask_leave(c, mgr, staff["jo"], vid, sat)
    assert [o["cover_id"] for o in dec["opened_shifts"]] == [None]
    assert "posted for cover" not in dec["message"]
    assert dec["message"] == ("Left open: Sat 4-11pm Duty Manager — it goes on the cover board "
                              "when the roster is published.")
    rec = _record_leave(c, mgr, vid, ids["sam"], fri)
    assert "posted for cover" not in rec["message"]
    assert "Left open: Fri 5-11pm Front Bar — it goes on the cover board when the roster is " \
           "published." in rec["message"]
    assert rec["paid_hours"] == 6.0
    assert c.get(f"/api/cover?venue_id={vid}", headers=mgr).json()["count"] == 0

    assert _publish_api(c, mgr, rid)["success"] is True
    assert _grabs(c, staff["meca"]) == ["jo-sat"]
    assert _grabs(c, staff["pat"]) == ["jo-sat", "open-sun", "sam-fri"]
    covers = _covers(c, mgr, vid)
    assert (covers["jo-sat"]["origin"], covers["jo-sat"]["requested_by_name"]) == ("leave", "Jo Taylor")
    assert (covers["sam-fri"]["origin"], covers["sam-fri"]["requested_by"]) == ("leave", ids["sam"])
    assert (covers["open-sun"]["origin"], covers["open-sun"]["requested_by_name"]) == ("open", "Open shift")
    # shift_covers.requested_by is NOT NULL in Postgres
    assert get_db().get_shift_cover(covers["open-sun"]["id"])["requested_by"] == ""

    # Republishing after a recall posts nothing twice
    assert c.post(f"/api/v1/rosters/{rid}/recall", json={"reason": "fix"}, headers=mgr).status_code == 200
    assert _publish_api(c, mgr, rid)["success"] is True
    assert c.get(f"/api/cover?venue_id={vid}", headers=mgr).json()["count"] == 3

    # The open shift fills like any other
    assert c.post(f"/api/me/cover/{covers['open-sun']['id']}/claim", headers=staff["pat"]).status_code == 200
    ok = c.post(f"/api/cover/{covers['open-sun']['id']}/decide",
                json={"venue_id": vid, "approve": True}, headers=mgr)
    assert ok.status_code == 200, ok.text
    assert _roster_shift(rid, "open-sun").employee_id == ids["pat"]


# ---------------------------------------------------------------------------
# [14] / [15] / [37] Stored total and the roster lock
# ---------------------------------------------------------------------------

def test_leave_and_an_approved_cover_keep_the_roster_total_cost_current():
    c, mgr, vid, ids, staff, _ = _world(extra={"pat": {"hourly_base_rate": "45.00"}})
    nm = _next_week(vid)
    thu, fri = nm + timedelta(days=3), nm + timedelta(days=4)
    rid = _publish(vid, nm, [
        _shift("jo-thu", ids["jo"], thu, dtime(16), dtime(23), "Duty Manager"),
        _shift("sam-fri", ids["sam"], fri, dtime(17), dtime(23), "Front Bar"),
    ])
    db = get_db()
    emps = {e.id: e for e in db.get_employees(vid)}

    def fresh():
        r = db.get_roster(rid)
        return calculate_roster_cost(r.model_copy(update={"shifts": [
            s for s in r.shifts if s.employee_id in emps]}), emps, State.wa)

    roster = db.get_roster(rid)
    edit.recost_total(roster, emps, State.wa)
    db.save_roster(roster)
    published = db.get_roster(rid).total_cost

    _record_leave(c, mgr, vid, ids["jo"], thu)
    after_leave = db.get_roster(rid).total_cost
    assert after_leave == fresh() and after_leave < published

    cover_id = c.post("/api/me/shifts/sam-fri/cover", json={}, headers=staff["sam"]).json()["cover_id"]
    assert c.post(f"/api/me/cover/{cover_id}/claim", headers=staff["pat"]).status_code == 200
    assert c.post(f"/api/cover/{cover_id}/decide", json={"venue_id": vid, "approve": True},
                  headers=mgr).status_code == 200
    after_cover = db.get_roster(rid).total_cost
    assert after_cover == fresh() and after_cover > after_leave
    assert Decimal(str(c.get(f"/rosters/{rid}", headers=mgr).json()["total_cost"])) == after_cover


def test_leave_and_cover_approval_save_the_roster_under_its_lock(monkeypatch):
    c, mgr, vid, ids, staff, _ = _world()
    nm = _next_week(vid)
    fri, sat = nm + timedelta(days=4), nm + timedelta(days=5)
    rid = _publish(vid, nm, [
        _shift("jo-fri", ids["jo"], fri, dtime(16), dtime(23), "Duty Manager"),
        _shift("jo-sat", ids["jo"], sat, dtime(16), dtime(23), "Duty Manager"),
    ])
    db = get_db()
    seen = []
    real_get, real_save = db.get_roster, db.save_roster

    def get(roster_id):
        seen.append(("get", roster_id, edit._lock.locked()))
        return real_get(roster_id)

    def save(roster):
        seen.append(("save", roster.id, edit._lock.locked()))
        return real_save(roster)

    monkeypatch.setattr(db, "get_roster", get)
    monkeypatch.setattr(db, "save_roster", save)

    _record_leave(c, mgr, vid, ids["jo"], sat)
    cover_id = c.post("/api/me/shifts/jo-fri/cover", json={}, headers=staff["jo"]).json()["cover_id"]
    assert c.post(f"/api/me/cover/{cover_id}/claim", headers=staff["meca"]).status_code == 200
    assert c.post(f"/api/cover/{cover_id}/decide", json={"venue_id": vid, "approve": True},
                  headers=mgr).status_code == 200

    saves = [i for i, x in enumerate(seen) if x[0] == "save" and x[1] == rid]
    assert len(saves) == 2
    for i in saves:
        assert seen[i][2], "saved without the roster lock"
        reads = [x for x in seen[:i] if x[0] == "get" and x[1] == rid]
        assert reads and reads[-1][2], "saved a copy read outside the lock"
    assert _roster_shift(rid, "jo-sat").employee_id == ""
    assert _roster_shift(rid, "jo-fri").employee_id == ids["meca"]


def test_cover_approval_while_the_roster_is_being_saved_asks_to_try_again(monkeypatch):
    c, mgr, vid, ids, staff, _ = _world()
    nm = _next_week(vid)
    fri = nm + timedelta(days=4)
    rid = _publish(vid, nm, [_shift("jo-fri", ids["jo"], fri, dtime(16), dtime(23), "Duty Manager")])
    cover_id = c.post("/api/me/shifts/jo-fri/cover", json={}, headers=staff["jo"]).json()["cover_id"]
    assert c.post(f"/api/me/cover/{cover_id}/claim", headers=staff["meca"]).status_code == 200

    class _Held:
        def acquire(self, timeout=None):
            return False

        def locked(self):
            return True

    monkeypatch.setattr(edit, "_lock", _Held())
    r = c.post(f"/api/cover/{cover_id}/decide", json={"venue_id": vid, "approve": True}, headers=mgr)
    assert r.status_code == 409 and "try again" in _detail(r)
    assert get_db().get_shift_cover(cover_id)["status"] == "claimed"
    assert _roster_shift(rid, "jo-fri").employee_id == ids["jo"]


# ---------------------------------------------------------------------------
# [7] / [41] A cover whose shift changed is never approved
# ---------------------------------------------------------------------------

def _leave_three_days(c, mgr, vid, ids):
    nm = _next_week(vid)
    thu, fri, sat = nm + timedelta(days=3), nm + timedelta(days=4), nm + timedelta(days=5)
    rid = _publish(vid, nm, [
        _shift("jo-thu", ids["jo"], thu, dtime(10), dtime(17), "Front Bar"),
        _shift("jo-fri", ids["jo"], fri, dtime(10), dtime(17), "Front Bar"),
        _shift("jo-sat", ids["jo"], sat, dtime(10), dtime(17), "Front Bar"),
    ])
    _record_leave(c, mgr, vid, ids["jo"], thu, sat)
    return rid


def _change_underneath(rid, ids):
    """Fill Thursday, retime Friday, drop Saturday, outside the cover flow."""
    db = get_db()
    roster = db.get_roster(rid).model_copy(deep=True)
    for s in roster.shifts:
        if s.id == "jo-thu":
            s.employee_id = ids["sam"]
        elif s.id == "jo-fri":
            s.start_time = dtime(11)
    roster.shifts = [s for s in roster.shifts if s.id != "jo-sat"]
    db.save_roster(roster)


def test_a_claimed_cover_whose_shift_changed_is_refused_and_withdrawn():
    c, mgr, vid, ids, staff, _ = _world()
    rid = _leave_three_days(c, mgr, vid, ids)
    up = {x["shift_id"]: x["id"] for x in
          c.get("/api/me/cover", headers=staff["pat"]).json()["up_for_grabs"]}
    assert sorted(up) == ["jo-fri", "jo-sat", "jo-thu"]
    for cid in up.values():
        assert c.post(f"/api/me/cover/{cid}/claim", headers=staff["pat"]).status_code == 200
    _change_underneath(rid, ids)

    # Not counted as waiting on the manager, nor shown as ready to approve
    attention = c.get(f"/api/briefing?venue_id={vid}", headers=mgr).json()["attention"]
    assert not any("cover claim" in line for line in attention), attention
    assert {k: v["status"] for k, v in _covers(c, mgr, vid).items()} == \
        {"jo-thu": "withdrawn", "jo-fri": "withdrawn", "jo-sat": "withdrawn"}
    mine = c.get("/api/me/cover", headers=staff["pat"]).json()["claimed_by_me"]
    assert [x["status"] for x in mine] == ["withdrawn"] * 3

    db = get_db()
    for sid, cid in up.items():
        r = c.post(f"/api/cover/{cid}/decide", json={"venue_id": vid, "approve": True}, headers=mgr)
        assert r.status_code == 409, (sid, r.text)
        assert "changed" in _detail(r) and "withdrawn" in _detail(r)
        assert db.get_shift_cover(cid)["status"] == "withdrawn"
    assert _roster_shift(rid, "jo-thu").employee_id == ids["sam"]
    fri = _roster_shift(rid, "jo-fri")
    assert (fri.employee_id, fri.start_time) == ("", dtime(11))
    assert [e["details"]["decision"] for e in db.list_events(venue_id=vid, action_prefix="cover.decide")] \
        == ["withdraw"] * 3


def test_an_open_cover_whose_shift_changed_leaves_the_board_and_the_briefing():
    c, mgr, vid, ids, staff, _ = _world()
    rid = _leave_three_days(c, mgr, vid, ids)
    up = {x["shift_id"]: x["id"] for x in
          c.get("/api/me/cover", headers=staff["pat"]).json()["up_for_grabs"]}
    _change_underneath(rid, ids)

    assert _grabs(c, staff["pat"]) == []
    attention = c.get(f"/api/briefing?venue_id={vid}", headers=mgr).json()["attention"]
    assert not any("up for cover" in line for line in attention), attention
    assert c.get(f"/api/cover?venue_id={vid}&status=open", headers=mgr).json()["count"] == 0
    for cid in up.values():
        r = c.post(f"/api/me/cover/{cid}/claim", headers=staff["pat"])
        assert r.status_code == 409, r.text


def test_a_staff_cover_still_stands_once_the_requesters_own_leave_leaves_it_open():
    c, mgr, vid, ids, staff, _ = _world()
    nm = _next_week(vid)
    fri = nm + timedelta(days=4)
    rid = _publish(vid, nm, [_shift("jo-fri", ids["jo"], fri, dtime(16), dtime(23), "Duty Manager")])
    cover_id = c.post("/api/me/shifts/jo-fri/cover", json={}, headers=staff["jo"]).json()["cover_id"]
    _record_leave(c, mgr, vid, ids["jo"], fri)
    assert _roster_shift(rid, "jo-fri").employee_id == ""
    assert _grabs(c, staff["meca"]) == ["jo-fri"]
    assert c.post(f"/api/me/cover/{cover_id}/claim", headers=staff["meca"]).status_code == 200
    r = c.post(f"/api/cover/{cover_id}/decide", json={"venue_id": vid, "approve": True}, headers=mgr)
    assert r.status_code == 200, r.text
    assert _roster_shift(rid, "jo-fri").employee_id == ids["meca"]


# ---------------------------------------------------------------------------
# [17] / [24] The paid-leave default
# ---------------------------------------------------------------------------

def _impact(c, mgr, vid, emp_id, day):
    r = c.get(f"/api/leave/impact?venue_id={vid}&employee_id={emp_id}&start_date={day}"
              f"&end_date={day}&leave_type=personal", headers=mgr)
    assert r.status_code == 200, r.text
    return r.json()


def test_paid_leave_for_a_shift_already_part_worked_defaults_to_the_hours_not_worked():
    c, mgr, vid, ids, staff, _ = _world()
    db = get_db()
    yday = venue_today(vid, db) - timedelta(days=1)
    _publish(vid, _monday(yday), [
        _shift("jo-yday", ids["jo"], yday, dtime(16), dtime(23), "Duty Manager", brk=30),
        _shift("sam-yday", ids["sam"], yday, dtime(17), dtime(23), "Front Bar"),
    ])
    # Jo worked 16:00-18:00 local (punches are naive UTC) and went home sick
    db.save_timesheet({"id": f"ts-{uuid.uuid4().hex[:6]}", "venue_id": vid,
                       "employee_id": ids["jo"], "work_date": yday,
                       "clock_in": datetime.combine(yday, dtime(8)).isoformat(),
                       "clock_out": datetime.combine(yday, dtime(10)).isoformat(),
                       "break_minutes": 0, "status": "approved"})

    imp = _impact(c, mgr, vid, ids["jo"], yday)
    assert imp["rostered_hours"] == 6.5
    assert imp["paid_hours_default"] == 4.5
    assert "they worked 2h that day, so 4.5h of it counts as leave" in imp["message"]
    assert _record_leave(c, mgr, vid, ids["jo"], yday, leave_type="personal")["paid_hours"] == 4.5

    # Nobody punched for Sam: the whole rostered shift
    imp = _impact(c, mgr, vid, ids["sam"], yday)
    assert imp["paid_hours_default"] == 6.0
    assert "nothing was punched that day, so its 6h counts as leave" in imp["message"]


def test_paid_leave_default_counts_a_day_once_when_the_import_and_a_newer_draft_share_the_week():
    c, mgr, vid, ids, staff, _ = _world()
    nm = _next_week(vid)
    sat = nm + timedelta(days=5)
    db = get_db()
    imported = f"imported-{vid}-{nm.isoformat()}"
    db.save_roster(Roster(id=imported, venue_id=vid, week_start=nm, week_end=nm + timedelta(days=6),
                          shifts=[_shift("imp-jo-sat", ids["jo"], sat, dtime(16), dtime(23),
                                         "Front Bar", brk=30)],
                          total_cost=None, created_at=datetime.utcnow()))
    draft = _draft(vid, nm, [_shift("gen-jo-sat", ids["jo"], sat, dtime(17), dtime(23, 30),
                                    "Front Bar", brk=30)])

    imp = _impact(c, mgr, vid, ids["jo"], sat)
    assert len(imp["shifts"]) == 2
    assert imp["rostered_hours"] == 6.5 and imp["paid_hours_default"] == 6.5
    rec = _record_leave(c, mgr, vid, ids["jo"], sat)
    assert rec["paid_hours"] == 6.5
    assert _roster_shift(imported, "imp-jo-sat").employee_id == ""
    assert _roster_shift(draft, "gen-jo-sat").employee_id == ""
