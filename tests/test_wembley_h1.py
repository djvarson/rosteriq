"""
Approved leave after it is approved, and the Deputy paste around it.

[1]/[8] A manager can cancel approved leave, or shorten it to dates inside it
        (POST /api/leave/{id}/amend). The released days stop counting as leave
        everywhere that reads approved leave (the publish check, the
        generator, payroll prep); a defaulted paid-hours figure is worked out
        again over the new dates; the still-open covers posted for the
        released days are withdrawn; nobody is put back on a shift
        automatically. The publish refusal says how to change the leave, and
        the dashboard's approved leave rows carry Cancel leave and Change
        dates.
[12]    Recording or approving leave that overlaps the same person's approved
        leave is refused with a 409 naming that record, unless the manager
        says both are right (allow_overlap); the dashboard asks first.
[3]/[4] A pasted row for someone on approved leave that day, whose shift
        hasn't started, comes in as an open shift posted for cover and is
        flagged; a re-paste finds the slot their leave left open instead of
        adding them back, and a row for someone else in that slot fills it.
[7]     A paste into a week RosterIQ has published (or whose import a publish
        superseded) changes nothing for that week and says so plainly.
"""

import json
import os
import re
import shutil
import subprocess
import uuid
from datetime import date, datetime, time as dtime, timedelta

import pytest

from rosteriq.database import PostgresStore, get_db
from rosteriq.services import roster_edit as edit
from rosteriq.services.clock import venue_today
from rosteriq.services.current_roster import current_roster, roster_state
from rosteriq.services.leave_shifts import approved_paid_leave
from rosteriq.tests.test_wembley_b7 import (
    _PG, _detail, _login, _monday, _next_week, _publish, _roster_shift, _shift, _venue, _world,
)
from rosteriq.tests.test_wembley_g3 import _draft, _grabs, _publish_api, _record_leave


def _amend(c, mgr, vid, req_id, **body):
    return c.post(f"/api/leave/{req_id}/amend", json={"venue_id": vid, **body}, headers=mgr)


def _leave_covers(vid, *shift_ids):
    return {cv["shift_id"]: cv for cv in get_db().list_shift_covers(vid) or []
            if cv["shift_id"] in shift_ids}


# ---------------------------------------------------------------------------
# [1]/[8] Cancel or shorten approved leave
# ---------------------------------------------------------------------------

def test_cancelling_approved_leave_releases_every_day_and_withdraws_its_open_covers():
    from rosteriq.api import _approved_leave_map
    c, mgr, vid, ids, staff, mgr_email = _world()
    db = get_db()
    nm = _next_week(vid)
    wed, sat = nm + timedelta(days=2), nm + timedelta(days=5)
    rid = _publish(vid, nm, [_shift("jo-wed", ids["jo"], wed, dtime(10), dtime(17), "Front Bar"),
                             _shift("jo-sat", ids["jo"], sat, dtime(16), dtime(23), "Duty Manager")])
    lv = _record_leave(c, mgr, vid, ids["jo"], wed, sat)
    assert lv["paid_hours"] == 14.0
    covers = _leave_covers(vid, "jo-wed", "jo-sat")
    assert {k: v["status"] for k, v in covers.items()} == {"jo-wed": "open", "jo-sat": "open"}
    # Pat has already put a hand up for Wednesday: that claim waits for the manager
    assert c.post(f"/api/me/cover/{covers['jo-wed']['id']}/claim",
                  headers=staff["pat"]).status_code == 200

    r = _amend(c, mgr, vid, lv["request_id"], cancel=True, note="Trip called off")
    assert r.status_code == 200, r.text
    out = r.json()
    assert out["status"] == "cancelled" and out["paid_hours"] is None
    assert ("Still open on the days released: Wed 10am-5pm Front Bar, Sat 4-11pm Duty Manager"
            in out["message"])
    assert "back on the roster" in out["message"] and "claimed 1" in out["message"]

    stored = db.get_leave_request(lv["request_id"])
    mgr_id = db.get_user_by_email(mgr_email)["id"]
    assert (stored["status"], stored["decided_by"], stored["decision_note"]) == \
        ("cancelled", mgr_id, "Trip called off")
    assert stored["paid_hours"] is None and stored["decided_at"] is not None
    # The open cover stops; nobody is put back on the shifts
    assert {k: v["status"] for k, v in _leave_covers(vid, "jo-wed", "jo-sat").items()} == \
        {"jo-wed": "claimed", "jo-sat": "withdrawn"}
    assert _grabs(c, staff["pat"]) == []
    assert _roster_shift(rid, "jo-sat").employee_id == ""
    assert _roster_shift(rid, "jo-wed").employee_id == ""

    # Every reader of approved leave follows
    assert ids["jo"] not in edit.approved_leave(db, vid)
    assert ids["jo"] not in _approved_leave_map(vid)
    assert approved_paid_leave(db.list_leave_requests(vid), nm, nm + timedelta(days=6)) == {}
    listed = c.get(f"/api/leave?venue_id={vid}&status=cancelled", headers=mgr).json()["requests"]
    assert [x["id"] for x in listed] == [lv["request_id"]]
    mine = c.get("/api/me/leave", headers=staff["jo"]).json()["requests"]
    assert [x["status"] for x in mine] == ["cancelled"]
    # A draft that puts Jo back on Saturday now publishes
    d = _draft(vid, nm, [_shift("jo-sat-2", ids["jo"], sat, dtime(16), dtime(23), "Duty Manager")])
    assert _publish_api(c, mgr, d)["success"] is True

    rows = db.list_events(venue_id=vid, action_prefix="leave.amend")
    assert len(rows) == 1
    details = rows[0]["details"]
    assert details["change"] == "cancel" and rows[0]["user_id"] == mgr_id
    assert (details["old_start_date"], details["old_end_date"]) == (wed.isoformat(), sat.isoformat())
    assert details["start_date"] is None and float(details["old_paid_hours"]) == 14.0
    assert details["paid_hours"] is None and details["note"] == "Trip called off"

    # Cancelled leave can't be changed again
    again = _amend(c, mgr, vid, lv["request_id"], cancel=True)
    assert again.status_code == 409 and "cancelled" in _detail(again)


def test_shortening_approved_leave_releases_only_the_days_cut_off():
    c, mgr, vid, ids, staff, _ = _world()
    db = get_db()
    nm = _next_week(vid)
    wed, thu, sat = (nm + timedelta(days=n) for n in (2, 3, 5))
    _publish(vid, nm, [_shift("jo-wed", ids["jo"], wed, dtime(10), dtime(17), "Front Bar"),
                       _shift("jo-sat", ids["jo"], sat, dtime(16), dtime(23), "Duty Manager")])
    lv = _record_leave(c, mgr, vid, ids["jo"], wed, sat)
    assert lv["paid_hours"] == 14.0

    # Back early: on leave Wednesday and Thursday only
    r = _amend(c, mgr, vid, lv["request_id"], end_date=thu.isoformat(), note="Back Friday")
    assert r.status_code == 200, r.text
    out = r.json()
    assert out["status"] == "approved"
    assert (out["start_date"], out["end_date"]) == (wed.isoformat(), thu.isoformat())
    # The defaulted hours are worked out again: Wednesday's 7h is all that's left
    assert out["paid_hours"] == 7.0 and "Paid hours: 14h \u2192 7h" in out["message"]
    assert "Still open on the days released: Sat 4-11pm Duty Manager" in out["message"]
    assert "Wed 10am" not in out["message"]
    stored = db.get_leave_request(lv["request_id"])
    assert (str(stored["start_date"])[:10], str(stored["end_date"])[:10]) == \
        (wed.isoformat(), thu.isoformat())
    assert stored["status"] == "approved" and stored["paid_hours"] == 7.0

    covers = _leave_covers(vid, "jo-wed", "jo-sat")
    assert (covers["jo-wed"]["status"], covers["jo-sat"]["status"]) == ("open", "withdrawn")
    assert edit.approved_leave(db, vid)[ids["jo"]] == [(wed, thu)]
    paid = approved_paid_leave(db.list_leave_requests(vid), nm, nm + timedelta(days=6))
    assert [(t, str(h)) for t, h, _ in paid[ids["jo"]]] == [("annual", "7.00")]

    # Saturday is released; Wednesday is still leave, and the refusal says how to change it
    ok = _draft(vid, nm, [_shift("jo-sat-2", ids["jo"], sat, dtime(16), dtime(23), "Duty Manager")])
    assert _publish_api(c, mgr, ok)["success"] is True
    bad = _draft(vid, nm, [_shift("jo-wed-2", ids["jo"], wed, dtime(10), dtime(17), "Front Bar")])
    pub = _publish_api(c, mgr, bad)
    assert pub["success"] is False and "Jo Taylor is on approved leave" in pub["message"]
    assert "Leave page" in pub["message"] and "change its dates" in pub["message"]

    rows = db.list_events(venue_id=vid, action_prefix="leave.amend")
    details = rows[0]["details"]
    assert details["change"] == "shorten"
    assert (details["old_end_date"], details["end_date"]) == (sat.isoformat(), thu.isoformat())
    assert (float(details["old_paid_hours"]), float(details["paid_hours"])) == (14.0, 7.0)


def test_shortening_keeps_hours_the_manager_typed_unless_given_new_ones():
    c, mgr, vid, ids, staff, _ = _world()
    db = get_db()
    nm = _next_week(vid)
    mon, wed, fri = nm, nm + timedelta(days=2), nm + timedelta(days=4)
    _publish(vid, nm, [_shift("jo-wed", ids["jo"], wed, dtime(10), dtime(17), "Front Bar")])
    lv = _record_leave(c, mgr, vid, ids["jo"], mon, fri, paid_hours=38)
    r = _amend(c, mgr, vid, lv["request_id"], start_date=wed.isoformat())
    assert r.status_code == 200, r.text
    assert r.json()["paid_hours"] == 38 and "check" in r.json()["message"]
    r = _amend(c, mgr, vid, lv["request_id"], end_date=wed.isoformat(), paid_hours=7.6)
    assert r.status_code == 200, r.text
    assert db.get_leave_request(lv["request_id"])["paid_hours"] == 7.6

    # Unpaid leave never carries paid hours
    un = _record_leave(c, mgr, vid, ids["sam"], mon, fri, leave_type="unpaid")
    r = _amend(c, mgr, vid, un["request_id"], end_date=wed.isoformat(), paid_hours=5)
    assert r.status_code == 200 and r.json()["paid_hours"] is None




def test_amending_leave_is_for_the_venues_managers_and_approved_leave_only():
    c, mgr, vid, ids, staff, _ = _world()
    db = get_db()
    nm = _next_week(vid)
    wed, sat = nm + timedelta(days=2), nm + timedelta(days=5)
    req_id = _record_leave(c, mgr, vid, ids["jo"], wed, sat)["request_id"]

    assert _amend(c, staff["jo"], vid, req_id, cancel=True).status_code == 403
    # Another owner's manager: refused for this venue, not found through their own
    email = f"h1o{uuid.uuid4().hex[:8]}@x.com"
    vid2 = f"h1-{uuid.uuid4().hex[:6]}"
    _venue(c, _login(c, email), vid2, "Another Pub")
    other = _login(c, email)
    assert _amend(c, other, vid, req_id, cancel=True).status_code == 403
    assert _amend(c, other, vid2, req_id, cancel=True).status_code == 404

    # New dates sit inside the leave and change it
    for dates in ({"start_date": (wed - timedelta(days=1)).isoformat()},
                  {"end_date": (sat + timedelta(days=1)).isoformat()},
                  {"start_date": wed.isoformat(), "end_date": sat.isoformat()},
                  {"start_date": sat.isoformat(), "end_date": wed.isoformat()}):
        r = _amend(c, mgr, vid, req_id, **dates)
        assert r.status_code == 422, (dates, r.text)

    # Pending leave is decided, not amended
    p = c.post("/api/me/leave", json={"start_date": sat.isoformat(), "end_date": sat.isoformat()},
               headers=staff["sam"])
    assert p.status_code == 200, p.text
    r = _amend(c, mgr, vid, p.json()["request_id"], cancel=True)
    assert r.status_code == 409 and "pending" in _detail(r)
    assert db.get_leave_request(req_id)["status"] == "approved"
    assert db.get_leave_request(p.json()["request_id"])["status"] == "pending"


# ---------------------------------------------------------------------------
# [12] Leave over approved leave
# ---------------------------------------------------------------------------

def test_recording_leave_over_approved_leave_needs_the_manager_to_say_both_are_right():
    c, mgr, vid, ids, staff, _ = _world()
    db = get_db()
    yday = venue_today(vid, db) - timedelta(days=1)
    first = _record_leave(c, mgr, vid, ids["jo"], yday, leave_type="personal", paid_hours=4.5)
    body = {"venue_id": vid, "employee_id": ids["jo"], "start_date": yday.isoformat(),
            "end_date": yday.isoformat(), "leave_type": "personal"}

    again = c.post("/api/leave", json=body, headers=mgr)
    assert again.status_code == 409, again.text
    assert again.json()["error"]["code"] == "LEAVE_OVERLAP"
    msg = _detail(again)
    assert "Jo Taylor already has approved leave" in msg and yday.isoformat() in msg
    assert "Personal/carer's leave" in msg and "4.5h" in msg and "Approved" in msg
    jo = [r for r in db.list_leave_requests(vid) if r["employee_id"] == ids["jo"]]
    assert [r["id"] for r in jo] == [first["request_id"]]

    wide = c.post("/api/leave", json={**body, "start_date": (yday - timedelta(days=3)).isoformat()},
                  headers=mgr)
    assert wide.status_code == 409
    assert c.post("/api/leave", json={**body, "employee_id": ids["sam"]}, headers=mgr).status_code == 200

    # Half annual, half personal: both right, recorded when the manager says so
    both = c.post("/api/leave", json={**body, "leave_type": "annual", "paid_hours": 2,
                                      "allow_overlap": True}, headers=mgr)
    assert both.status_code == 200, both.text
    assert len([r for r in db.list_leave_requests(vid) if r["employee_id"] == ids["jo"]]) == 2

    # Cancelled leave is no longer in the way
    for r in db.list_leave_requests(vid):
        if r["employee_id"] == ids["jo"]:
            assert _amend(c, mgr, vid, r["id"], cancel=True).status_code == 200
    assert c.post("/api/leave", json=body, headers=mgr).status_code == 200


def test_approving_a_request_over_approved_leave_needs_the_manager_to_say_both_are_right():
    c, mgr, vid, ids, staff, _ = _world()
    db = get_db()
    nm = _next_week(vid)
    sat, sun = nm + timedelta(days=5), nm + timedelta(days=6)
    _record_leave(c, mgr, vid, ids["jo"], sat)
    p = c.post("/api/me/leave", json={"start_date": sat.isoformat(), "end_date": sun.isoformat(),
                                      "leave_type": "personal"}, headers=staff["jo"])
    assert p.status_code == 200, p.text
    req_id = p.json()["request_id"]

    dec = c.post(f"/api/leave/{req_id}/decide", json={"venue_id": vid, "approve": True},
                 headers=mgr)
    assert dec.status_code == 409, dec.text
    assert dec.json()["error"]["code"] == "LEAVE_OVERLAP"
    assert "Annual leave" in _detail(dec) and sat.isoformat() in _detail(dec)
    assert db.get_leave_request(req_id)["status"] == "pending"

    ok = c.post(f"/api/leave/{req_id}/decide",
                json={"venue_id": vid, "approve": True, "allow_overlap": True}, headers=mgr)
    assert ok.status_code == 200 and ok.json()["status"] == "approved", ok.text

    # Declining never needs it
    p2 = c.post("/api/me/leave", json={"start_date": sun.isoformat(), "end_date": sun.isoformat()},
                headers=staff["jo"])
    assert p2.status_code == 200, p2.text
    no = c.post(f"/api/leave/{p2.json()['request_id']}/decide",
                json={"venue_id": vid, "approve": False}, headers=mgr)
    assert no.status_code == 200 and no.json()["status"] == "declined"


# ---------------------------------------------------------------------------
# [3]/[4] The Deputy paste and approved leave
# ---------------------------------------------------------------------------

def _csv(rows):
    return "\n".join(["Name,Date,Start,End,Area,Break"] + [
        f"{n},{d:%d/%m/%Y},{s},{e},{a},{b}" for n, d, s, e, a, b in rows])


def _paste(c, mgr, vid, rows):
    r = c.post("/api/setup/import-roster", json={"venue_id": vid, "content": _csv(rows)},
               headers=mgr)
    assert r.status_code == 200, r.text
    return r.json()


def _day_shifts(rid, day):
    return sorted((s.start_time.strftime("%H:%M"), s.employee_id, s.role)
                  for s in get_db().get_roster(rid).shifts if s.date == day)


def _my_days(c, hdr):
    return [s["date"] for s in c.get("/api/me/shifts", headers=hdr).json()["shifts"]]


def _week_rows(fri, sat):
    return [("Jo Taylor", fri, "16:00", "23:00", "Duty Manager", 30),
            ("Jo Taylor", sat, "16:00", "23:00", "Duty Manager", 30),
            ("Sam Kelly", sat, "17:00", "23:00", "Front Bar", 0)]


def test_a_first_paste_leaves_open_the_shift_of_someone_on_approved_leave():
    c, mgr, vid, ids, staff, _ = _world()
    db = get_db()
    nm = _next_week(vid)
    fri, sat = nm + timedelta(days=4), nm + timedelta(days=5)
    _record_leave(c, mgr, vid, ids["jo"], sat)

    out = _paste(c, mgr, vid, _week_rows(fri, sat))
    assert out["shifts_imported"] == 3
    rid = f"imported-{vid}-{nm.isoformat()}"
    assert _day_shifts(rid, sat) == [("16:00", "", "Duty Manager"),
                                     ("17:00", ids["sam"], "Front Bar")]
    hole = next(s for s in db.get_roster(rid).shifts if s.date == sat and not s.employee_id)
    assert hole.cost is None and hole.break_minutes == 30
    assert [(f["name"], f["date"], f["flag"]) for f in out["flagged"]] == \
        [("Jo Taylor", sat.isoformat(), "on_leave")]
    assert "approved leave" in out["flagged"][0]["detail"]
    assert "left open" in out["flagged"][0]["detail"]
    assert out["flags_summary"] == {"on_leave": 1}

    assert _my_days(c, staff["jo"]) == [fri.isoformat()]
    cover = _leave_covers(vid, hole.id)[hole.id]
    assert (cover["status"], cover["origin"], cover["requested_by"]) == ("open", "leave", ids["jo"])
    assert _grabs(c, staff["pat"]) == [hole.id]

    # A leave day already started or past is history: imported as pasted
    yday = venue_today(vid, db) - timedelta(days=1)
    _record_leave(c, mgr, vid, ids["sam"], yday, leave_type="personal", paid_hours=7)
    past = _paste(c, mgr, vid, [("Sam Kelly", yday, "16:00", "23:00", "Front Bar", 0)])
    assert past["shifts_imported"] == 1 and past["flagged"] == []
    assert _day_shifts(f"imported-{vid}-{_monday(yday).isoformat()}", yday) == \
        [("16:00", ids["sam"], "Front Bar")]


def test_a_re_paste_keeps_the_slot_leave_left_open_and_its_cover():
    c, mgr, vid, ids, staff, _ = _world()
    db = get_db()
    nm = _next_week(vid)
    fri, sat = nm + timedelta(days=4), nm + timedelta(days=5)
    rows = _week_rows(fri, sat)
    _paste(c, mgr, vid, rows)
    rid = f"imported-{vid}-{nm.isoformat()}"
    jo_sat = next(s.id for s in db.get_roster(rid).shifts
                  if s.date == sat and s.employee_id == ids["jo"])
    lv = _record_leave(c, mgr, vid, ids["jo"], sat)
    assert "Sat 4-11pm Duty Manager" in lv["message"]
    cover = _leave_covers(vid, jo_sat)[jo_sat]

    again = _paste(c, mgr, vid, rows)
    assert (again["shifts_imported"], again["shifts_removed"]) == (0, 0)
    assert _day_shifts(rid, sat) == [("16:00", "", "Duty Manager"),
                                     ("17:00", ids["sam"], "Front Bar")]
    assert [s.id for s in db.get_roster(rid).shifts if s.date == sat and not s.employee_id] == [jo_sat]
    row = next(x for x in again["rows"] if x["date"] == sat.isoformat() and x["name"] == "Jo Taylor")
    assert row["status"] == "skipped" and "approved leave" in row["reason"]
    assert [f["flag"] for f in again["flagged"]] == ["on_leave"]
    live = [cv for cv in db.list_shift_covers(vid) if cv["status"] in ("open", "claimed")]
    assert [(cv["id"], cv["shift_id"]) for cv in live] == [(cover["id"], jo_sat)]
    assert _my_days(c, staff["jo"]) == [fri.isoformat()]

    # Pat covers it; the next paste still adds no second Saturday duty manager
    assert c.post(f"/api/me/cover/{cover['id']}/claim", headers=staff["pat"]).status_code == 200
    ok = c.post(f"/api/cover/{cover['id']}/decide", json={"venue_id": vid, "approve": True},
                headers=mgr)
    assert ok.status_code == 200, ok.text
    third = _paste(c, mgr, vid, rows)
    assert third["shifts_imported"] == 0
    assert _day_shifts(rid, sat) == [("16:00", ids["pat"], "Duty Manager"),
                                     ("17:00", ids["sam"], "Front Bar")]


def test_a_re_paste_with_someone_else_in_the_open_slot_fills_it_and_withdraws_the_cover():
    c, mgr, vid, ids, staff, _ = _world()
    db = get_db()
    nm = _next_week(vid)
    fri, sat = nm + timedelta(days=4), nm + timedelta(days=5)
    _paste(c, mgr, vid, _week_rows(fri, sat))
    rid = f"imported-{vid}-{nm.isoformat()}"
    jo_sat = next(s.id for s in db.get_roster(rid).shifts
                  if s.date == sat and s.employee_id == ids["jo"])
    _record_leave(c, mgr, vid, ids["jo"], sat)
    assert _grabs(c, staff["pat"]) == [jo_sat]

    # Deputy now has Meca on Jo's Saturday
    out = _paste(c, mgr, vid, [("Jo Taylor", fri, "16:00", "23:00", "Duty Manager", 30),
                               ("Meca Ryan", sat, "16:00", "23:00", "Duty Manager", 30),
                               ("Sam Kelly", sat, "17:00", "23:00", "Front Bar", 0)])
    assert (out["shifts_imported"], out["shifts_replaced"]) == (0, 1)
    assert _day_shifts(rid, sat) == [("16:00", ids["meca"], "Duty Manager"),
                                     ("17:00", ids["sam"], "Front Bar")]
    filled = _roster_shift(rid, jo_sat)
    assert filled.employee_id == ids["meca"] and filled.cost is not None
    row = next(x for x in out["rows"] if x["name"] == "Meca Ryan")
    assert row["status"] == "replaced" and "open shift" in row["reason"]
    assert _leave_covers(vid, jo_sat)[jo_sat]["status"] == "withdrawn"
    assert [n for n in out["cover_notes"] if n.startswith("Sat 4-11pm Duty Manager:")
            and "withdrawn" in n]
    assert _grabs(c, staff["pat"]) == []
    assert sat.isoformat() in _my_days(c, staff["meca"])


# ---------------------------------------------------------------------------
# [7] A week RosterIQ has published
# ---------------------------------------------------------------------------

def test_a_paste_into_a_week_published_in_rosteriq_changes_nothing_there_and_says_so():
    c, mgr, vid, ids, staff, _ = _world()
    db = get_db()
    nm = _next_week(vid)
    sat, sun = nm + timedelta(days=5), nm + timedelta(days=6)
    later = nm + timedelta(days=9)
    _paste(c, mgr, vid, [("Jo Taylor", sat, "16:00", "23:00", "Duty Manager", 30)])
    imp = f"imported-{vid}-{nm.isoformat()}"
    gen = _draft(vid, nm, [_shift("g-jo", ids["jo"], sat, dtime(16), dtime(23), "Duty Manager")])
    assert _publish_api(c, mgr, gen)["success"] is True
    assert roster_state(db, imp) == "superseded"

    out = _paste(c, mgr, vid, [("Jo Taylor", sat, "16:00", "23:00", "Duty Manager", 30),
                               ("Chloe Murphy", sun, "11:00", "15:00", "Floor Runner", 0),
                               ("Chloe Murphy", later, "11:00", "15:00", "Floor Runner", 0)])
    # The free week imports; the published one is left alone, plainly
    assert (out["shifts_imported"], out["weeks"]) == (1, 1)
    assert out["published_weeks"] == [nm.isoformat()]
    reasons = [s["reason"] for s in out["skipped"]]
    assert len(reasons) == 2 and all("published in RosterIQ" in r for r in reasons)
    assert all("staff see" in r for r in reasons)
    assert [s.id for s in db.get_roster(imp).shifts if s.employee_id == ids["chloe"]] == []
    assert roster_state(db, imp) == "superseded"
    assert current_roster(db, vid, nm, staff=True).id == gen


def test_a_paste_into_a_published_week_without_an_import_or_after_a_recall_changes_nothing():
    c, mgr, vid, ids, staff, _ = _world()
    db = get_db()
    nm = _next_week(vid)
    sat = nm + timedelta(days=5)
    gen = _draft(vid, nm, [_shift("g1-jo", ids["jo"], sat, dtime(16), dtime(23), "Duty Manager")])
    assert _publish_api(c, mgr, gen)["success"] is True
    out = _paste(c, mgr, vid, [("Chloe Murphy", sat, "11:00", "15:00", "Floor Runner", 0)])
    assert (out["shifts_imported"], out["published_weeks"]) == (0, [nm.isoformat()])
    assert db.get_roster(f"imported-{vid}-{nm.isoformat()}") is None

    # Published over an import, then recalled to change it: the paste never
    # replaces the manager's recalled roster
    wk = nm + timedelta(days=7)
    wsat = wk + timedelta(days=5)
    _paste(c, mgr, vid, [("Jo Taylor", wsat, "16:00", "23:00", "Duty Manager", 30)])
    gen2 = _draft(vid, wk, [_shift("g2-jo", ids["jo"], wsat, dtime(16), dtime(23), "Duty Manager")])
    assert _publish_api(c, mgr, gen2)["success"] is True
    assert c.post(f"/api/v1/rosters/{gen2}/recall", json={"reason": "fix"},
                  headers=mgr).status_code == 200
    out = _paste(c, mgr, vid, [("Chloe Murphy", wsat, "11:00", "15:00", "Floor Runner", 0)])
    assert (out["shifts_imported"], out["published_weeks"]) == (0, [wk.isoformat()])
    assert roster_state(db, gen2) == "draft"
    assert current_roster(db, vid, wk).id == gen2


# ---------------------------------------------------------------------------
# Postgres parity
# ---------------------------------------------------------------------------

def test_pg_saves_changed_leave_dates():
    pg = _PG()
    PostgresStore.save_leave_request(pg, {
        "id": "lv-1", "venue_id": "v", "employee_id": "e", "start_date": date(2026, 10, 19),
        "end_date": date(2026, 10, 21), "reason": "", "leave_type": "annual", "paid_hours": 7.0,
        "status": "cancelled", "decided_by": "u1", "decided_at": datetime(2026, 10, 10),
        "decision_note": "back early"})
    sql, params = next((s, p) for s, p in pg.log if s.startswith("INSERT INTO leave_requests"))
    on_conflict = sql[sql.index("DO UPDATE SET"):]
    for col in ("start_date=EXCLUDED.start_date", "end_date=EXCLUDED.end_date",
                "status=EXCLUDED.status", "paid_hours=EXCLUDED.paid_hours",
                "decided_by=EXCLUDED.decided_by", "decision_note=EXCLUDED.decision_note"):
        assert col in on_conflict, col
    assert params[3:5] == (date(2026, 10, 19), date(2026, 10, 21))


# ---------------------------------------------------------------------------
# Dashboard
# ---------------------------------------------------------------------------

DASHBOARD = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "static", "dashboard.html")
NODE = shutil.which("node")
needs_node = pytest.mark.skipif(NODE is None, reason="node is required to run the dashboard JS")

HARNESS = r"""
const _els = {};
function el(id) {
  if (!_els[id]) _els[id] = { id, innerHTML: '', textContent: '', value: '', disabled: false,
    style: {}, selectedOptions: [{ textContent: '' }], options: [],
    setAttribute() {}, getAttribute() { return null; } };
  return _els[id];
}
global.document = { getElementById: el };
global.esc = function (s) { return String(s == null ? '' : s).replace(/[&<>]/g, function (c) {
  return { '&': '&amp;', '<': '&lt;', '>': '&gt;' }[c]; }); };
global.toasts = [];
global.showToast = function (m, k) { toasts.push([m, k]); };
global.state = { currentVenue: 'wem' };
global.calls = [];
global.prompts = [];
global.answers = [];
global.window = {
  prompt: function (msg, def) { prompts.push(msg); return answers.length ? answers.shift() : def; },
  confirm: function (msg) { prompts.push(msg); return answers.length ? answers.shift() : true; },
};
global.loadCoverRequests = function () {};
global.fetchVenueEmployees = async function () { return { items: [] }; };
global.reply = function () { return {}; };
global.fetchAPI = async function (url, opts) {
  const body = opts && opts.body ? JSON.parse(opts.body) : null;
  calls.push({ url: url, method: (opts && opts.method) || 'GET', body: body });
  return reply(url, body);
};
function overlap(message) {
  const e = new Error(message);
  e.status = 409;
  e.data = { error: { code: 'LEAVE_OVERLAP', message: message, status: 409 } };
  return e;
}
"""


def _between(src, start, end):
    i = src.index(start)
    return src[i:src.index(end, i)]


def _run_js(scenario):
    with open(DASHBOARD, encoding="utf-8") as fh:
        src = re.findall(r"<script[^>]*>([\s\S]*?)</script>", fh.read())[1]
    parts = [_between(src, "let lvRows = [];", "// SETUP WIZARD"),
             _between(src, "async function decideLeave(", "async function preparePayroll("),
             _between(src, "// ----- Import reports", "async function runRosterImport() {")]
    prog = HARNESS + "\n".join(parts) + "\n(async () => {\n" + scenario + \
        "\n})().catch(e => { console.log(JSON.stringify({error: String(e && e.stack || e)})); });"
    out = subprocess.run([NODE, "-e", prog], capture_output=True, text=True, timeout=30)
    assert out.returncode == 0, out.stderr
    data = json.loads(out.stdout.strip().splitlines()[-1])
    assert "error" not in data, data.get("error")
    return data


LEAVE_ROWS = """
lvRows = [
  { id: 'lv-1', employee_id: 'e1', employee_name: '<b>Jo</b>', start_date: '2026-10-19',
    end_date: '2026-10-25', status: 'approved', leave_type: 'annual', leave_type_label: 'Annual leave',
    paid_hours: 38, reason: '<i>trip</i>', opened_shifts: ['Sat 4-11pm <Duty>'] },
  { id: 'lv-2', employee_id: 'e2', employee_name: 'Sam', start_date: '2026-10-20', end_date: '2026-10-20',
    status: 'cancelled', leave_type: 'personal', leave_type_label: "Personal/carer's leave",
    decision_note: 'called <off>' },
  { id: 'lv-3', employee_id: 'e3', employee_name: 'Pat', start_date: '2026-10-21', end_date: '2026-10-21',
    status: 'pending', leave_type: 'other', leave_type_label: 'Other' },
];
"""


@needs_node
def test_dashboard_approved_leave_rows_offer_cancel_and_change_dates():
    data = _run_js(LEAVE_ROWS + """
      renderLeaveRequests();
      console.log(JSON.stringify({ rows: el('lv-rows').innerHTML.split('<tr').slice(1) }));
    """)
    approved, cancelled, pending = data["rows"]
    assert "cancelLeave(0)" in approved and "changeLeaveDates(0)" in approved
    assert "Cancel leave" in approved and "Change dates" in approved
    assert "&lt;b&gt;Jo&lt;/b&gt;" in approved and "<b>Jo</b>" not in approved
    assert "&lt;i&gt;trip&lt;/i&gt;" in approved and "&lt;Duty&gt;" in approved
    assert "CANCELLED" in cancelled and "PENDING" not in cancelled
    assert "cancelLeave(" not in cancelled and "decideLeave(" not in cancelled
    assert "called &lt;off&gt;" in cancelled
    assert "decideLeave(2, true)" in pending and "cancelLeave(" not in pending


@needs_node
def test_dashboard_cancel_and_change_dates_send_the_amendment():
    data = _run_js(LEAVE_ROWS + """
      reply = function (url, body) {
        if (url.indexOf('/amend') > 0) return { status: body.cancel ? 'cancelled' : 'approved', message: 'Done <ok>' };
        return { requests: lvRows };
      };
      answers = ['Trip off'];
      await cancelLeave(0);
      const cancel = calls.filter(function (c) { return c.method === 'POST'; });
      const cancelPrompt = prompts.slice();
      calls = []; prompts = [];
      answers = ['2026-10-19', '2026-10-21', ''];
      await changeLeaveDates(0);
      const change = calls.filter(function (c) { return c.method === 'POST'; });
      calls = [];
      answers = ['19/10/2026'];
      await changeLeaveDates(0);
      const bad = calls.filter(function (c) { return c.method === 'POST'; });
      calls = []; answers = [null];
      await cancelLeave(0);
      console.log(JSON.stringify({ cancel, cancelPrompt, change, bad, after: calls, toasts }));
    """)
    assert [(c["url"], c["body"]) for c in data["cancel"]] == \
        [("/api/leave/lv-1/amend", {"venue_id": "wem", "cancel": True, "note": "Trip off"})]
    assert "<b>Jo</b>" in data["cancelPrompt"][0] and "2026-10-19" in data["cancelPrompt"][0]
    assert [(c["url"], c["body"]) for c in data["change"]] == \
        [("/api/leave/lv-1/amend", {"venue_id": "wem", "start_date": "2026-10-19",
                                    "end_date": "2026-10-21"})]
    assert data["bad"] == [] and data["after"] == []
    assert ["Done <ok>", "success"] in data["toasts"]
    assert any(k == "warning" for _, k in data["toasts"])


@needs_node
def test_dashboard_offers_record_anyway_on_overlapping_leave():
    data = _run_js("""
      el('lvr-emp').value = 'e1'; el('lvr-emp').selectedOptions = [{ textContent: 'Jo Taylor' }];
      el('lvr-type').value = 'personal';
      el('lvr-type').selectedOptions = [{ textContent: "Personal/carer's leave" }];
      el('lvr-start').value = '2026-10-17';
      el('lv-filter').value = 'pending';
      reply = function (url, body) {
        if (url.indexOf('/api/leave/impact') === 0) return { shifts: [], message: 'No shifts.' };
        if (url === '/api/leave' && !body.allow_overlap) throw overlap('Jo Taylor already has approved leave for 2026-10-17');
        if (url === '/api/leave') return { message: 'Recorded' };
        if (url.indexOf('/decide') > 0 && !body.allow_overlap) throw overlap('Jo Taylor already has approved leave for 2026-10-17');
        if (url.indexOf('/decide') > 0) return { status: 'approved', opened_shifts: [] };
        if (url.indexOf('/api/leave/impact') === 0) return {};
        return { requests: [] };
      };
      answers = [true, true];
      await recordLeave();
      const recorded = calls.filter(function (c) { return c.url === '/api/leave'; }).map(function (c) { return c.body; });
      const recordPrompts = prompts.slice();
      const filter = el('lv-filter').value;
      calls = []; prompts = [];
      el('lvr-start').value = '2026-10-17';
      answers = [true, false];
      await recordLeave();
      const declined = calls.filter(function (c) { return c.url === '/api/leave'; }).length;

      lvRows = [{ id: 'lv-9', employee_id: 'e1', employee_name: 'Jo Taylor', start_date: '2026-10-17',
                  end_date: '2026-10-18', status: 'pending', leave_type: 'other', leave_type_label: 'Other' }];
      calls = []; prompts = [];
      answers = ['', true];
      await decideLeave(0, true);
      const decided = calls.filter(function (c) { return c.url.indexOf('/decide') > 0; }).map(function (c) { return c.body; });
      console.log(JSON.stringify({ recorded, recordPrompts, filter, declined, decided, decidePrompts: prompts }));
    """)
    assert [b.get("allow_overlap") for b in data["recorded"]] == [None, True]
    assert "already has approved leave" in data["recordPrompts"][-1]
    assert "Record anyway" in data["recordPrompts"][-1]
    assert data["filter"] == "approved"
    assert data["declined"] == 1
    assert [b.get("allow_overlap") for b in data["decided"]] == [None, True]
    assert "Approve anyway" in data["decidePrompts"][-1]


@needs_node
def test_dashboard_import_report_says_when_a_week_is_published_in_rosteriq():
    data = _run_js("""
      const html = renderRosterImportReport({
        published_weeks: ['2026-10-19'],
        rows: [{ line: 2, name: 'Jo', date: '2026-10-24', status: 'skipped', reason: 'published <x>' }],
        skipped_count: 1,
        flagged: [{ line: 3, name: '<Jo>', date: '2026-10-25', flag: 'on_leave',
                    detail: 'Jo is on approved leave <x>' }],
        flags_summary: { on_leave: 1 },
      });
      console.log(JSON.stringify({ html }));
    """)
    html = data["html"]
    assert "19/10/2026" in html and "published in RosterIQ" in html and "staff see" in html
    assert "on approved leave: left open" in html
    assert "&lt;Jo&gt;" in html and "<Jo>" not in html and "published &lt;x&gt;" in html
