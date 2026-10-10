"""
Wembley pilot (g8): hand edits and Copy last week, after the review.

[11] A recorded visa fortnight cap covers every job: a hand edit counts the
     person's shifts at the owner's other venue (this week and last), and
     Copy last week flags the cap and other-venue clashes in its report.
[12]/[47] Copy last week carries an open shift over as an open shift, and a
     shift whose person is now on leave, unavailable, inactive or gone is
     left open for the manager to fill, never "Unknown staff member".
[13] An open shift can be retimed, re-dated or moved to another area and
     stay open; an area the venue doesn't have is warned about, as it is
     for an assigned shift.
[7]/[41] Filling, reassigning, moving, retiming, re-areaing or deleting a
     shift by hand withdraws the cover request on it, so a later claim can't
     take the shift off whoever the manager put on it; the manager is told.
     The withdrawal is conditional, so a claim made while the manager saves
     is kept and named, and a cover settled meanwhile is left alone.
"""

import json
import uuid
from contextlib import contextmanager
from datetime import date, datetime, time, timedelta
from decimal import Decimal

from fastapi.testclient import TestClient

from rosteriq.api import app
from rosteriq.database import PostgresStore, get_db
from rosteriq.models import Shift, ShiftStatus
from rosteriq.tests.test_wembley_a6 import _dash, _fns, _run, needs_node
from rosteriq.tests.test_wembley_b3 import (
    _emp, _manager_with_venue, _next_monday, _roster, _shift as _b3_shift,
)
from rosteriq.tests.test_wembley_b7 import (
    _detail, _next_week, _publish, _roster_shift, _shift as _b7_shift, _world,
)


def _codes(resp):
    return [w["code"] for w in resp.json()["warnings"]]


def _messages(resp, code):
    return [w["message"] for w in resp.json()["warnings"] if w["code"] == code]


# ---------------------------------------------------------------------------
# [11] other-venue hours count toward the recorded visa cap
# ---------------------------------------------------------------------------

def _sister_venue(c, mgr):
    apo = f"apo-{uuid.uuid4().hex[:6]}"
    r = c.post("/venues", json={"id": apo, "name": "Apothecary", "state": "wa",
                                "max_labour_pct": 30, "tanda_org_id": "",
                                "timezone": "Australia/Perth",
                                "created_at": "2026-10-01T00:00:00"}, headers=mgr)
    assert r.status_code == 200, r.text
    return apo


def _meca_at_both(vid, apo, cap=48):
    email = f"meca{uuid.uuid4().hex[:6]}@x.com"
    here = _emp(vid, "meca", "Meca Rossi", email=email, visa_status="student",
                visa_work_limit_fortnight=cap)
    there = _emp(apo, "meca", "Meca Rossi", email=email, skills=["bar"], visa_status="student",
                 visa_work_limit_fortnight=cap)
    return here, there


def _apothecary_evenings(apo, emp_id, monday, days=(0, 2, 4)):
    """Seven-hour closes at Apothecary on the given days of the week."""
    return _roster(apo, monday, [_b3_shift(emp_id, monday + timedelta(days=d), time(17), time(0),
                                           role="bar", brk=0) for d in days], state="published")


def test_hand_add_counts_the_persons_other_venue_hours_toward_their_visa_cap():
    c = TestClient(app)
    mgr, vid, _ = _manager_with_venue(c)
    apo = _sister_venue(c, mgr)
    monday = _next_monday()
    meca, meca_apo = _meca_at_both(vid, apo)
    other = _emp(vid, "oli", "Oli Other")
    # 21h at Apothecary last week and 21h this week: 42h of a 48h cap
    _apothecary_evenings(apo, meca_apo.id, monday - timedelta(days=7))
    _apothecary_evenings(apo, meca_apo.id, monday)
    r = _roster(vid, monday, [_b3_shift(other.id, monday, time(10), time(17), sid=f"{vid}-o1")])

    sat = monday + timedelta(days=5)
    add = c.post(f"/rosters/{r.id}/shifts", json={
        "employee_id": meca.id, "date": sat.isoformat(), "start_time": "10:00", "end_time": "17:00",
        "break_minutes": 30, "role": "front bar"}, headers=mgr)
    assert add.status_code == 200, add.text
    assert "visa_cap" in _codes(add)
    assert "49h across the fortnight" in _messages(add, "visa_cap")[0]

    # reassigning a Wembley shift onto her is checked the same way
    move = c.put(f"/rosters/{r.id}/shifts/{vid}-o1", json={"employee_id": meca.id}, headers=mgr)
    assert move.status_code == 200, move.text
    assert "visa_cap" in _codes(move)


def test_hand_add_under_the_cap_across_both_venues_has_no_visa_warning():
    c = TestClient(app)
    mgr, vid, _ = _manager_with_venue(c)
    apo = _sister_venue(c, mgr)
    monday = _next_monday()
    meca, meca_apo = _meca_at_both(vid, apo)
    _apothecary_evenings(apo, meca_apo.id, monday, days=(0,))
    r = _roster(vid, monday, [])
    add = c.post(f"/rosters/{r.id}/shifts", json={
        "employee_id": meca.id, "date": (monday + timedelta(days=5)).isoformat(),
        "start_time": "10:00", "end_time": "17:00", "break_minutes": 30, "role": "front bar"},
        headers=mgr)
    assert add.status_code == 200, add.text
    assert "visa_cap" not in _codes(add)


def test_copy_last_week_reports_the_visa_cap_and_clashes_at_the_other_venue():
    c = TestClient(app)
    mgr, vid, _ = _manager_with_venue(c)
    apo = _sister_venue(c, mgr)
    source_monday = _next_monday()
    target = source_monday + timedelta(days=7)
    meca, meca_apo = _meca_at_both(vid, apo)
    source = _roster(vid, source_monday, [
        _b3_shift(meca.id, source_monday + timedelta(days=2), time(18), time(22), brk=0),
        _b3_shift(meca.id, source_monday + timedelta(days=5), time(10), time(17)),
    ], state="published")
    _apothecary_evenings(apo, meca_apo.id, source_monday)
    _apothecary_evenings(apo, meca_apo.id, target)

    resp = c.post(f"/rosters/{source.id}/copy-to?week_start={target.isoformat()}", headers=mgr)
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["copied"] == 2
    # 21h + 11h at Wembley last week, 21h + 11h this week across both venues
    assert [f["name"] for f in body["visa_flags"]] == ["Meca Rossi"]
    assert body["visa_flags"][0]["rostered_fortnight_hours"] == 64
    assert any("Meca Rossi is on at Apothecary 17:00-00:00" in m for m in body["cross_venue_warnings"])


def test_copy_with_no_other_venue_keeps_the_report_quiet():
    c = TestClient(app)
    mgr, vid, _ = _manager_with_venue(c)
    monday = _next_monday()
    amy = _emp(vid, "amy", "Amy Barker")
    source = _roster(vid, monday, [_b3_shift(amy.id, monday, time(17), time(23))])
    body = c.post(f"/rosters/{source.id}/copy-to", headers=mgr).json()
    assert body["visa_flags"] == [] and body["cross_venue_warnings"] == []
    assert body["left_open"] == [] and body["skipped"] == []


# ---------------------------------------------------------------------------
# [12]/[47] Copy last week leaves slots open instead of dropping them
# ---------------------------------------------------------------------------

def test_copy_carries_open_shifts_over_and_leaves_unavailable_peoples_shifts_open():
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
                           "start_date": target + timedelta(days=1), "end_date": target + timedelta(days=1),
                           "reason": "Holiday", "status": "approved", "created_at": datetime.utcnow()})
    wed = this_monday + timedelta(days=2)
    source = _roster(vid, this_monday, [
        _b3_shift("", wed, time(10), time(17), role="kitchen"),       # left open by leave last week
        _b3_shift(amy.id, wed, time(17), time(23)),
        _b3_shift(ben.id, this_monday + timedelta(days=1), time(11), time(17), role="kitchen"),
        _b3_shift(cat.id, this_monday + timedelta(days=3), time(17), time(23)),
        _b3_shift(dan.id, this_monday + timedelta(days=4), time(17), time(23)),
    ], state="published")
    dan_rec = db.get_employee(dan.id)
    dan_rec.active = False
    db.save_employee(dan_rec)

    resp = c.post(f"/rosters/{source.id}/copy-to?week_start={target.isoformat()}", headers=mgr)
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["copied"] == 1
    assert body["skipped"] == []
    assert sorted((r["name"], r["reason"]) for r in body["left_open"]) == [
        ("Ben Leave", "on approved leave"), ("Cat Busy", "unavailable"),
        ("Dan Gone", "inactive"), ("Open shift", "open last week")]
    assert "Unknown staff member" not in resp.text

    new = db.get_roster(body["roster_id"])
    slots = sorted((s.date, s.start_time, s.role, s.employee_id) for s in new.shifts)
    assert slots == [
        (target + timedelta(days=1), time(11), "kitchen", ""),
        (target + timedelta(days=2), time(10), "kitchen", ""),
        (target + timedelta(days=2), time(17), "front bar", amy.id),
        (target + timedelta(days=3), time(17), "front bar", ""),
        (target + timedelta(days=4), time(17), "front bar", ""),
    ]
    assert all(s.cost is None for s in new.shifts if not s.employee_id)
    priced = next(s for s in new.shifts if s.employee_id == amy.id)
    assert priced.cost and priced.cost > 0 and new.total_cost and new.total_cost > 0


def test_copy_skips_only_shifts_that_would_land_outside_the_week():
    c = TestClient(app)
    mgr, vid, _ = _manager_with_venue(c)
    monday = _next_monday()
    amy = _emp(vid, "amy", "Amy Barker")
    # an odd row dated past the source week's Sunday
    source = _roster(vid, monday, [_b3_shift(amy.id, monday, time(17), time(23)),
                                   _b3_shift("", monday + timedelta(days=7), time(10), time(16))])
    body = c.post(f"/rosters/{source.id}/copy-to", headers=mgr).json()
    assert body["copied"] == 1 and body["left_open"] == []
    assert [(s["name"], s["reason"]) for s in body["skipped"]] == [("Open shift", "outside the week")]


def test_copy_after_leave_keeps_the_leave_opened_slot():
    c, mgr, vid, ids, staff, _ = _world()
    nm = _next_week(vid)
    fri = nm + timedelta(days=4)
    rid = _publish(vid, nm, [_b7_shift("sam-fri", ids["sam"], fri, time(16), time(23), "Front Bar"),
                             _b7_shift("pat-sat", ids["pat"], fri + timedelta(days=1), time(16),
                                       time(23), "Front Bar")])
    r = c.post("/api/leave", json={"venue_id": vid, "employee_id": ids["sam"],
                                   "start_date": fri.isoformat(), "end_date": fri.isoformat(),
                                   "leave_type": "annual", "reason": "x"}, headers=mgr)
    assert r.status_code == 200, r.text
    assert _roster_shift(rid, "sam-fri").employee_id == ""

    copy = c.post(f"/rosters/{rid}/copy-to", headers=mgr)
    assert copy.status_code == 200, copy.text
    body = copy.json()
    assert body["skipped"] == []
    assert [(x["name"], x["role"], x["reason"]) for x in body["left_open"]] == [
        ("Open shift", "Front Bar", "open last week")]
    new = get_db().get_roster(body["roster_id"])
    assert sorted((s.date.weekday(), s.employee_id) for s in new.shifts) == [(4, ""), (5, ids["pat"])]


# ---------------------------------------------------------------------------
# [13] an open shift is retimed, re-dated or re-areaed and stays open
# [7]/[41] hand changes withdraw the shift's cover request
# ---------------------------------------------------------------------------

def _leave_opened(c, mgr, vid, ids, staff):
    """Jo's Thu and Fri Front Bar shifts, left open by approved leave, each
    posted for cover."""
    nm = _next_week(vid)
    thu, fri = nm + timedelta(days=3), nm + timedelta(days=4)
    rid = _publish(vid, nm, [
        _b7_shift("jo-thu", ids["jo"], thu, time(10), time(17), "Front Bar", cost=Decimal("200")),
        _b7_shift("jo-fri", ids["jo"], fri, time(10), time(17), "Front Bar", cost=Decimal("200")),
        _b7_shift("sam-sat", ids["sam"], fri + timedelta(days=1), time(10), time(17), "Front Bar"),
    ])
    r = c.post("/api/me/leave", json={"start_date": thu.isoformat(), "end_date": fri.isoformat(),
                                      "reason": "x", "leave_type": "annual"}, headers=staff["jo"])
    assert r.status_code == 200, r.text
    dec = c.post(f"/api/leave/{r.json()['request_id']}/decide",
                 json={"venue_id": vid, "approve": True}, headers=mgr)
    assert dec.status_code == 200, dec.text
    assert _roster_shift(rid, "jo-thu").employee_id == "" == _roster_shift(rid, "jo-fri").employee_id
    return rid, nm


def _covers(c, mgr, vid):
    return c.get(f"/api/cover?venue_id={vid}", headers=mgr).json()["covers"]


def _cover_for(c, mgr, vid, shift_id, status=None):
    return [x for x in _covers(c, mgr, vid)
            if x["shift_id"] == shift_id and (status is None or x["status"] == status)]


def _grabs(c, hdr):
    return {x["shift_id"]: x for x in c.get("/api/me/cover", headers=hdr).json()["up_for_grabs"]}


def test_an_open_shift_can_be_retimed_moved_and_re_dated_and_stays_open():
    c, mgr, vid, ids, staff, _ = _world()
    rid, nm = _leave_opened(c, mgr, vid, ids, staff)

    retime = c.put(f"/rosters/{rid}/shifts/jo-fri", json={"start_time": "11:00"}, headers=mgr)
    assert retime.status_code == 200, retime.text
    assert retime.json()["action"] == "update"
    s = _roster_shift(rid, "jo-fri")
    assert (s.employee_id, s.start_time, s.cost) == ("", time(11), None)

    area = c.put(f"/rosters/{rid}/shifts/jo-fri", json={"role": "floor runner"}, headers=mgr)
    assert area.status_code == 200, area.text
    assert _roster_shift(rid, "jo-fri").role == "floor runner"
    wed = nm + timedelta(days=2)
    move = c.put(f"/rosters/{rid}/shifts/jo-fri", json={"date": wed.isoformat()}, headers=mgr)
    assert move.status_code == 200, move.text
    s = _roster_shift(rid, "jo-fri")
    assert (s.employee_id, s.date) == ("", wed)

    # still a whole-week check, and the times still have to make sense
    out = c.put(f"/rosters/{rid}/shifts/jo-fri", json={"date": (nm + timedelta(days=9)).isoformat()},
                headers=mgr)
    assert out.status_code == 400
    same = c.put(f"/rosters/{rid}/shifts/jo-fri", json={"start_time": "17:00", "end_time": "17:00"},
                 headers=mgr)
    assert same.status_code == 400
    assert c.put(f"/rosters/{rid}/shifts/jo-fri", json={}, headers=mgr).json()["action"] == "unchanged"


def test_retiming_an_open_shift_reposts_its_cover_with_the_new_details():
    c, mgr, vid, ids, staff, _ = _world()
    rid, nm = _leave_opened(c, mgr, vid, ids, staff)
    old = _cover_for(c, mgr, vid, "jo-fri", "open")[0]

    retime = c.put(f"/rosters/{rid}/shifts/jo-fri", json={"start_time": "11:00"}, headers=mgr)
    assert retime.status_code == 200, retime.text
    note = _messages(retime, "cover_withdrawn")
    assert note and note[0].startswith("The cover request for this shift was withdrawn")
    assert "posted for cover again" in note[0]

    rows = _cover_for(c, mgr, vid, "jo-fri")
    assert {(x["id"] == old["id"], x["status"]) for x in rows} == {(True, "withdrawn"), (False, "open")}
    fresh = next(x for x in rows if x["status"] == "open")
    assert (fresh["shift_start"], fresh["shift_end"], fresh["origin"]) == ("11:00", "17:00", "leave")
    assert _grabs(c, staff["pat"])["jo-fri"]["shift_start"] == "11:00"


def test_filling_an_open_shift_by_hand_withdraws_its_cover_and_a_claim_cannot_take_it():
    c, mgr, vid, ids, staff, _ = _world()
    rid, _ = _leave_opened(c, mgr, vid, ids, staff)
    cover = _cover_for(c, mgr, vid, "jo-thu", "open")[0]
    claim = c.post(f"/api/me/cover/{cover['id']}/claim", headers=staff["pat"])
    assert claim.status_code == 200, claim.text

    fill = c.put(f"/rosters/{rid}/shifts/jo-thu", json={"employee_id": ids["sam"]}, headers=mgr)
    assert fill.status_code == 200, fill.text
    assert _messages(fill, "cover_withdrawn") == [
        "The cover request for this shift was withdrawn. Pat Nguyen had claimed it — let them know."]
    assert [x["status"] for x in _cover_for(c, mgr, vid, "jo-thu")] == ["withdrawn"]

    approve = c.post(f"/api/cover/{cover['id']}/decide", json={"venue_id": vid, "approve": True},
                     headers=mgr)
    assert approve.status_code == 409, approve.text
    assert _roster_shift(rid, "jo-thu").employee_id == ids["sam"]
    assert "jo-thu" not in _grabs(c, staff["pat"])
    board = c.get("/api/me/cover", headers=staff["pat"]).json()
    assert board["claimed_by_me"] == []


def test_filling_an_open_cover_nobody_claimed_closes_it_off_the_board():
    c, mgr, vid, ids, staff, _ = _world()
    rid, _ = _leave_opened(c, mgr, vid, ids, staff)
    assert "jo-thu" in _grabs(c, staff["pat"])
    fill = c.put(f"/rosters/{rid}/shifts/jo-thu", json={"employee_id": ids["sam"]}, headers=mgr)
    assert _messages(fill, "cover_withdrawn") == ["The cover request for this shift was withdrawn."]
    assert "jo-thu" not in _grabs(c, staff["pat"])
    cover = _cover_for(c, mgr, vid, "jo-thu")[0]
    claim = c.post(f"/api/me/cover/{cover['id']}/claim", headers=staff["pat"])
    assert claim.status_code == 409


def test_deleting_a_shift_withdraws_its_cover_and_the_briefing_stops_counting_it():
    c, mgr, vid, ids, staff, _ = _world()
    rid, _ = _leave_opened(c, mgr, vid, ids, staff)
    for sid in ("jo-thu", "jo-fri"):
        gone = c.delete(f"/rosters/{rid}/shifts/{sid}", headers=mgr)
        assert gone.status_code == 200, gone.text
        assert _messages(gone, "cover_withdrawn") == ["The cover request for this shift was withdrawn."]
    assert {x["status"] for x in _covers(c, mgr, vid)} == {"withdrawn"}
    brief = c.get(f"/api/briefing?venue_id={vid}", headers=mgr)
    assert brief.status_code == 200, brief.text
    body = brief.json()
    attention = body.get("attention") or (body.get("data") or {}).get("attention") or []
    assert not any("up for cover" in a for a in attention), attention


def test_reassigning_a_shift_a_staff_member_offered_withdraws_their_request():
    c, mgr, vid, ids, staff, _ = _world()
    nm = _next_week(vid)
    rid = _publish(vid, nm, [_b7_shift("sam-tue", ids["sam"], nm + timedelta(days=1), time(10),
                                       time(17), "Front Bar")])
    offer = c.post("/api/me/shifts/sam-tue/cover", json={"reason": "Exam"}, headers=staff["sam"])
    assert offer.status_code == 200, offer.text

    # a break change doesn't touch who, when or where: the request stands
    brk = c.put(f"/rosters/{rid}/shifts/sam-tue", json={"break_minutes": 30}, headers=mgr)
    assert brk.status_code == 200 and "cover_withdrawn" not in _codes(brk)
    assert [x["status"] for x in _cover_for(c, mgr, vid, "sam-tue")] == ["open"]

    move = c.put(f"/rosters/{rid}/shifts/sam-tue", json={"employee_id": ids["pat"]}, headers=mgr)
    assert move.status_code == 200, move.text
    assert "cover_withdrawn" in _codes(move)
    mine = c.get("/api/me/cover", headers=staff["sam"]).json()["mine"]
    assert [(x["shift_id"], x["status"]) for x in mine] == [("sam-tue", "withdrawn")]
    assert mine[0]["decision_note"] == "The shift was changed on the roster."
    assert "sam-tue" not in _grabs(c, staff["meca"])


def test_retiming_an_offered_shift_withdraws_the_offer_and_a_new_one_can_be_made():
    c, mgr, vid, ids, staff, _ = _world()
    nm = _next_week(vid)
    rid = _publish(vid, nm, [_b7_shift("sam-tue", ids["sam"], nm + timedelta(days=1), time(10),
                                       time(17), "Front Bar")])
    assert c.post("/api/me/shifts/sam-tue/cover", json={"reason": "x"}, headers=staff["sam"]).status_code == 200
    retime = c.put(f"/rosters/{rid}/shifts/sam-tue", json={"end_time": "15:00"}, headers=mgr)
    assert retime.status_code == 200 and "cover_withdrawn" in _codes(retime)
    again = c.post("/api/me/shifts/sam-tue/cover", json={"reason": "x"}, headers=staff["sam"])
    assert again.status_code == 200, _detail(again)
    live = _cover_for(c, mgr, vid, "sam-tue", "open")
    assert len(live) == 1 and live[0]["shift_end"] == "15:00"


def test_moving_an_open_shift_to_an_area_the_venue_lacks_warns_like_an_assigned_one():
    c, mgr, vid, ids, staff, _ = _world()
    rid, _ = _leave_opened(c, mgr, vid, ids, staff)

    typo = c.put(f"/rosters/{rid}/shifts/jo-fri", json={"role": "Frnt Bar"}, headers=mgr)
    assert typo.status_code == 200, typo.text
    assert _messages(typo, "area") == ["'frnt bar' isn't one of this venue's areas."]
    assigned = c.put(f"/rosters/{rid}/shifts/sam-sat", json={"role": "Frnt Bar"}, headers=mgr)
    assert _messages(assigned, "area") == _messages(typo, "area")

    known = c.put(f"/rosters/{rid}/shifts/jo-fri", json={"role": "Front Bar"}, headers=mgr)
    assert known.status_code == 200 and "area" not in _codes(known)
    assert "jo-fri" in _grabs(c, staff["pat"])


def _snapshot_then(db, monkeypatch, vid, between):
    """list_shift_covers returns copies, as a Postgres SELECT does, and runs
    ``between`` once right after the read, as another worker would."""
    real = db.list_shift_covers
    fired = []

    def racing(venue_id):
        rows = [dict(r) for r in (real(venue_id) or [])]
        if venue_id == vid and not fired:
            fired.append(between())
        return rows

    monkeypatch.setattr(db, "list_shift_covers", racing)
    return fired


def test_a_claim_landing_while_the_manager_fills_the_shift_is_named_not_erased(monkeypatch):
    c, mgr, vid, ids, staff, _ = _world()
    rid, _ = _leave_opened(c, mgr, vid, ids, staff)
    db = get_db()
    cover = _cover_for(c, mgr, vid, "jo-thu", "open")[0]
    fired = _snapshot_then(db, monkeypatch, vid, lambda: db.claim_shift_cover(cover["id"], ids["pat"]))

    fill = c.put(f"/rosters/{rid}/shifts/jo-thu", json={"employee_id": ids["sam"]}, headers=mgr)
    assert fill.status_code == 200, fill.text
    assert fired == [True]
    assert _messages(fill, "cover_withdrawn") == [
        "The cover request for this shift was withdrawn. Pat Nguyen had claimed it — let them know."]
    row = db.get_shift_cover(cover["id"])
    assert (row["status"], row["claimed_by"]) == ("withdrawn", ids["pat"])


def test_a_request_cancelled_while_the_manager_edits_stays_cancelled(monkeypatch):
    c, mgr, vid, ids, staff, _ = _world()
    nm = _next_week(vid)
    rid = _publish(vid, nm, [_b7_shift("sam-tue", ids["sam"], nm + timedelta(days=1), time(10),
                                       time(17), "Front Bar")])
    offer = c.post("/api/me/shifts/sam-tue/cover", json={"reason": "x"}, headers=staff["sam"])
    assert offer.status_code == 200, offer.text
    db = get_db()
    cid = offer.json()["cover_id"]
    # what Sam's cancel saves
    fired = _snapshot_then(db, monkeypatch, vid,
                           lambda: db.save_shift_cover(dict(db.get_shift_cover(cid), status="cancelled")))

    move = c.put(f"/rosters/{rid}/shifts/sam-tue", json={"employee_id": ids["pat"]}, headers=mgr)
    assert move.status_code == 200, move.text
    assert fired == [None]
    assert "cover_withdrawn" not in _codes(move)
    assert db.get_shift_cover(cid)["status"] == "cancelled"


# ---------------------------------------------------------------------------
# Postgres: a withdrawal is one conditional UPDATE per cover
# ---------------------------------------------------------------------------

class _Cur:
    def __init__(self, pg):
        self.pg, self.result = pg, []

    def execute(self, sql, params=None):
        flat = " ".join(sql.split())
        self.pg.log.append((flat, params))
        if flat.startswith("SELECT"):
            self.result = list(self.pg.rows)
        elif flat.startswith("UPDATE shift_covers"):
            self.result = self.pg.returning.pop(0) if self.pg.returning else []
        else:
            self.result = []

    def fetchone(self):
        return self.result[0] if self.result else None

    def fetchall(self):
        return list(self.result)

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


class _PG:
    _TABLE_DDL = PostgresStore._TABLE_DDL
    _ensure_table = PostgresStore._ensure_table
    list_shift_covers = PostgresStore.list_shift_covers
    save_shift_cover = PostgresStore.save_shift_cover
    withdraw_shift_cover = PostgresStore.withdraw_shift_cover

    def __init__(self, rows, returning=None, staff=()):
        """``rows`` is what the SELECT reads; ``returning`` what each UPDATE
        hands back, by default the read row now withdrawn."""
        self.log, self.rows = [], rows
        self.returning = list(returning) if returning is not None else \
            [[dict(r, status="withdrawn")] for r in rows]
        self.staff = {e.id: e for e in staff}
        self._ensured_tables = {"shift_covers"}

    @contextmanager
    def _cursor(self):
        yield _Cur(self)

    def get_employee(self, emp_id):
        return self.staff.get(emp_id)


def _pg_cover_row(**kw):
    row = {"id": "cv-1", "venue_id": "wem", "shift_id": "s1", "shift_date": date(2026, 10, 16),
           "shift_start": "10:00", "shift_end": "17:00", "role": "Front Bar", "requested_by": "wem-jo",
           "reason": "", "origin": "leave", "claimed_by": None, "status": "open", "decided_by": None,
           "decided_at": None, "decision_note": None, "created_at": datetime(2026, 10, 9),
           "updated_at": datetime(2026, 10, 9)}
    row.update(kw)
    return row


def _upserts(pg):
    return [p for s, p in pg.log if s.startswith("INSERT INTO shift_covers")]


def _withdrawals(pg):
    return [(s, p) for s, p in pg.log if s.startswith("UPDATE shift_covers")]


def test_pg_withdrawal_is_a_conditional_update_and_a_repost_is_a_new_row():
    from rosteriq.services import roster_edit as edit
    before = Shift(id="s1", employee_id="", date=date(2026, 10, 16), start_time=time(10),
                   end_time=time(17), status=ShiftStatus.scheduled, role="Front Bar")

    pg = _PG([_pg_cover_row()])
    filled = before.model_copy(update={"employee_id": "wem-sam"})
    notes = edit.settle_covers(pg, "wem", before, filled, "mgr-1")
    assert [n["code"] for n in notes] == ["cover_withdrawn"]
    ((sql, params),) = _withdrawals(pg)
    assert "SET status = 'withdrawn'" in sql and "decided_at = now()" in sql
    assert "WHERE id = %s AND status IN ('open', 'claimed') RETURNING *" in sql
    assert params == ("mgr-1", "The shift was filled on the roster.", "cv-1")
    assert _upserts(pg) == []
    assert all("BEGIN" not in s and "COMMIT" not in s for s, _ in pg.log)

    pg = _PG([_pg_cover_row()])
    retimed = before.model_copy(update={"start_time": time(11)})
    edit.settle_covers(pg, "wem", before, retimed, "mgr-1")
    assert [p[2] for _, p in _withdrawals(pg)] == ["cv-1"]
    (fresh,) = _upserts(pg)
    assert fresh[0] != "cv-1" and fresh[2:6] == ("s1", date(2026, 10, 16), "11:00", "17:00")
    assert fresh[7] == "wem-jo" and fresh[9] == "leave" and fresh[11] == "open"

    pg = _PG([_pg_cover_row(status="approved")])
    assert edit.settle_covers(pg, "wem", before, filled, "mgr-1") == []
    assert _withdrawals(pg) == [] and _upserts(pg) == []


def test_pg_withdrawal_names_a_claim_made_after_the_read_and_skips_a_settled_cover():
    from types import SimpleNamespace
    from rosteriq.services import roster_edit as edit
    before = Shift(id="s1", employee_id="", date=date(2026, 10, 16), start_time=time(10),
                   end_time=time(17), status=ShiftStatus.scheduled, role="Front Bar")
    filled = before.model_copy(update={"employee_id": "wem-sam"})
    pat = SimpleNamespace(id="wem-pat", name="Pat Nguyen", venue_id="wem")

    # read as open; Pat's claim committed before the UPDATE ran
    pg = _PG([_pg_cover_row()], returning=[[_pg_cover_row(status="withdrawn", claimed_by="wem-pat")]],
             staff=[pat])
    notes = edit.settle_covers(pg, "wem", before, filled, "mgr-1")
    assert [n["message"] for n in notes] == [
        "The cover request for this shift was withdrawn. Pat Nguyen had claimed it — let them know."]

    # read as open; approved or cancelled before the UPDATE ran: nothing to withdraw or repost
    retimed = before.model_copy(update={"start_time": time(11)})
    pg = _PG([_pg_cover_row()], returning=[[]])
    assert edit.settle_covers(pg, "wem", before, retimed, "mgr-1") == []
    assert len(_withdrawals(pg)) == 1 and _upserts(pg) == []


# ---------------------------------------------------------------------------
# the Roster page
# ---------------------------------------------------------------------------

@needs_node
def test_copy_report_lists_open_slots_and_other_venue_clashes_escaped():
    js = _fns(_dash(), "rosterDayLabel", "renderCopyReport")
    result = {
        "copied": 3,
        "skipped": [],
        "left_open": [
            {"name": "Ben <b>Leave</b>", "date": "2026-10-20", "start": "11:00", "end": "17:00",
             "role": "kitchen", "reason": "on approved leave"},
            {"name": "Open shift", "date": "2026-10-21", "start": "10:00", "end": "17:00",
             "role": "front bar", "reason": "open last week"},
        ],
        "visa_flags": [{"message": "Meca Rossi is rostered 64h across the fortnight"}],
        "cross_venue_warnings": ["Meca <i>Rossi</i> is on at Apothecary 17:00-00:00 on Wed 21 Oct"],
    }
    data = _run(js + r"""
async function main() {
  renderCopyReport(""" + json.dumps(result) + r""");
  console.log(JSON.stringify({ html: el('roster-copy-report').innerHTML }));
}
""")
    html = data["html"]
    assert "2 shifts left open for you to fill:" in html
    assert "Ben &lt;b&gt;Leave&lt;/b&gt;" in html and "<b>" not in html
    assert "Open shift" in html and "open last week" in html
    assert "Meca &lt;i&gt;Rossi&lt;/i&gt; is on at Apothecary" in html and "<i>" not in html
    assert "64h across the fortnight" in html
    assert "Nobody was left off" not in html and "Unknown staff member" not in html


@needs_node
def test_shift_editor_saves_an_open_shift_without_choosing_a_person():
    js = _fns(_dash(), "localIso", "rosterWeekDays", "rosterDayLabel", "rosterAreaOptions", "escAttr",
              "openShiftEditor", "shiftEditProblem", "saveShiftEdit")
    roster = {"id": "r1", "venue_id": "wem", "week_start": "2026-10-12", "week_end": "2026-10-18",
              "shifts": [{"id": "open-1", "employee_id": "", "date": "2026-10-16", "start_time": "10:00:00",
                          "end_time": "17:00:00", "role": "front bar", "break_minutes": 0},
                         {"id": "s2", "employee_id": "e1", "date": "2026-10-16", "start_time": "17:00:00",
                          "end_time": "23:00:00", "role": "front bar", "break_minutes": 0}]}
    data = _run(r"""
global.shiftEdit = { id: null, rosterId: null };
global.state = { currentVenue: 'wem', currentRoster: """ + json.dumps(roster) + r""",
                 venues: [{ id: 'wem', areas: [{ name: 'front bar' }] }] };
global.rosterPeople = function () { return [{ id: 'e1', name: 'Amy' }]; };
global.openModal = function () {};
global.afterShiftEdit = function (r, label) { log.push(label); };
global.fetchAPI = async function (url, opts) { calls.push({ url: url, method: opts.method, body: JSON.parse(opts.body) }); return {}; };
""" + js + r"""
async function main() {
  openShiftEditor('open-1');
  const options = el('es-emp').innerHTML;
  el('es-start').value = '11:00';
  await saveShiftEdit();
  openShiftEditor('s2');
  el('es-emp').value = '';
  await saveShiftEdit();
  console.log(JSON.stringify({ calls: calls, options: options, problem: el('es-warnings').innerHTML }));
}
""")
    assert data["options"].count('value=""') == 1
    assert len(data["calls"]) == 1
    call = data["calls"][0]
    assert call["url"] == "/rosters/r1/shifts/open-1" and call["method"] == "PUT"
    assert "employee_id" not in call["body"] and call["body"]["start_time"] == "11:00"
    assert "Choose who works this shift." in data["problem"]
