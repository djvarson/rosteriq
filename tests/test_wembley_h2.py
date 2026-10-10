"""
Wembley pilot (h2): covers and claims across publishes, open-shift costing,
and what the manager reads after a cover approval or Copy last week.

[5]  Someone on approved leave that day isn't offered a cover, can't claim
     one, and can't be approved onto one: the approval checks carry an
     'on approved leave' block.
[6]  Publishing a newer roster of the week moves a live cover whose shift
     isn't on it, with its claim, to the identical open slot on it, a claim
     ahead of an unclaimed cover; with no such slot the cover is withdrawn
     and the publish, and every dashboard path that publishes, names who had
     claimed it. The withdrawal is conditional, so a claim landing meanwhile
     is kept.
[9]  An open slot has no labour cost until someone is on it: leave leaves it
     with no cost, and Reports and the snapshot count its hours, not a cost.
[20] Approving a claim on an open or leave shift says the shift is filled,
     not "Swap approved".
[21] Copy last week's report lists each other-venue clash once, under the
     person and the shift on this roster it collides with.
"""

import json
from datetime import date, datetime, time, timedelta
from decimal import Decimal

from fastapi.testclient import TestClient

from rosteriq.api import app
from rosteriq.database import get_db
from rosteriq.models import Employee, Roster, Shift, ShiftStatus
from rosteriq.tests.test_wembley_a6 import _dash, _fns, _run, needs_node
from rosteriq.tests.test_wembley_b3 import (
    _manager_with_venue, _next_monday, _roster, _shift as _b3_shift,
)
from rosteriq.tests.test_wembley_b7 import (
    _detail, _next_week, _publish, _roster_shift, _shift, _world,
)
from rosteriq.tests.test_wembley_g3 import _ask_leave, _draft
from rosteriq.tests.test_wembley_g8 import (
    _PG, _meca_at_both, _pg_cover_row, _sister_venue, _upserts, _withdrawals,
)


def _board(c, hdr):
    return c.get("/api/me/cover", headers=hdr).json()


# ---------------------------------------------------------------------------
# [5] approved leave and covers
# ---------------------------------------------------------------------------

def test_someone_on_approved_leave_that_day_is_not_offered_the_cover_and_cannot_claim_it():
    c, mgr, vid, ids, staff, _ = _world()
    db = get_db()
    monday = _next_week(vid)
    sat = monday + timedelta(days=5)
    _publish(vid, monday, [_shift("jo-sat", ids["jo"], sat, time(16), time(23), "duty manager", 30),
                           _shift("pat-satl", ids["pat"], sat, time(10), time(14), "front bar")])
    _ask_leave(c, mgr, staff["pat"], vid, sat)
    off = c.post("/api/me/shifts/jo-sat/cover", json={"reason": "wedding"}, headers=staff["jo"])
    assert off.status_code == 200, off.text
    cid = off.json()["cover_id"]

    assert cid in [x["id"] for x in _board(c, staff["meca"])["up_for_grabs"]]
    assert cid not in [x["id"] for x in _board(c, staff["pat"])["up_for_grabs"]]

    r = c.post(f"/api/me/cover/{cid}/claim", headers=staff["pat"])
    assert r.status_code == 409, r.text
    assert "approved leave" in _detail(r)
    assert db.get_shift_cover(cid)["status"] == "open"


def test_a_claim_made_before_the_leave_was_approved_is_blocked_at_approval():
    c, mgr, vid, ids, staff, _ = _world()
    db = get_db()
    monday = _next_week(vid)
    sat = monday + timedelta(days=5)
    rid = _publish(vid, monday, [_shift("jo-sat", ids["jo"], sat, time(16), time(23), "duty manager", 30)])
    later = _draft(vid, monday, [_shift("jo-sat-2", ids["jo"], sat, time(16), time(23), "duty manager", 30)])
    cid = c.post("/api/me/shifts/jo-sat/cover", json={"reason": "wedding"}, headers=staff["jo"]).json()["cover_id"]
    assert c.post(f"/api/me/cover/{cid}/claim", headers=staff["pat"]).status_code == 200
    _ask_leave(c, mgr, staff["pat"], vid, sat)

    chk = c.get(f"/api/cover/{cid}/checks", params={"venue_id": vid}, headers=mgr)
    assert chk.status_code == 200, chk.text
    body = chk.json()
    assert body["blocked"] is True
    leave = [x for x in body["checks"] if x["check"] == "leave"]
    assert [x["result"] for x in leave] == ["block"]
    assert leave[0]["message"].startswith("Pat Nguyen is on approved leave")
    assert sat.strftime("%a") in leave[0]["message"]

    r = c.post(f"/api/cover/{cid}/decide", json={"venue_id": vid, "approve": True}, headers=mgr)
    assert r.status_code == 409, r.text
    assert "approved leave" in _detail(r)
    assert _roster_shift(rid, "jo-sat").employee_id == ids["jo"]
    assert _roster_shift(later, "jo-sat-2").employee_id == ids["jo"]
    assert db.get_shift_cover(cid)["status"] == "claimed"


# ---------------------------------------------------------------------------
# [6] a newer roster's publish and the covers of the roster it replaces
# ---------------------------------------------------------------------------

def _imported_week_with_a_claim(draft_start=time(16)):
    """Jo's Saturday Duty Manager shift imported from Deputy, a newer draft of
    the week, Jo's leave leaving Saturday open on both, and Pat's claim on
    the imported week's cover."""
    c, mgr, vid, ids, staff, _ = _world()
    db = get_db()
    monday = _next_week(vid)
    sat = monday + timedelta(days=5)
    csv = "Name,Date,Start,End,Area,Break\n" + f"Jo Taylor,{sat:%d/%m/%Y},16:00,23:00,Duty Manager,30"
    imp = c.post("/api/setup/import-roster", json={"venue_id": vid, "content": csv}, headers=mgr)
    assert imp.status_code == 200, imp.text
    later = _draft(vid, monday, [_shift("jo-sat-2", ids["jo"], sat, draft_start, time(23), "Duty Manager", 30)])
    lv = c.post("/api/leave", json={"venue_id": vid, "employee_id": ids["jo"], "start_date": sat.isoformat(),
                                    "end_date": sat.isoformat(), "leave_type": "annual"}, headers=mgr)
    assert lv.status_code == 200, lv.text
    cover = next(x for x in db.list_shift_covers(vid) if x["status"] == "open")
    claim = c.post(f"/api/me/cover/{cover['id']}/claim", headers=staff["pat"])
    assert claim.status_code == 200, claim.text
    return c, mgr, vid, ids, staff, later, cover, sat


def _publish_api(c, mgr, rid):
    r = c.post(f"/api/v1/rosters/{rid}/publish", json={"skip_approval": True}, headers=mgr)
    assert r.status_code == 200, r.text
    assert r.json()["success"] is True, r.json()
    return r.json()


def test_publishing_a_newer_roster_carries_a_claim_to_the_same_open_slot():
    c, mgr, vid, ids, staff, later, old, sat = _imported_week_with_a_claim()
    db = get_db()
    pub = _publish_api(c, mgr, later)
    assert "had claimed" not in pub["message"] and pub["cover_notices"] == []

    live = [x for x in db.list_shift_covers(vid) if x["status"] in ("open", "claimed")]
    assert [(x["shift_id"], x["status"], x["claimed_by"]) for x in live] == [("jo-sat-2", "claimed", ids["pat"])]
    assert (live[0]["origin"], live[0]["shift_start"], live[0]["shift_end"]) == ("leave", "16:00", "23:00")
    gone = db.get_shift_cover(old["id"])
    assert gone["status"] == "withdrawn" and gone["claimed_by"] == ids["pat"]

    board = _board(c, staff["pat"])
    assert [x["shift_id"] for x in board["claimed_by_me"] if x["status"] == "claimed"] == ["jo-sat-2"]
    assert board["up_for_grabs"] == []

    ok = c.post(f"/api/cover/{live[0]['id']}/decide", json={"venue_id": vid, "approve": True}, headers=mgr)
    assert ok.status_code == 200, ok.text
    filled = _roster_shift(later, "jo-sat-2")
    assert filled.employee_id == ids["pat"] and filled.cost is not None


def test_publishing_a_roster_without_the_slot_names_the_claim_it_withdraws():
    c, mgr, vid, ids, staff, later, old, sat = _imported_week_with_a_claim(draft_start=time(17))
    db = get_db()
    pub = _publish_api(c, mgr, later)
    note = (f"Pat Nguyen had claimed {sat:%a} {sat.day} {sat:%b} 4-11pm Duty Manager, "
            "which isn't up for cover on this roster — let them know.")
    assert pub["cover_notices"] == [note]
    assert note in pub["message"]

    gone = db.get_shift_cover(old["id"])
    assert gone["status"] == "withdrawn" and gone["claimed_by"] == ids["pat"]
    live = [x for x in db.list_shift_covers(vid) if x["status"] in ("open", "claimed")]
    assert [(x["shift_id"], x["status"], x["claimed_by"]) for x in live] == [("jo-sat-2", "open", None)]


def test_publishing_a_newer_roster_keeps_a_swap_claim_on_the_requesters_same_shift():
    c, mgr, vid, ids, staff, _ = _world()
    db = get_db()
    monday = _next_week(vid)
    sat = monday + timedelta(days=5)
    csv = "Name,Date,Start,End,Area,Break\n" + f"Jo Taylor,{sat:%d/%m/%Y},16:00,23:00,Duty Manager,30"
    assert c.post("/api/setup/import-roster", json={"venue_id": vid, "content": csv}, headers=mgr).status_code == 200
    later = _draft(vid, monday, [_shift("jo-sat-2", ids["jo"], sat, time(16), time(23), "Duty Manager", 30)])
    imported = next(s for s in db.get_rosters_by_date_range(vid, sat, sat) if s.id != later)
    jo_sat = next(s.id for s in imported.shifts if s.date == sat)
    off = c.post(f"/api/me/shifts/{jo_sat}/cover", json={"reason": "wedding"}, headers=staff["jo"])
    assert off.status_code == 200, off.text
    assert c.post(f"/api/me/cover/{off.json()['cover_id']}/claim", headers=staff["pat"]).status_code == 200

    pub = _publish_api(c, mgr, later)
    assert pub["cover_notices"] == []
    live = [x for x in db.list_shift_covers(vid) if x["status"] in ("open", "claimed")]
    assert [(x["shift_id"], x["status"], x["claimed_by"], x["requested_by"], x["origin"], x["reason"])
            for x in live] == [("jo-sat-2", "claimed", ids["pat"], ids["jo"], None, "wedding")]
    ok = c.post(f"/api/cover/{live[0]['id']}/decide", json={"venue_id": vid, "approve": True}, headers=mgr)
    assert ok.status_code == 200, ok.text
    assert _roster_shift(later, "jo-sat-2").employee_id == ids["pat"]


def test_a_claimed_cover_takes_the_only_identical_slot_ahead_of_an_unclaimed_one():
    c, mgr, vid, ids, staff, _ = _world()
    db = get_db()
    monday = _next_week(vid)
    sat = monday + timedelta(days=5)
    csv = ("Name,Date,Start,End,Area,Break\n"
           f"Sam Kelly,{sat:%d/%m/%Y},16:00,23:00,Front Bar,30\n"
           f"Chloe Murphy,{sat:%d/%m/%Y},16:00,23:00,Front Bar,30")
    assert c.post("/api/setup/import-roster", json={"venue_id": vid, "content": csv}, headers=mgr).status_code == 200
    later = _draft(vid, monday, [_shift("sam-sat-2", ids["sam"], sat, time(16), time(23), "Front Bar", 30)])
    for who in ("sam", "chloe"):
        lv = c.post("/api/leave", json={"venue_id": vid, "employee_id": ids[who], "start_date": sat.isoformat(),
                                        "end_date": sat.isoformat(), "leave_type": "annual"}, headers=mgr)
        assert lv.status_code == 200, lv.text
    # The cover a publish reaches last, whichever order the store lists them in.
    covers = [x for x in db.list_shift_covers(vid) if x["status"] == "open"]
    assert len(covers) == 2
    claim = c.post(f"/api/me/cover/{covers[-1]['id']}/claim", headers=staff["pat"])
    assert claim.status_code == 200, claim.text

    pub = _publish_api(c, mgr, later)
    assert pub["cover_notices"] == [] and "had claimed" not in pub["message"]
    live = [x for x in db.list_shift_covers(vid) if x["status"] in ("open", "claimed")]
    assert [(x["shift_id"], x["status"], x["claimed_by"]) for x in live] == [("sam-sat-2", "claimed", ids["pat"])]
    board = _board(c, staff["pat"])
    assert [x["shift_id"] for x in board["claimed_by_me"]] == ["sam-sat-2"]
    assert board["up_for_grabs"] == []


_DROPPED = ("Pat Nguyen had claimed Sat 17 Oct 4-11pm Duty Manager, which isn't up for cover on this "
            "roster — let them know.")


@needs_node
def test_the_roster_page_tells_the_manager_which_claim_a_publish_dropped():
    from rosteriq.tests.test_wembley_a6 import ROSTER_STUBS, _publish_js
    resp = {"success": True, "delivered": {"email": 0, "sms": 0, "push": 0, "in_app": 0},
            "channels_available": ["in_app"], "staffing_warnings": ["Duty manager: nobody 17:00-23:00 Sat"],
            "cover_notices": [_DROPPED], "message": "Roster published successfully. " + _DROPPED}
    data = _run(ROSTER_STUBS + _publish_js() + r"""
async function main() {
  state.currentRosterId = 'r2';
  global.__resp = """ + json.dumps(resp) + r""";
  await publishRoster();
  global.__resp = { success: true, delivered: { email: 0, sms: 0, push: 0, in_app: 0 }, cover_notices: [] };
  const before = toasts.length;
  await publishRoster();
  console.log(JSON.stringify({ toasts: toasts, quiet: toasts.slice(before) }));
}
""")
    dropped = [t for t in data["toasts"] if "had claimed" in t["m"]]
    assert [t["m"] for t in dropped] == [_DROPPED]
    assert dropped[0]["k"] == "warning"
    assert len(data["quiet"]) == 1 and "had claimed" not in data["quiet"][0]["m"]


@needs_node
def test_publishing_the_first_roster_from_setup_names_a_dropped_claim():
    js = _fns(_dash(), "rosterDeliverySummary", "obPublishFirstRoster")
    resp = {"success": True, "delivered": {"email": 0, "sms": 0, "push": 0, "in_app": 0},
            "channels_available": ["in_app"], "cover_notices": [_DROPPED]}
    data = _run(r"""
global.obState = { rosterId: 'r2' };
global.fetchAPI = async function () { return """ + json.dumps(resp) + r"""; };
""" + js + r"""
async function main() {
  await obPublishFirstRoster();
  console.log(JSON.stringify({ out: el('ob-roster-result').textContent }));
}
""")
    assert "Published" in data["out"]
    assert data["out"].count(_DROPPED) == 1


@needs_node
def test_publishing_through_a_connected_tanda_names_a_dropped_claim():
    js = _fns(_dash(), "displayedRosterId", "pushToRosteringSystem")
    resp = {"success": True, "cover_notices": [_DROPPED]}
    data = _run(r"""
global.state = { currentVenue: 'wem', currentRosterId: 'r2', rosters: [] };
global.fetchAPI = async function (url) {
  calls.push(url);
  if (url.indexOf('/api/feeds/config/') === 0) return { enabled: url.slice(-5) === 'tanda' };
  return """ + json.dumps(resp) + r""";
};
""" + js + r"""
async function main() {
  await pushToRosteringSystem();
  console.log(JSON.stringify({ calls: calls, toasts: toasts }));
}
""")
    assert "/api/v1/rosters/r2/publish" in data["calls"]
    assert [t["m"] for t in data["toasts"] if "had claimed" in t["m"]] == [_DROPPED]


class _PGPublish(_PG):
    def list_leave_requests(self, venue_id):
        return []


def _open_slot(sid, start=time(10)):
    return Shift(id=sid, employee_id="", date=date(2026, 10, 16), start_time=start, end_time=time(17),
                 status=ShiftStatus.scheduled, role="front bar")


def _week(shifts):
    return Roster(id="r-new", venue_id="wem", week_start=date(2026, 10, 12), week_end=date(2026, 10, 18),
                  shifts=shifts, total_cost=None, created_at=datetime(2026, 10, 9))


def test_pg_publish_withdraws_conditionally_and_carries_a_claim_landed_meanwhile():
    from rosteriq.services.leave_shifts import post_open_covers
    pat = Employee(id="wem-pat", venue_id="wem", name="Pat Nguyen", employment_type="casual",
                   award_level="level_2", state="wa", hourly_base_rate="31.50",
                   created_at=datetime(2026, 10, 1), updated_at=datetime(2026, 10, 1))

    # read as open; Pat's claim committed before the UPDATE ran
    pg = _PGPublish([_pg_cover_row(shift_id="old-1")],
                    returning=[[_pg_cover_row(shift_id="old-1", status="withdrawn", claimed_by=pat.id)]],
                    staff=[pat])
    posted, notices = post_open_covers(pg, _week([_open_slot("new-1")]), now=datetime(2026, 10, 1))
    assert len(_withdrawals(pg)) == 1
    sql, params = _withdrawals(pg)[0]
    assert "WHERE id = %s AND status IN ('open', 'claimed')" in sql and params[-1] == "cv-1"
    inserts = _upserts(pg)
    assert len(inserts) == 1
    row = inserts[0]
    # (id, venue_id, shift_id, date, start, end, role, requested_by, reason, origin, claimed_by, status, ...)
    assert row[2:8] == ("new-1", date(2026, 10, 16), "10:00", "17:00", "front bar", "wem-jo")
    assert (row[9], row[10], row[11]) == ("leave", pat.id, "claimed")
    assert posted == [] and notices == []

    # read as claimed; approved before the UPDATE ran: left alone, the slot posted afresh
    pg = _PGPublish([_pg_cover_row(shift_id="old-1", status="claimed", claimed_by=pat.id)],
                    returning=[[]], staff=[pat])
    posted, notices = post_open_covers(pg, _week([_open_slot("new-1")]), now=datetime(2026, 10, 1))
    assert len(_withdrawals(pg)) == 1 and notices == []
    assert [(p[2], p[10], p[11]) for p in _upserts(pg)] == [("new-1", None, "open")]

    # no identical slot: withdrawn, the claimant named
    pg = _PGPublish([_pg_cover_row(shift_id="old-1", status="claimed", claimed_by=pat.id)],
                    staff=[pat])
    pg.returning = [[_pg_cover_row(shift_id="old-1", status="withdrawn", claimed_by=pat.id)]]
    posted, notices = post_open_covers(pg, _week([_open_slot("new-1", start=time(11))]),
                                       now=datetime(2026, 10, 1))
    assert notices == ["Pat Nguyen had claimed Fri 16 Oct 10am-5pm Front Bar, which isn't up for "
                       "cover on this roster — let them know."]
    assert [(p[2], p[10], p[11]) for p in _upserts(pg)] == [("new-1", None, "open")]


# ---------------------------------------------------------------------------
# [9] an open slot costs nothing until someone is on it
# ---------------------------------------------------------------------------

def test_leave_leaves_the_open_shift_with_no_cost():
    c, mgr, vid, ids, staff, _ = _world()
    monday = _next_week(vid)
    sat = monday + timedelta(days=5)
    rid = _publish(vid, monday, [_shift("jo-sat", ids["jo"], sat, time(16), time(23), "Duty Manager",
                                        cost=Decimal("210"))])
    _ask_leave(c, mgr, staff["jo"], vid, sat)
    opened = _roster_shift(rid, "jo-sat")
    assert (opened.employee_id, opened.cost) == ("", None)


def test_snapshot_labour_leaves_out_open_slots():
    from rosteriq.routes.snapshot import _labour_by_day
    c, mgr, vid, ids, staff, _ = _world()
    monday = _next_week(vid)
    fri = monday + timedelta(days=4)
    _publish(vid, monday, [
        _shift("pat-fri", ids["pat"], fri, time(10), time(16), "front bar", cost=Decimal("180")),
        _shift("open-stale", "", fri, time(16), time(23), "front bar", cost=Decimal("210")),
        _shift("open-new", "", fri, time(17), time(23), "front bar"),
    ])
    assert _labour_by_day(get_db(), vid, fri, fri) == {fri.isoformat(): 180.0}


@needs_node
def test_reports_count_an_open_slots_hours_but_never_its_cost():
    js = _fns(_dash(), "_reportRange", "_realReportRows")
    rosters = [{"shifts": [
        {"date": "2026-10-16", "start_time": "10:00:00", "end_time": "16:00:00", "break_minutes": 0,
         "employee_id": "pat", "cost": "180.00"},
        {"date": "2026-10-16", "start_time": "16:00:00", "end_time": "23:00:00", "break_minutes": 0,
         "employee_id": "", "cost": "210.00"},
        {"date": "2026-10-23", "start_time": "16:00:00", "end_time": "23:00:00", "break_minutes": 0,
         "employee_id": "", "cost": None},
        {"date": "2026-10-23", "start_time": "10:00:00", "end_time": "18:00:00", "break_minutes": 30,
         "employee_id": "pat", "cost": "273.84"},
    ]}]
    data = _run(r"""
global.state = { rosters: """ + json.dumps(rosters) + r""" };
el('report-start-date').value = '2026-10-12';
el('report-end-date').value = '2026-10-25';
""" + js + r"""
async function main() { console.log(JSON.stringify({ rows: _realReportRows() })); }
""")
    assert data["rows"] == [
        {"date": "2026-10-16", "hours": 13, "staff": 1, "cost": 180},
        {"date": "2026-10-23", "hours": 14.5, "staff": 1, "cost": 274},
    ]


# ---------------------------------------------------------------------------
# [20] the cover approval toast
# ---------------------------------------------------------------------------

@needs_node
def test_approving_a_claim_on_an_open_shift_says_it_is_filled():
    js = _fns(_dash(), "decideCover")
    rows = [
        {"id": "cv-open", "origin": "open", "role": "front bar", "shift_date": "2026-10-14",
         "claimed_by_name": "Ellie <b>Import</b>", "requested_by_name": "Open shift"},
        {"id": "cv-leave", "origin": "leave", "role": "front bar", "shift_date": "2026-10-14",
         "claimed_by_name": "Ellie Import", "requested_by_name": "Cara"},
        {"id": "cv-swap", "origin": "staff", "role": "front bar", "shift_date": "2026-10-14",
         "claimed_by_name": "Ellie Import", "requested_by_name": "Cara"},
    ]
    data = _run(r"""
global.cvRows = """ + json.dumps(rows) + r""";
global.state = { currentVenue: 'wem' };
global.prompt = function () { return ''; };
global.alert = function () {};
global.loadCoverRequests = function () {};
global.fetchAPI = async function (url) {
  calls.push(url);
  return url.indexOf('/checks') >= 0 ? { blocked: false, checks: [] } : { status: 'approved' };
};
""" + js + r"""
async function main() {
  for (let i = 0; i < cvRows.length; i++) await decideCover(i, true);
  console.log(JSON.stringify({ toasts: toasts }));
}
""")
    msgs = [t["m"] for t in data["toasts"]]
    assert msgs[0].startswith("Shift filled: Ellie <b>Import</b> is on it")
    assert msgs[1].startswith("Shift filled: Ellie Import is on it")
    assert msgs[2] == "Swap approved — roster updated ✓"
    assert not any("Swap" in m for m in msgs[:2])


# ---------------------------------------------------------------------------
# [21] Copy last week: other-venue clashes by person and shift
# ---------------------------------------------------------------------------

def test_copy_names_the_shift_here_each_other_venue_clash_collides_with():
    c = TestClient(app)
    mgr, vid, _ = _manager_with_venue(c)
    apo = _sister_venue(c, mgr)
    source_monday = _next_monday()
    target = source_monday + timedelta(days=7)
    meca, meca_apo = _meca_at_both(vid, apo)
    sun = source_monday + timedelta(days=6)
    source = _roster(vid, source_monday, [
        _b3_shift(meca.id, sun, time(10), time(14), brk=0),
        _b3_shift(meca.id, sun, time(17), time(21), brk=0),
    ], state="published")
    _roster(apo, target, [_b3_shift(meca_apo.id, sun + timedelta(days=7), time(9), time(22), role="bar", brk=0)],
            state="published")

    resp = c.post(f"/rosters/{source.id}/copy-to?week_start={target.isoformat()}", headers=mgr)
    assert resp.status_code == 200, resp.text
    body = resp.json()
    new_sun = (sun + timedelta(days=7)).isoformat()
    on_screen = {s["id"]: s for s in body["roster"]["shifts"]}
    clashes = body["cross_venue_clashes"]
    assert sorted((x["name"], x["date"], x["start"], x["end"]) for x in clashes) == [
        ("Meca Rossi", new_sun, "10:00", "14:00"), ("Meca Rossi", new_sun, "17:00", "21:00")]
    assert all(x["shift_id"] in on_screen and on_screen[x["shift_id"]]["start_time"][:5] == x["start"]
               for x in clashes)
    assert all(x["message"].startswith("Meca Rossi is on at Apothecary 09:00-22:00") for x in clashes)
    assert len(body["cross_venue_warnings"]) == len(set(body["cross_venue_warnings"])) == 1


@needs_node
def test_copy_report_groups_other_venue_clashes_by_person_and_shift_once_each():
    js = _fns(_dash(), "rosterDayLabel", "renderCopyReport")
    msg = "Meca <i>Rossi</i> is on at Apothecary 09:00-22:00 on Sun 25 Oct"
    rest = "Meca <i>Rossi</i> is on at Apothecary 23:30-02:00 on Sun 25 Oct — only 2.5h between that shift and this one"
    result = {
        "copied": 2, "skipped": [], "left_open": [], "visa_flags": [],
        "cross_venue_warnings": [msg, msg, rest],
        "cross_venue_clashes": [
            {"employee_id": "m", "name": "Meca <i>Rossi</i>", "shift_id": "s1", "date": "2026-10-25",
             "start": "10:00", "end": "14:00", "role": "front bar", "message": msg},
            {"employee_id": "m", "name": "Meca <i>Rossi</i>", "shift_id": "s2", "date": "2026-10-25",
             "start": "17:00", "end": "21:00", "role": "front bar", "message": msg},
            {"employee_id": "m", "name": "Meca <i>Rossi</i>", "shift_id": "s2", "date": "2026-10-25",
             "start": "17:00", "end": "21:00", "role": "front bar", "message": rest},
            {"employee_id": "m", "name": "Meca <i>Rossi</i>", "shift_id": "s2", "date": "2026-10-25",
             "start": "17:00", "end": "21:00", "role": "front bar", "message": rest},
        ],
    }
    data = _run(js + r"""
async function main() {
  renderCopyReport(""" + json.dumps(result) + r""");
  console.log(JSON.stringify({ html: el('roster-copy-report').innerHTML, day: rosterDayLabel('2026-10-25') }));
}
""")
    html = data["html"]
    assert "<i>" not in html
    first = html.index(data["day"] + " 10:00-14:00")
    second = html.index(data["day"] + " 17:00-21:00")
    assert first < second
    esc_msg = "Meca &lt;i&gt;Rossi&lt;/i&gt; is on at Apothecary 09:00-22:00 on Sun 25 Oct"
    assert html.count(esc_msg) == 2
    assert html.index(esc_msg) > first and html.index(esc_msg, html.index(esc_msg) + 1) > second
    assert html.count("only 2.5h between that shift and this one") == 1
    assert html.count("Meca &lt;i&gt;Rossi&lt;/i&gt; — ") == 2


@needs_node
def test_copy_report_without_clash_rows_lists_each_warning_once():
    js = _fns(_dash(), "rosterDayLabel", "renderCopyReport")
    msg = "Meca Rossi is on at Apothecary 09:00-22:00 on Sun 25 Oct"
    result = {"copied": 2, "skipped": [], "left_open": [], "visa_flags": [],
              "cross_venue_warnings": [msg, msg]}
    data = _run(js + r"""
async function main() {
  renderCopyReport(""" + json.dumps(result) + r""");
  console.log(JSON.stringify({ html: el('roster-copy-report').innerHTML }));
}
""")
    assert data["html"].count(msg) == 1
