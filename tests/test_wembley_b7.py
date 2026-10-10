"""
Leave, cover and salaried managers at the Wembley.

[60] Approving leave leaves the person's upcoming shifts OPEN: unassigned but
     kept on the roster with role and time, posted to the cover board for
     people who can work them, gone from the person's /my, and named in the
     Today briefing. Leave has a type; managers can record leave for someone,
     past-dated too, with an audit trail; payroll prep shows approved paid
     leave hours as their own line for the advisor.
[61] Covers a person can't work aren't offered to them, claiming one says
     "You're not trained for Duty Manager", and approval runs and names the
     skill, hours, visa-cap, rest and other-venue checks — a supervisory area
     is a hard block for anyone without the skill.
[79] Salaried managers: pay_basis + weekly_salary on the record and in
     Postgres; costed at weekly_salary / 7 per rostered day with no penalties
     or overtime; a fixed line in payroll prep; salary rows in the staff
     import become salaried.
"""

import sys
import types
import uuid
from contextlib import contextmanager
from datetime import date, datetime, time as dtime, timedelta
from decimal import Decimal

from fastapi.testclient import TestClient

from rosteriq.api import app
from rosteriq.cost_calculator import (
    calculate_roster_cost, calculate_shift_cost_breakdown, cost_employee_week,
)
from rosteriq.database import PostgresStore, get_db
from rosteriq.models import (
    AwardLevel, Employee, EmploymentType, Roster, Shift, ShiftStatus, State,
)
from rosteriq.services.clock import venue_today

PW = "Passw0rd!234"
AREAS = [
    {"name": "duty manager", "label": "Duty Manager", "requires_skill": True,
     "min_by_day": {d: 1 for d in ("mon", "tue", "wed", "thu", "fri", "sat", "sun")}},
    {"name": "front bar", "label": "Front Bar",
     "min_by_day": {d: 1 for d in ("mon", "tue", "wed", "thu", "fri", "sat", "sun")}},
    {"name": "floor runner", "label": "Floor Runner", "min_by_day": {}},
]
PEOPLE = {
    "jo": ("Jo Taylor", "full_time", ["Duty Manager"]),
    "meca": ("Meca Ryan", "casual", ["Duty Manager"]),
    "chloe": ("Chloe Murphy", "casual", ["Floor Runner"]),
    "sam": ("Sam Kelly", "casual", ["Front Bar"]),
    "pat": ("Pat Nguyen", "casual", ["Duty Manager", "Front Bar"]),
}


def _login(c, email):
    c.post("/api/auth/register", json={"email": email, "password": PW, "name": "U"})
    tok = c.post("/api/auth/login", json={"email": email, "password": PW}).json()["access_token"]
    return {"Authorization": f"Bearer {tok}"}


def _venue(c, h, vid, name):
    r = c.post("/venues", json={"id": vid, "name": name, "state": "wa",
                                "timezone": "Australia/Perth", "max_labour_pct": 30,
                                "tanda_org_id": "", "areas": AREAS,
                                "created_at": "2026-07-01T00:00:00"}, headers=h)
    assert r.status_code in (200, 201), r.text


def _employee(c, mgr, vid, key, name, etype, skills, email=None, **extra):
    body = {"id": f"{vid}-{key}", "venue_id": vid, "name": name, "employment_type": etype,
            "award_level": "level_3", "state": "wa", "hourly_base_rate": "32.60",
            "skills": skills, "email": email,
            "created_at": "2026-07-01T00:00:00", "updated_at": "2026-07-01T00:00:00", **extra}
    r = c.post("/employees", json=body, headers=mgr)
    assert r.status_code == 200, r.text
    return body["id"]


def _as_staff(email, vid):
    db = get_db()
    rec = db.get_user_by_email(email)
    rec["venue_ids"], rec["role"] = [vid], "staff"
    db.save_user(rec)


def _world(extra=None):
    """A manager, the Wembley with its areas, and five linked staff."""
    c = TestClient(app)
    _login(c, f"b7_boot_{uuid.uuid4().hex[:6]}@x.com")
    mgr_email = f"b7m{uuid.uuid4().hex[:8]}@x.com"
    h = _login(c, mgr_email)
    vid = f"b7-{uuid.uuid4().hex[:6]}"
    _venue(c, h, vid, "The Wembley Hotel")
    mgr = _login(c, mgr_email)
    people = {}
    for key, (name, etype, skills) in PEOPLE.items():
        email = f"b7{key}{uuid.uuid4().hex[:6]}@x.com"
        emp_id = _employee(c, mgr, vid, key, name, etype, skills, email,
                           **((extra or {}).get(key, {})))
        people[key] = (emp_id, email)
    staff = {}
    for key, (emp_id, email) in people.items():
        hdr = _login(c, email)
        _as_staff(email, vid)
        staff[key] = hdr
    ids = {k: v[0] for k, v in people.items()}
    return c, mgr, vid, ids, staff, mgr_email


def _monday(d):
    return d - timedelta(days=d.weekday())


def _shift(sid, emp, day, start, end, role, brk=0, cost=None):
    return Shift(id=sid, employee_id=emp, date=day, start_time=start, end_time=end,
                 break_minutes=brk, status=ShiftStatus.scheduled, role=role, cost=cost)


def _publish(vid, monday, shifts, rid=None):
    rid = rid or f"b7r-{uuid.uuid4().hex[:6]}"
    db = get_db()
    db.save_roster(Roster(id=rid, venue_id=vid, week_start=monday,
                          week_end=monday + timedelta(days=6), shifts=shifts,
                          total_cost=None, created_at=datetime.utcnow()))
    db.update_roster_state(rid, "published", "test", "test")
    return rid


def _next_week(vid):
    return _monday(venue_today(vid, get_db())) + timedelta(days=7)


def _roster_shift(rid, sid):
    return next(s for s in get_db().get_roster(rid).shifts if s.id == sid)


def _detail(r):
    body = r.json()
    return str(body.get("detail") or body.get("error", {}).get("message") or body)


# ---------------------------------------------------------------------------
# [60] Approving leave
# ---------------------------------------------------------------------------

def test_approving_leave_leaves_the_shift_open_on_the_roster_and_the_cover_board():
    c, mgr, vid, ids, staff, _ = _world()
    nm = _next_week(vid)
    fri, sat = nm + timedelta(days=4), nm + timedelta(days=5)
    rid = _publish(vid, nm, [
        _shift("jo-fri", ids["jo"], fri, dtime(16), dtime(23), "Duty Manager"),
        _shift("jo-sat", ids["jo"], sat, dtime(16), dtime(23), "Duty Manager"),
        _shift("meca-thu", ids["meca"], nm + timedelta(days=3), dtime(10), dtime(15), "Duty Manager"),
    ])

    r = c.post("/api/me/leave", json={"start_date": sat.isoformat(), "end_date": sat.isoformat(),
                                      "reason": "sick", "leave_type": "personal"},
               headers=staff["jo"])
    assert r.status_code == 200, r.text
    req_id = r.json()["request_id"]

    # The approve dialog names the shift that will be left open
    imp = c.get(f"/api/leave/impact?venue_id={vid}&employee_id={ids['jo']}"
                f"&start_date={sat}&end_date={sat}", headers=mgr)
    assert imp.status_code == 200, imp.text
    assert "Jo Taylor is rostered Sat 4-11pm as Duty Manager; this shift will be left open" \
        in imp.json()["message"]
    assert imp.json()["will_open_count"] == 1

    dec = c.post(f"/api/leave/{req_id}/decide", json={"venue_id": vid, "approve": True},
                 headers=mgr)
    assert dec.status_code == 200, dec.text
    assert [o["label"] for o in dec.json()["opened_shifts"]] == ["Sat 4-11pm Duty Manager"]

    # Unassigned but kept on the roster with its role and time
    sat_shift = _roster_shift(rid, "jo-sat")
    assert sat_shift.employee_id == ""
    assert (sat_shift.role, sat_shift.start_time, sat_shift.end_time) == \
        ("Duty Manager", dtime(16), dtime(23))
    assert _roster_shift(rid, "jo-fri").employee_id == ids["jo"]

    # Off Jo's phone; Friday stays
    mine = c.get("/api/me/shifts", headers=staff["jo"]).json()["shifts"]
    assert [s["date"] for s in mine] == [fri.isoformat()]

    # On the board for a trained DM, not for the floor runner, not as Jo's own
    board = c.get("/api/me/cover", headers=staff["meca"]).json()
    grabs = [x for x in board["up_for_grabs"] if x["shift_id"] == "jo-sat"]
    assert len(grabs) == 1
    assert grabs[0]["requested_by_name"] == "Open shift" and grabs[0]["reason"] == ""
    assert not c.get("/api/me/cover", headers=staff["chloe"]).json()["up_for_grabs"]
    jo_board = c.get("/api/me/cover", headers=staff["jo"]).json()
    assert jo_board["mine"] == [] and jo_board["up_for_grabs"] == []
    r = c.post(f"/api/me/cover/{grabs[0]['id']}/cancel", headers=staff["jo"])
    assert r.status_code in (403, 409)

    # The leave record: type, rostered hours as the default paid hours, what opened
    rows = c.get(f"/api/leave?venue_id={vid}&status=approved", headers=mgr).json()["requests"]
    row = next(x for x in rows if x["id"] == req_id)
    assert row["leave_type"] == "personal" and row["leave_type_label"] == "Personal/carer's leave"
    assert row["paid_hours"] == 7.0
    assert row["opened_shifts"] == ["Sat 4-11pm Duty Manager"]

    # Filling it puts Meca on the shift
    assert c.post(f"/api/me/cover/{grabs[0]['id']}/claim", headers=staff["meca"]).status_code == 200
    ok = c.post(f"/api/cover/{grabs[0]['id']}/decide", json={"venue_id": vid, "approve": True},
                headers=mgr)
    assert ok.status_code == 200, ok.text
    filled = _roster_shift(rid, "jo-sat")
    assert filled.employee_id == ids["meca"] and filled.cost is not None


def test_open_shift_counts_as_a_gap_not_cover_and_costs_nobody():
    from rosteriq.services.conflict_detector import ConflictDetector
    c, mgr, vid, ids, staff, _ = _world()
    nm = _next_week(vid)
    sat = nm + timedelta(days=5)
    rid = _publish(vid, nm, [_shift("jo-sat", ids["jo"], sat, dtime(16), dtime(23), "Duty Manager")])
    r = c.post("/api/leave", json={"venue_id": vid, "employee_id": ids["jo"],
                                   "start_date": sat.isoformat(), "end_date": sat.isoformat(),
                                   "leave_type": "annual"}, headers=mgr)
    assert r.status_code == 200, r.text
    db = get_db()
    roster = db.get_roster(rid)
    emps = {e.id: e for e in db.get_employees(vid)}
    assert calculate_roster_cost(roster, emps, State.wa) == Decimal("0.00")
    venue = db.get_venue(vid)
    conflicts = ConflictDetector().detect_conflicts(roster, venue.model_copy(
        update={"min_staff": {"duty manager": 1}}), list(emps.values()))
    assert any("Duty manager: nobody" in x.message for x in conflicts), \
        [x.message for x in conflicts]


def test_today_briefing_names_the_shift_left_open_by_leave():
    c, mgr, vid, ids, staff, _ = _world()
    db = get_db()
    tomorrow = venue_today(vid, db) + timedelta(days=1)
    _publish(vid, _monday(tomorrow),
             [_shift("jo-tmr", ids["jo"], tomorrow, dtime(16), dtime(23), "Duty Manager")])
    r = c.post("/api/me/leave", json={"start_date": tomorrow.isoformat(),
                                      "end_date": tomorrow.isoformat(), "leave_type": "unavailable"},
               headers=staff["jo"])
    req_id = r.json()["request_id"]
    assert c.post(f"/api/leave/{req_id}/decide", json={"venue_id": vid, "approve": True},
                  headers=mgr).status_code == 200

    brief = c.get(f"/api/briefing?venue_id={vid}", headers=mgr).json()
    expected = f"{tomorrow.strftime('%a')} 4-11pm Duty Manager is open after leave"
    assert any(expected in line for line in brief["attention"]), brief["attention"]
    assert brief["open_shifts"][0]["after_leave"] is True
    # Unpaid unavailability records no paid hours
    row = next(x for x in c.get(f"/api/leave?venue_id={vid}", headers=mgr).json()["requests"]
               if x["id"] == req_id)
    assert row["paid_hours"] is None


def test_manager_records_past_dated_sick_leave_with_an_audit_trail():
    c, mgr, vid, ids, staff, mgr_email = _world()
    db = get_db()
    today = venue_today(vid, db)
    yesterday = today - timedelta(days=1)
    rid = _publish(vid, _monday(yesterday),
                   [_shift("jo-yday", ids["jo"], yesterday, dtime(16), dtime(23), "Duty Manager")])

    body = {"venue_id": vid, "employee_id": ids["jo"], "start_date": yesterday.isoformat(),
            "end_date": yesterday.isoformat(), "leave_type": "personal", "paid_hours": 7.5,
            "reason": "rang in sick"}
    # Staff can't record leave, and a manager can't for someone outside their venue
    assert c.post("/api/leave", json=body, headers=staff["meca"]).status_code == 403
    assert c.post("/api/leave", json={**body, "employee_id": "nobody-here"},
                  headers=mgr).status_code == 404
    r = c.post("/api/leave", json=body, headers=mgr)
    assert r.status_code == 200, r.text
    out = r.json()
    assert out["status"] == "approved" and out["opened_shifts"] == []
    # A shift that already happened stays as it was
    assert _roster_shift(rid, "jo-yday").employee_id == ids["jo"]

    rows = db.list_events(venue_id=vid, action_prefix="leave.record")
    assert len(rows) == 1
    details = rows[0]["details"]
    assert details["past_dated"] is True and details["leave_type"] == "personal"
    assert details["employee"] == "Jo Taylor" and float(details["paid_hours"]) == 7.5
    mgr_id = db.get_user_by_email(mgr_email)["id"]
    assert rows[0]["user_id"] == mgr_id and details["entered_by"] == mgr_id
    listed = c.get(f"/api/leave?venue_id={vid}", headers=mgr).json()["requests"]
    assert listed[0]["entered_by_manager"] is True


def test_leave_types_on_staff_requests():
    c, mgr, vid, ids, staff, _ = _world()
    nxt = venue_today(vid, get_db()) + timedelta(days=20)
    bad = c.post("/api/me/leave", json={"start_date": nxt.isoformat(), "end_date": nxt.isoformat(),
                                        "leave_type": "holiday"}, headers=staff["sam"])
    assert bad.status_code == 422
    ok = c.post("/api/me/leave", json={"start_date": nxt.isoformat(), "end_date": nxt.isoformat(),
                                       "leave_type": "annual"}, headers=staff["sam"])
    assert ok.status_code == 200, ok.text
    plain = c.post("/api/me/leave", json={"start_date": (nxt + timedelta(days=5)).isoformat(),
                                          "end_date": (nxt + timedelta(days=5)).isoformat()},
                   headers=staff["sam"])
    assert plain.status_code == 200
    mine = {r["id"]: r for r in c.get("/api/me/leave", headers=staff["sam"]).json()["requests"]}
    assert mine[ok.json()["request_id"]]["leave_type_label"] == "Annual leave"
    assert mine[plain.json()["request_id"]]["leave_type"] == "other"


def test_payroll_prep_shows_approved_paid_leave_hours_as_their_own_line():
    c, mgr, vid, ids, staff, _ = _world()
    today = date.today()
    r = c.post("/api/leave", json={"venue_id": vid, "employee_id": ids["jo"],
                                   "start_date": today.isoformat(), "end_date": today.isoformat(),
                                   "leave_type": "annual", "paid_hours": 7.6}, headers=mgr)
    assert r.status_code == 200, r.text
    # Unpaid leave never shows up as paid hours
    c.post("/api/leave", json={"venue_id": vid, "employee_id": ids["sam"],
                               "start_date": today.isoformat(), "end_date": today.isoformat(),
                               "leave_type": "unpaid", "paid_hours": 5}, headers=mgr)
    _approved_punch(c, mgr, vid, ids["chloe"])

    batch = _prepare_actuals(c, mgr, vid, today, today)
    lines = {e["employee_id"]: e for e in batch["employees"]}
    jo = lines[ids["jo"]]
    assert [(le["leave_type"], le["hours"], le["amount"]) for le in jo["leave_entries"]] == \
        [("annual", "7.60", "0.00")]
    assert "advisor" in jo["leave_entries"][0]["note"]
    assert Decimal(jo["total_gross"]) == Decimal("0.00")
    assert ids["sam"] not in lines

    text = c.get(f"/api/payroll/batch/{batch['batch_id']}/csv?venue_id={vid}", headers=mgr).text
    assert "Paid leave hours (advisor to price)" in text
    jo_row = [line for line in text.splitlines() if line.startswith("Jo Taylor")][0]
    assert "annual 7.60h" in jo_row


def _approved_punch(c, mgr, vid, emp_id, start=dtime(9), end=dtime(17, 30), brk=30):
    c.post("/api/clock/in", json={"venue_id": vid, "employee_id": emp_id}, headers=mgr)
    ts_id = c.post("/api/clock/out", json={"venue_id": vid, "employee_id": emp_id},
                   headers=mgr).json()["timesheet_id"]
    today = date.today()
    r = c.post(f"/api/clock/timesheets/{ts_id}/review", json={
        "venue_id": vid, "approve": True,
        "clock_in": datetime.combine(today, start).isoformat(),
        "clock_out": datetime.combine(today, end).isoformat(),
        "break_minutes": brk, "note": "b7 payroll test",
    }, headers=mgr)
    assert r.status_code == 200, r.text


def _prepare_actuals(c, mgr, vid, start, end):
    r = c.post("/api/payroll/prepare-actuals", json={
        "venue_id": vid, "period_start": start.isoformat(), "period_end": end.isoformat(),
        "state": "wa"}, headers=mgr)
    assert r.status_code == 200, r.text
    return get_db().get_payroll_batch(r.json()["batch_id"])


# ---------------------------------------------------------------------------
# [61] Cover checks
# ---------------------------------------------------------------------------

def test_covers_are_only_offered_to_people_who_can_work_them():
    c, mgr, vid, ids, staff, _ = _world()
    nm = _next_week(vid)
    fri = nm + timedelta(days=4)
    _publish(vid, nm, [
        _shift("jo-fri", ids["jo"], fri, dtime(16), dtime(23), "Duty Manager"),
        _shift("sam-fri", ids["sam"], fri, dtime(17), dtime(23), "Front Bar"),
        _shift("chloe-fri", ids["chloe"], fri, dtime(11, 30), dtime(15), "Floor Runner"),
        _shift("sam-sun", ids["sam"], nm + timedelta(days=6), dtime(10), dtime(14), "general"),
    ])
    # A generic shift is anyone's to pick up
    gen = c.post("/api/me/shifts/sam-sun/cover", json={}, headers=staff["sam"])
    assert gen.status_code == 200
    assert [x["role"] for x in c.get("/api/me/cover", headers=staff["chloe"]).json()["up_for_grabs"]] \
        == ["general"]
    c.post(f"/api/me/cover/{gen.json()['cover_id']}/cancel", headers=staff["sam"])
    dm = c.post("/api/me/shifts/jo-fri/cover", json={"reason": ""}, headers=staff["jo"])
    bar = c.post("/api/me/shifts/sam-fri/cover", json={"reason": "uni exam"}, headers=staff["sam"])
    assert dm.status_code == 200 and bar.status_code == 200

    def grabs(who):
        return {x["role"] for x in c.get("/api/me/cover", headers=staff[who]).json()["up_for_grabs"]}

    assert grabs("chloe") == set()
    assert grabs("meca") == {"Duty Manager"}
    assert grabs("pat") == {"Duty Manager", "Front Bar"}

    r = c.post(f"/api/me/cover/{dm.json()['cover_id']}/claim", headers=staff["chloe"])
    assert r.status_code == 409 and "You're not trained for Duty Manager" in _detail(r)
    r = c.post(f"/api/me/cover/{bar.json()['cover_id']}/claim", headers=staff["chloe"])
    assert r.status_code == 409 and "You're not trained for Front Bar" in _detail(r)


def test_a_supervisory_cover_is_hard_blocked_for_someone_without_the_skill():
    c, mgr, vid, ids, staff, _ = _world()
    nm = _next_week(vid)
    fri = nm + timedelta(days=4)
    rid = _publish(vid, nm, [_shift("jo-fri", ids["jo"], fri, dtime(16), dtime(23), "Duty Manager")])
    db = get_db()
    # A claim that got past the board (made before the skill rule, or skills
    # edited since): approval still refuses it.
    db.save_shift_cover({"id": "cv-b7-dm", "venue_id": vid, "shift_id": "jo-fri", "shift_date": fri,
                         "shift_start": "16:00", "shift_end": "23:00", "role": "Duty Manager",
                         "requested_by": ids["jo"], "reason": "", "claimed_by": ids["chloe"],
                         "status": "claimed", "created_at": datetime.utcnow()})
    chk = c.get(f"/api/cover/cv-b7-dm/checks?venue_id={vid}", headers=mgr)
    assert chk.status_code == 200, chk.text
    body = chk.json()
    assert body["blocked"] is True
    trained = next(x for x in body["checks"] if x["check"] == "trained")
    assert trained["result"] == "block" and "Chloe Murphy isn't trained for Duty Manager" in trained["message"]

    r = c.post("/api/cover/cv-b7-dm/decide", json={"venue_id": vid, "approve": True}, headers=mgr)
    assert r.status_code == 409 and "isn't trained for Duty Manager" in _detail(r)
    assert _roster_shift(rid, "jo-fri").employee_id == ids["jo"]
    # Staff can't read the checks
    assert c.get(f"/api/cover/cv-b7-dm/checks?venue_id={vid}", headers=staff["meca"]).status_code == 403


def test_approval_names_hours_visa_and_rest_results():
    c, mgr, vid, ids, staff, _ = _world(extra={"pat": {"max_hours_per_week": 10,
                                                       "visa_work_limit_fortnight": 12}})
    nm = _next_week(vid)
    thu, fri = nm + timedelta(days=3), nm + timedelta(days=4)
    _publish(vid, nm, [
        _shift("jo-fri", ids["jo"], fri, dtime(16), dtime(23), "Duty Manager"),
        _shift("pat-thu", ids["pat"], thu, dtime(8), dtime(14), "Front Bar"),
        _shift("pat-fri", ids["pat"], fri, dtime(7), dtime(10), "Front Bar"),
    ])
    cover_id = c.post("/api/me/shifts/jo-fri/cover", json={}, headers=staff["jo"]).json()["cover_id"]
    assert c.post(f"/api/me/cover/{cover_id}/claim", headers=staff["pat"]).status_code == 200

    body = c.get(f"/api/cover/{cover_id}/checks?venue_id={vid}", headers=mgr).json()
    results = {x["check"]: x for x in body["checks"]}
    assert body["blocked"] is False
    assert results["trained"]["result"] == "ok"
    assert results["hours"]["result"] == "warn" and "16h this week" in results["hours"]["message"]
    assert results["visa"]["result"] == "warn" and "12h fortnight visa cap" in results["visa"]["message"]
    assert results["rest"]["result"] == "warn" and "Only 6h" in results["rest"]["message"]
    assert all("Pat Nguyen" in x["message"] for x in body["checks"])

    r = c.post(f"/api/cover/{cover_id}/decide", json={"venue_id": vid, "approve": True}, headers=mgr)
    assert r.status_code == 200, r.text
    assert len(r.json()["warnings"]) == 3
    event = get_db().list_events(venue_id=vid, action_prefix="cover.decide")[0]
    assert len(event["details"]["warnings"]) == 3


def _fake_identity(monkeypatch, records):
    import rosteriq.services as services_pkg
    fake = types.ModuleType("rosteriq.services.person_identity")
    fake.same_person_records = lambda db, employee, scope=None: [
        r for r in records if r[0].email == employee.email]
    fake.busy_elsewhere = lambda *a, **k: []
    monkeypatch.setitem(sys.modules, "rosteriq.services.person_identity", fake)
    monkeypatch.setattr(services_pkg, "person_identity", fake, raising=False)


def test_a_shift_at_the_other_venue_at_the_same_time_blocks_approval(monkeypatch):
    c, mgr, vid, ids, staff, mgr_email = _world()
    nm = _next_week(vid)
    fri = nm + timedelta(days=4)
    _publish(vid, nm, [_shift("jo-fri", ids["jo"], fri, dtime(16), dtime(23), "Duty Manager")])
    apo = f"b7apo-{uuid.uuid4().hex[:6]}"
    _venue(c, mgr, apo, "Apothecary")
    mgr2 = _login(c, mgr_email)
    db = get_db()
    meca = db.get_employee(ids["meca"])
    twin = _employee(c, mgr2, apo, "meca", "Meca Ryan", "casual", ["Duty Manager"], meca.email)
    _publish(apo, nm, [_shift("apo-fri", twin, fri, dtime(17), dtime(22), "Duty Manager")])
    _fake_identity(monkeypatch, [(db.get_employee(twin), apo)])

    cover_id = c.post("/api/me/shifts/jo-fri/cover", json={}, headers=staff["jo"]).json()["cover_id"]
    assert c.post(f"/api/me/cover/{cover_id}/claim", headers=staff["meca"]).status_code == 200
    body = c.get(f"/api/cover/{cover_id}/checks?venue_id={vid}", headers=mgr2).json()
    elsewhere = next(x for x in body["checks"] if x["check"] == "elsewhere")
    assert elsewhere["result"] == "block" and "Apothecary" in elsewhere["message"]
    r = c.post(f"/api/cover/{cover_id}/decide", json={"venue_id": vid, "approve": True}, headers=mgr2)
    assert r.status_code == 409 and "Apothecary" in _detail(r)


# ---------------------------------------------------------------------------
# [79] Salaried managers
# ---------------------------------------------------------------------------

def _salaried(**kw):
    base = dict(id="sal-1", venue_id="v", name="Dana Venue Mgr",
                employment_type=EmploymentType.full_time, award_level=AwardLevel.level_6,
                state=State.wa, hourly_base_rate=Decimal("38.00"), pay_basis="salaried",
                weekly_salary=Decimal("1500.00"), created_at=datetime(2026, 1, 1),
                updated_at=datetime(2026, 1, 1))
    base.update(kw)
    return Employee(**base)


def test_salaried_shift_costs_a_day_of_salary_with_no_penalties_or_overtime():
    dana = _salaried()
    sunday = date(2026, 10, 18)
    bd = calculate_shift_cost_breakdown(
        dana, _shift("s", dana.id, sunday, dtime(16), dtime(23, 30), "Duty Manager"), State.wa)
    # 1500 / 7 = 214.29, plus 12% super = 25.71
    assert (bd.base_cost, bd.penalty_cost, bd.casual_loading) == \
        (Decimal("214.29"), Decimal("0.00"), Decimal("0.00"))
    assert bd.total_cost == Decimal("240.00")

    # A 52-hour week over six days, two shifts on one of them: six days of salary, no overtime
    monday = date(2026, 10, 12)
    shifts = [_shift(f"d{i}", dana.id, monday + timedelta(days=i), dtime(8), dtime(17), "Duty Manager")
              for i in range(6)]
    shifts.append(_shift("split", dana.id, monday, dtime(19), dtime(23), "Duty Manager"))
    week = cost_employee_week(dana, shifts, State.wa)
    assert week["overtime_cost"] == Decimal("0.00") and week["overtime_hours"] == 0
    assert week["total_cost"] == Decimal("1440.00")
    roster = Roster(id="r", venue_id="v", week_start=monday, week_end=monday + timedelta(days=6),
                    shifts=shifts, created_at=datetime(2026, 1, 1))
    assert calculate_roster_cost(roster, {dana.id: dana}, State.wa) == Decimal("1440.00")

    # The full-cost projection prices the same day of salary, no penalties
    from rosteriq.services.employee_costing import EmployeeCostingService
    base, penalty, loading, *_ = EmployeeCostingService._calculate_shift_costs(
        dana, _shift("s", dana.id, sunday, dtime(16), dtime(23, 30), "Duty Manager"))
    assert (base, penalty, loading) == (Decimal("214.29"), Decimal("0.00"), Decimal("0.00"))

    # Without a recorded salary the person is costed hourly
    hourly = _salaried(pay_basis="hourly", weekly_salary=None)
    assert calculate_shift_cost_breakdown(
        hourly, _shift("s", hourly.id, sunday, dtime(16), dtime(23, 30), "Duty Manager"),
        State.wa).penalty_cost > 0


def test_snapshot_and_briefing_charge_salary_per_rostered_day():
    c, mgr, vid, ids, staff, _ = _world(extra={"jo": {"pay_basis": "salaried",
                                                      "weekly_salary": "1500.00"}})
    db = get_db()
    today = venue_today(vid, db)
    _publish(vid, _monday(today), [
        # Stale hourly prices cached on the shifts: salary wins, once per day
        _shift("jo-am", ids["jo"], today, dtime(9), dtime(13), "Duty Manager", cost=Decimal("999")),
        _shift("jo-pm", ids["jo"], today, dtime(17), dtime(23), "Duty Manager", cost=Decimal("999")),
    ])
    snap = c.get(f"/api/snapshot?venue_id={vid}&start_date={today}&end_date={today}",
                 headers=mgr).json()
    assert snap["labour"] == 240.0 and snap["days"][0]["labour"] == 240.0
    from rosteriq.routes.snapshot import _labour_by_day
    assert _labour_by_day(db, vid, today, today) == {today.isoformat(): 240.0}
    brief = c.get(f"/api/briefing?venue_id={vid}", headers=mgr).json()
    assert brief["today"]["labour_cost"] == 240.0


def test_payroll_prep_shows_salaried_people_as_a_fixed_line():
    c, mgr, vid, ids, staff, _ = _world(extra={
        "jo": {"pay_basis": "salaried", "weekly_salary": "1500.00"},
        "meca": {"pay_basis": "salaried", "weekly_salary": "1400.00"}})
    _approved_punch(c, mgr, vid, ids["jo"], start=dtime(6), end=dtime(23, 30), brk=30)
    today = date.today()
    batch = _prepare_actuals(c, mgr, vid, today, today)
    lines = {e["employee_id"]: e for e in batch["employees"]}
    jo = lines[ids["jo"]]
    assert jo["pay_basis"] == "salaried"
    assert jo["salary_amount"] == "214.29" and jo["worked_hours"] == "17.00"
    assert jo["penalty_entries"] == [] and jo["overtime_amount"] == "0.00"
    assert jo["ordinary_gross"] == "0.00" and jo["total_gross"] == "214.29"
    # Salaried with no punches still gets their line
    assert lines[ids["meca"]]["salary_amount"] == "200.00"
    assert batch["total_salary"] == "414.29"

    text = c.get(f"/api/payroll/batch/{batch['batch_id']}/csv?venue_id={vid}", headers=mgr).text
    assert "Salary (fixed line)" in text
    jo_row = [line for line in text.splitlines() if line.startswith("Jo Taylor")][0]
    assert "salaried,214.29,17.00" in jo_row


def test_staff_import_salary_rows_become_salaried():
    c = TestClient(app)
    _login(c, f"b7_boot_{uuid.uuid4().hex[:6]}@x.com")
    email = f"b7i{uuid.uuid4().hex[:8]}@x.com"
    h = _login(c, email)
    vid = f"b7i-{uuid.uuid4().hex[:6]}"
    _venue(c, h, vid, "The Wembley Hotel")
    h = _login(c, email)
    content = ("Name,Email,Pay Rate\n"
               "Dean Murray,dean@wembley-synthetic.com.au,\"$78,000 salary\"\n"
               "Rhys Rest,rhys@wembley-synthetic.com.au,\"$1,140.00 weekly\"\n"
               "Casey Casual,casey@wembley-synthetic.com.au,$32.60/hr\n")
    out = c.post("/api/setup/import-staff", json={"venue_id": vid, "content": content},
                 headers=h).json()
    emps = {e.name: e for e in get_db().get_employees(vid)}
    dean, rhys, casey = emps["Dean Murray"], emps["Rhys Rest"], emps["Casey Casual"]
    assert (dean.pay_basis, dean.weekly_salary, dean.hourly_base_rate) == \
        ("salaried", Decimal("1500.00"), Decimal("39.47"))
    assert (rhys.pay_basis, rhys.weekly_salary, rhys.hourly_base_rate) == \
        ("salaried", Decimal("1140.00"), Decimal("30.00"))
    assert casey.pay_basis == "hourly" and casey.weekly_salary is None
    rows = {r["name"]: r for r in out["rows"]}
    assert "salaried" in rows["Dean Murray"]["flags"]
    assert "rate_converted" not in rows["Dean Murray"]["flags"]
    assert rows["Dean Murray"]["weekly_salary"] == "1500.00"
    assert out["flags_summary"]["salaried"] == 2

    # Refreshing an hourly record from a salary export makes it salaried
    again = ("Name,Email,Pay Rate\n"
             "Casey Casual,casey@wembley-synthetic.com.au,\"$62,400 per annum\"\n")
    out = c.post("/api/setup/import-staff", json={"venue_id": vid, "content": again,
                                                  "update_existing": True}, headers=h).json()
    casey = {e.name: e for e in get_db().get_employees(vid)}["Casey Casual"]
    assert (casey.pay_basis, casey.weekly_salary) == ("salaried", Decimal("1200.00"))
    assert "salaried" in out["rows"][0]["flags"]


def test_pay_basis_is_in_the_api_for_managers_and_hidden_from_colleagues():
    c, mgr, vid, ids, staff, _ = _world(extra={"jo": {"pay_basis": "salaried",
                                                      "weekly_salary": "1500.00"}})
    got = c.get(f"/employees/{ids['jo']}", headers=mgr).json()
    assert got["pay_basis"] == "salaried" and Decimal(str(got["weekly_salary"])) == Decimal("1500.00")
    colleague = c.get(f"/employees/{ids['jo']}", headers=staff["sam"]).json()
    assert "weekly_salary" not in colleague and "pay_basis" not in colleague
    listed = c.get(f"/employees?venue_id={vid}", headers=staff["sam"]).json()["items"]
    assert all("weekly_salary" not in e for e in listed if e["id"] != ids["sam"])
    bad = c.post("/employees", json={
        "id": f"{vid}-x", "venue_id": vid, "name": "X", "employment_type": "casual",
        "award_level": "level_2", "state": "wa", "hourly_base_rate": "30", "pay_basis": "monthly",
        "created_at": "2026-07-01T00:00:00", "updated_at": "2026-07-01T00:00:00"}, headers=mgr)
    assert bad.status_code == 422


# ---------------------------------------------------------------------------
# Postgres parity (recorded SQL; no live server)
# ---------------------------------------------------------------------------

class _Cur:
    def __init__(self, log, rows=None):
        self.log, self.rows, self.rowcount = log, rows or [], 1

    def execute(self, sql, params=None):
        self.log.append((" ".join(sql.split()), params))

    def fetchall(self):
        return list(self.rows)

    def fetchone(self):
        return self.rows[0] if self.rows else None


class _PG:
    _TABLE_DDL = PostgresStore._TABLE_DDL
    _ensure_table = PostgresStore._ensure_table
    _leave_row = getattr(PostgresStore, "_leave_row", None)
    _row_to_shift = PostgresStore._row_to_shift

    def __init__(self, rows=None):
        self.log, self.rows = [], rows

    @contextmanager
    def _cursor(self):
        yield _Cur(self.log, self.rows)


def _stmt(pg, prefix):
    return [(s, p) for s, p in pg.log if s.startswith(prefix)]


def test_pg_stores_open_shifts_leave_fields_and_cover_origin():
    pg = _PG()
    roster = Roster(id="r1", venue_id="v", week_start=date(2026, 10, 12),
                    week_end=date(2026, 10, 18), created_at=datetime(2026, 1, 1),
                    shifts=[_shift("s1", "", date(2026, 10, 17), dtime(16), dtime(23), "Duty Manager")])
    PostgresStore.save_roster(pg, roster)
    insert = _stmt(pg, "INSERT INTO shifts")[0]
    assert insert[1][2] is None            # "" -> NULL, not a dangling FK
    row = {"id": "s1", "employee_id": None, "shift_date": date(2026, 10, 17),
           "start_time": dtime(16), "end_time": dtime(23), "break_minutes": 0,
           "status": "scheduled", "role": "Duty Manager", "cost": None, "penalty_multiplier": 1}
    assert PostgresStore._row_to_shift(pg, row).employee_id == ""

    PostgresStore.save_leave_request(pg, {
        "id": "lv-1", "venue_id": "v", "employee_id": "e", "start_date": date(2026, 10, 17),
        "end_date": date(2026, 10, 17), "reason": "", "leave_type": "personal", "paid_hours": 7.0,
        "entered_by": "u1", "opened_shifts": [{"label": "Sat 4-11pm Duty Manager"}],
        "status": "approved"})
    sql, params = _stmt(pg, "INSERT INTO leave_requests")[0]
    for col in ("leave_type", "paid_hours", "entered_by", "opened_shifts"):
        assert col in sql.split("VALUES")[0], col
    on_conflict = sql[sql.index("DO UPDATE SET"):]
    for col in ("leave_type=EXCLUDED.leave_type", "paid_hours=EXCLUDED.paid_hours",
                "opened_shifts=EXCLUDED.opened_shifts"):
        assert col in on_conflict, col
    assert '"Sat 4-11pm Duty Manager"' in [p for p in params if isinstance(p, str) and "Sat" in p][0]

    back = PostgresStore._leave_row({"id": "lv-1", "paid_hours": Decimal("7.00"),
                                     "opened_shifts": '[{"label": "Sat 4-11pm Duty Manager"}]'})
    assert back["paid_hours"] == 7.0 and back["opened_shifts"][0]["label"] == "Sat 4-11pm Duty Manager"

    PostgresStore.save_shift_cover(pg, {
        "id": "cv-1", "venue_id": "v", "shift_id": "s1", "shift_date": date(2026, 10, 17),
        "role": "Duty Manager", "requested_by": "e", "origin": "leave", "status": "open"})
    sql, params = _stmt(pg, "INSERT INTO shift_covers")[0]
    assert "origin" in sql.split("VALUES")[0] and "leave" in params

    for ddl in (PostgresStore._TABLE_DDL["leave_requests"], PostgresStore._TABLE_DDL["shift_covers"]):
        assert "leave_type" in ddl or "origin" in ddl


def test_pg_schema_lets_an_open_shift_have_no_employee():
    import inspect
    src = inspect.getsource(PostgresStore._ensure_core_schema)
    assert "ALTER TABLE shifts ALTER COLUMN employee_id DROP NOT NULL" in src
    for col in ("pay_basis", "weekly_salary"):
        assert f"ALTER TABLE employees ADD COLUMN IF NOT EXISTS {col}" in src
    for col in ("leave_type", "paid_hours", "entered_by", "opened_shifts"):
        assert f"ALTER TABLE leave_requests ADD COLUMN IF NOT EXISTS {col}" in src
    assert "ALTER TABLE shift_covers ADD COLUMN IF NOT EXISTS origin" in src
    shifts_ddl = dict(PostgresStore._CORE_SCHEMA_DDL)["shifts"]
    assert "employee_id TEXT REFERENCES employees(id)" in shifts_ddl
    employees_ddl = dict(PostgresStore._CORE_SCHEMA_DDL)["employees"]
    assert "pay_basis TEXT" in employees_ddl and "weekly_salary NUMERIC" in employees_ddl


def test_pg_employee_row_reads_pay_basis():
    row = {"id": "e", "tanda_id": None, "venue_id": None, "name": "Dana",
           "employment_type": "full_time", "award_level": "level_6", "state": "wa",
           "hourly_base_rate": Decimal("38.00"), "phone": None, "email": None,
           "skills": [], "availability": {}, "max_hours_per_week": Decimal("38"),
           "consecutive_days": 6, "visa_status": None, "visa_expiry": None,
           "visa_work_limit_fortnight": None, "pay_basis": "salaried",
           "weekly_salary": Decimal("1500.00"),
           "created_at": datetime(2026, 1, 1), "updated_at": datetime(2026, 1, 1)}
    emp = PostgresStore._row_to_employee(_PG(), row)
    assert emp.pay_basis == "salaried" and emp.weekly_salary == Decimal("1500.00")
    legacy = {**row, "pay_basis": None, "weekly_salary": None}
    assert PostgresStore._row_to_employee(_PG(), legacy).pay_basis == "hourly"
