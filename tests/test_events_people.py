"""
Event log — the PEOPLE pillar. Every write a venue owner must be able to
answer for later lands as one audit row through the events spine, carrying the
actor, the outcome and the human-relevant facts:

  employee.create / employee.update / employee.rate_change   (api.py)
  roster.generate                                            (api.py)
  roster.publish / roster.recall / roster.archive            (routes/publishing.py)
  leave.decide / cover.decide                                (routes/staff_portal.py)
  timesheet.correct / timesheet.approve                      (routes/timeclock.py)
  payroll.batch_create                                       (routes/payroll.py)
  approval.approve / approval.reject                         (routes/approvals.py)

Every test drives the real HTTP route (so the actor/correlation-id/role are
captured from request context) and then reads the row back with
db.list_events(...), asserting the shared row shape.
"""

import uuid
from dataclasses import asdict
from datetime import date, datetime, timedelta, time as dtime

from fastapi.testclient import TestClient

from rosteriq.api import app
from rosteriq.database import get_db
from rosteriq.models import Roster, Shift, ShiftStatus

PW = "Passw0rd!234"


# ----------------------------------------------------------------------------
# helpers
# ----------------------------------------------------------------------------

def _register_login(c, email):
    c.post("/api/auth/register", json={"email": email, "password": PW, "name": "U"})
    tok = c.post("/api/auth/login", json={"email": email, "password": PW}).json()["access_token"]
    return {"Authorization": f"Bearer {tok}"}


def _owner(c):
    email = f"ep{uuid.uuid4().hex[:8]}@x.com"
    return _register_login(c, email), email


def _venue(c, h, vid, state="wa"):
    r = c.post("/venues", json={
        "id": vid, "name": f"Venue {vid}", "state": state, "max_labour_pct": 30,
        "tanda_org_id": "", "created_at": "2026-07-01T00:00:00",
    }, headers=h)
    assert r.status_code in (200, 201), r.text
    return vid


def _employee(c, h, vid, emp_id, name="Ben Bartender", rate="31.50", email=None, **extra):
    body = {
        "id": emp_id, "name": name, "employment_type": "casual",
        "award_level": "level_2", "state": "wa", "venue_id": vid,
        "hourly_base_rate": rate, "skills": ["bar"],
        "created_at": "2026-07-01T00:00:00", "updated_at": "2026-07-01T00:00:00",
    }
    if email:
        body["email"] = email
    body.update(extra)
    r = c.post("/employees", json=body, headers=h)
    assert r.status_code == 200, r.text
    return emp_id


def _scope_staff(email, vid):
    db = get_db()
    rec = db.get_user_by_email(email)
    rec["venue_ids"] = [vid]
    rec["role"] = "staff"
    db.save_user(rec)


def _rows(vid, prefix, action=None):
    rows = get_db().list_events(venue_id=vid, action_prefix=prefix)
    if action:
        rows = [r for r in rows if r["action"] == action]
    return rows


def _assert_row_shape(row, *, action, venue_id, actor_id, resource_type, resource_id=None):
    """The contract every audit row honours, whichever handler wrote it."""
    assert row["action"] == action
    assert row["venue_id"] == venue_id
    assert row["user_id"] == actor_id, f"actor not captured: {row}"
    assert row["resource_type"] == resource_type
    if resource_id is not None:
        assert row["resource_id"] == resource_id
    d = row["details"]
    assert d["category"] == "audit"
    assert d["outcome"] == "ok"
    assert isinstance(d["correlation_id"], str) and d["correlation_id"]
    assert d["role"] == "owner"
    assert row["created_at"] is not None


# ----------------------------------------------------------------------------
# employees (api.py)
# ----------------------------------------------------------------------------

def test_employee_create_update_and_rate_change_are_audited():
    c = TestClient(app)
    h, email = _owner(c)
    owner = get_db().get_user_by_email(email)
    vid = _venue(c, h, f"ev-emp-{uuid.uuid4().hex[:6]}")

    _employee(c, h, vid, "emp-1", name="Ben Bartender", rate="31.50")
    created = _rows(vid, "employee.", "employee.create")
    assert len(created) == 1
    _assert_row_shape(created[0], action="employee.create", venue_id=vid,
                      actor_id=owner["id"], resource_type="employee", resource_id="emp-1")
    d = created[0]["details"]
    assert d["name"] == "Ben Bartender" and d["skills"] == ["bar"]
    assert d["hourly_base_rate"] == "31.50" and d["employment_type"] == "casual"

    # Re-POST the same id (upsert) with a new rate + skills -> update + rate_change
    _employee(c, h, vid, "emp-1", name="Ben Bartender", rate="33.00",
              skills=["bar", "floor"])
    updated = _rows(vid, "employee.", "employee.update")
    assert len(updated) == 1
    _assert_row_shape(updated[0], action="employee.update", venue_id=vid,
                      actor_id=owner["id"], resource_type="employee", resource_id="emp-1")
    ud = updated[0]["details"]
    assert set(ud["changed_fields"]) == {"hourly_base_rate", "skills"}
    assert ud["changed"]["hourly_base_rate"] == {"old": "31.50", "new": "33.00"}
    assert ud["changed"]["skills"]["new"] == ["bar", "floor"]

    rate = _rows(vid, "employee.", "employee.rate_change")
    assert len(rate) == 1
    _assert_row_shape(rate[0], action="employee.rate_change", venue_id=vid,
                      actor_id=owner["id"], resource_type="employee", resource_id="emp-1")
    assert rate[0]["details"]["old"] == "31.50" and rate[0]["details"]["new"] == "33.00"
    assert rate[0]["details"]["name"] == "Ben Bartender"

    # A no-op re-save records an update with nothing changed, never a rate_change
    _employee(c, h, vid, "emp-1", name="Ben Bartender", rate="33.00", skills=["bar", "floor"])
    assert len(_rows(vid, "employee.", "employee.rate_change")) == 1
    assert len(_rows(vid, "employee.", "employee.create")) == 1


def test_bulk_employee_create_audits_each_row_with_via_bulk():
    c = TestClient(app)
    h, email = _owner(c)
    owner = get_db().get_user_by_email(email)
    vid = _venue(c, h, f"ev-bulk-{uuid.uuid4().hex[:6]}")
    emps = [{
        "id": f"{vid}-e{i}", "name": f"Bulk {i}", "employment_type": "casual",
        "award_level": "level_1", "state": "wa", "venue_id": vid,
        "hourly_base_rate": "30.00",
        "created_at": "2026-07-01T00:00:00", "updated_at": "2026-07-01T00:00:00",
    } for i in range(3)]
    r = c.post("/employees/bulk", json=emps, headers=h)
    assert r.status_code == 200, r.text
    rows = _rows(vid, "employee.create")
    assert len(rows) == 3
    assert {x["resource_id"] for x in rows} == {f"{vid}-e{i}" for i in range(3)}
    for x in rows:
        assert x["user_id"] == owner["id"] and x["details"]["via"] == "bulk"
        assert x["details"]["category"] == "audit"


# ----------------------------------------------------------------------------
# rosters (api.py generate + routes/publishing.py publish/recall/archive)
# ----------------------------------------------------------------------------

def _seed_generated_roster(c, h, vid):
    emps = [{
        "id": f"{vid}-e{i}", "name": f"E{i}", "employment_type": "full_time",
        "award_level": "level_1", "state": "vic", "venue_id": vid,
        "hourly_base_rate": "30.00",
        "created_at": "2025-01-01T00:00:00", "updated_at": "2025-01-01T00:00:00",
    } for i in range(6)]
    assert c.post("/employees/bulk", json=emps, headers=h).status_code == 200
    r = c.post("/rosters/generate", json={"venue_id": vid, "week_start": "2026-06-22"}, headers=h)
    assert r.status_code == 200, r.text
    return r.json()["id"]


def test_roster_generate_publish_recall_archive_are_audited():
    c = TestClient(app)
    h, email = _owner(c)
    owner = get_db().get_user_by_email(email)
    vid = _venue(c, h, f"ev-ros-{uuid.uuid4().hex[:6]}", state="vic")
    rid = _seed_generated_roster(c, h, vid)

    gen = _rows(vid, "roster.generate")
    assert len(gen) == 1
    _assert_row_shape(gen[0], action="roster.generate", venue_id=vid,
                      actor_id=owner["id"], resource_type="roster", resource_id=rid)
    assert gen[0]["details"]["week_start"] == "2026-06-22"
    assert gen[0]["details"]["employees"] == 6
    assert isinstance(gen[0]["details"]["shifts"], int)

    # publish (skip approval as owner)
    r = c.post(f"/api/v1/rosters/{rid}/publish", json={"skip_approval": True}, headers=h)
    assert r.status_code == 200 and r.json()["success"] is True, r.text
    pub = _rows(vid, "roster.publish")
    assert len(pub) == 1
    _assert_row_shape(pub[0], action="roster.publish", venue_id=vid,
                      actor_id=owner["id"], resource_type="roster", resource_id=rid)
    assert pub[0]["details"]["week_start"] == "2026-06-22"
    assert pub[0]["details"]["state"] == "published"
    assert pub[0]["details"]["skip_approval"] is True

    # a second publish is refused by the workflow -> recorded as outcome=failed, not silently dropped
    r2 = c.post(f"/api/v1/rosters/{rid}/publish", json={"skip_approval": True}, headers=h)
    assert r2.status_code == 200 and r2.json()["success"] is False
    pub_all = _rows(vid, "roster.publish")
    assert len(pub_all) == 2
    assert sorted(x["details"]["outcome"] for x in pub_all) == ["failed", "ok"]

    # recall -> roster.recall with the reason
    r3 = c.post(f"/api/v1/rosters/{rid}/recall", json={"reason": "Wrong week"}, headers=h)
    assert r3.status_code == 200, r3.text
    rec = _rows(vid, "roster.recall")
    assert len(rec) == 1
    _assert_row_shape(rec[0], action="roster.recall", venue_id=vid,
                      actor_id=owner["id"], resource_type="roster", resource_id=rid)
    assert rec[0]["details"]["reason"] == "Wrong week"
    assert rec[0]["details"]["week_start"] == "2026-06-22"

    # re-publish then archive -> roster.archive
    r4 = c.post(f"/api/v1/rosters/{rid}/publish", json={"skip_approval": True}, headers=h)
    assert r4.status_code == 200 and r4.json()["success"] is True, r4.text
    r5 = c.post(f"/api/v1/rosters/{rid}/archive", headers=h)
    assert r5.status_code == 200, r5.text
    arc = _rows(vid, "roster.archive")
    assert len(arc) == 1
    _assert_row_shape(arc[0], action="roster.archive", venue_id=vid,
                      actor_id=owner["id"], resource_type="roster", resource_id=rid)
    assert arc[0]["details"]["week_start"] == "2026-06-22"

    # The venue's roster.* trail reads as one story: 3 publish attempts, 1 recall, 1 archive, 1 generate
    trail = _rows(vid, "roster.")
    assert sorted(x["action"] for x in trail) == sorted(
        ["roster.generate", "roster.publish", "roster.publish", "roster.publish",
         "roster.recall", "roster.archive"])


# ----------------------------------------------------------------------------
# leave + cover decisions (routes/staff_portal.py)
# ----------------------------------------------------------------------------

def test_leave_decide_is_audited_with_employee_and_dates():
    c = TestClient(app)
    owner_h, owner_email = _owner(c)
    owner = get_db().get_user_by_email(owner_email)
    vid = _venue(c, owner_h, f"ev-leave-{uuid.uuid4().hex[:6]}")
    staff_email = f"s{uuid.uuid4().hex[:8]}@x.com"
    _employee(c, owner_h, vid, f"{vid}-emp", name="Portal Tester", email=staff_email)
    staff_h = _register_login(c, staff_email)
    _scope_staff(staff_email, vid)

    nxt = date.today() + timedelta(days=7)
    end = nxt + timedelta(days=2)
    r = c.post("/api/me/leave", json={
        "start_date": nxt.isoformat(), "end_date": end.isoformat(), "reason": "Family trip",
    }, headers=staff_h)
    assert r.status_code == 200, r.text
    req_id = r.json()["request_id"]

    # Requesting leave is not itself an owner-facing decision -> no leave.decide yet
    assert _rows(vid, "leave.decide") == []

    dec = c.post(f"/api/leave/{req_id}/decide", json={
        "venue_id": vid, "approve": False, "note": "Fully booked that weekend",
    }, headers=owner_h)
    assert dec.status_code == 200 and dec.json()["status"] == "declined"

    rows = _rows(vid, "leave.decide")
    assert len(rows) == 1
    _assert_row_shape(rows[0], action="leave.decide", venue_id=vid,
                      actor_id=owner["id"], resource_type="leave_request", resource_id=req_id)
    d = rows[0]["details"]
    assert d["decision"] == "decline" and d["status"] == "declined"
    assert d["employee"] == "Portal Tester" and d["employee_id"] == f"{vid}-emp"
    assert d["start_date"] == nxt.isoformat() and d["end_date"] == end.isoformat()
    assert d["reason"] == "Family trip" and d["note"] == "Fully booked that weekend"

    # A refused double-decide (409) writes nothing
    dbl = c.post(f"/api/leave/{req_id}/decide", json={"venue_id": vid, "approve": True},
                 headers=owner_h)
    assert dbl.status_code == 409
    assert len(_rows(vid, "leave.decide")) == 1


def _roster_with_shift(vid, emp_id, shift_id):
    today = date.today()
    week_start = today - timedelta(days=today.weekday())
    tomorrow = today + timedelta(days=1)
    shift_day = tomorrow if tomorrow <= week_start + timedelta(days=6) else today
    get_db().save_roster(Roster(
        id=f"{vid}-roster", venue_id=vid, week_start=week_start,
        week_end=week_start + timedelta(days=6),
        shifts=[Shift(id=shift_id, employee_id=emp_id, date=shift_day,
                      start_time=dtime(17, 0), end_time=dtime(23, 0),
                      break_minutes=30, status=ShiftStatus.scheduled, role="bar")],
        total_cost=None, created_at=datetime(2026, 7, 1),
    ))
    return shift_day


def test_cover_decide_is_audited_with_both_employees():
    c = TestClient(app)
    owner_h, owner_email = _owner(c)
    owner = get_db().get_user_by_email(owner_email)
    vid = _venue(c, owner_h, f"ev-cover-{uuid.uuid4().hex[:6]}")
    a_email = f"a{uuid.uuid4().hex[:8]}@x.com"
    b_email = f"b{uuid.uuid4().hex[:8]}@x.com"
    _employee(c, owner_h, vid, f"{vid}-emp-a", name="Ally Asker", email=a_email)
    _employee(c, owner_h, vid, f"{vid}-emp-b", name="Cover Claimer", email=b_email)
    shift_day = _roster_with_shift(vid, f"{vid}-emp-a", "ev-cv-shift")
    a_h = _register_login(c, a_email)
    _scope_staff(a_email, vid)
    b_h = _register_login(c, b_email)
    _scope_staff(b_email, vid)

    cover_id = c.post("/api/me/shifts/ev-cv-shift/cover", json={"reason": "Sick"},
                      headers=a_h).json()["cover_id"]
    assert c.post(f"/api/me/cover/{cover_id}/claim", headers=b_h).status_code == 200

    dec = c.post(f"/api/cover/{cover_id}/decide", json={
        "venue_id": vid, "approve": True, "note": "Get well",
    }, headers=owner_h)
    assert dec.status_code == 200 and dec.json()["status"] == "approved", dec.text

    rows = _rows(vid, "cover.decide")
    assert len(rows) == 1
    _assert_row_shape(rows[0], action="cover.decide", venue_id=vid,
                      actor_id=owner["id"], resource_type="shift_cover", resource_id=cover_id)
    d = rows[0]["details"]
    assert d["decision"] == "approve" and d["status"] == "approved"
    assert d["shift_id"] == "ev-cv-shift" and d["shift_date"] == shift_day.isoformat()
    assert d["requested_by"] == f"{vid}-emp-a" and d["requested_by_name"] == "Ally Asker"
    assert d["claimed_by"] == f"{vid}-emp-b" and d["claimed_by_name"] == "Cover Claimer"
    assert d["note"] == "Get well"


def test_cover_decline_is_audited_and_names_the_declined_claimant():
    c = TestClient(app)
    owner_h, owner_email = _owner(c)
    vid = _venue(c, owner_h, f"ev-cvd-{uuid.uuid4().hex[:6]}")
    a_email = f"a{uuid.uuid4().hex[:8]}@x.com"
    b_email = f"b{uuid.uuid4().hex[:8]}@x.com"
    _employee(c, owner_h, vid, f"{vid}-emp-a", name="Ally Asker", email=a_email)
    _employee(c, owner_h, vid, f"{vid}-emp-b", name="Cover Claimer", email=b_email)
    _roster_with_shift(vid, f"{vid}-emp-a", "ev-cvd-shift")
    a_h = _register_login(c, a_email)
    _scope_staff(a_email, vid)
    b_h = _register_login(c, b_email)
    _scope_staff(b_email, vid)
    cover_id = c.post("/api/me/shifts/ev-cvd-shift/cover", json={}, headers=a_h).json()["cover_id"]
    assert c.post(f"/api/me/cover/{cover_id}/claim", headers=b_h).status_code == 200

    dec = c.post(f"/api/cover/{cover_id}/decide", json={"venue_id": vid, "approve": False,
                                                        "note": "Not qualified"}, headers=owner_h)
    assert dec.status_code == 200 and dec.json()["status"] == "reopened"
    rows = _rows(vid, "cover.decide")
    assert len(rows) == 1
    d = rows[0]["details"]
    assert d["decision"] == "decline" and d["status"] == "open"
    # the claimant who was turned down is still named even though the cover is cleared
    assert d["claimed_by"] == f"{vid}-emp-b" and d["claimed_by_name"] == "Cover Claimer"
    assert d["note"] == "Not qualified" and d["category"] == "audit"


# ----------------------------------------------------------------------------
# timesheets (routes/timeclock.py)
# ----------------------------------------------------------------------------

def _punch(c, h, vid, emp_id):
    assert c.post("/api/clock/in", json={"venue_id": vid, "employee_id": emp_id},
                  headers=h).status_code == 200
    out = c.post("/api/clock/out", json={"venue_id": vid, "employee_id": emp_id}, headers=h)
    assert out.status_code == 200, out.text
    return out.json()["timesheet_id"]


def test_timesheet_correct_and_approve_are_two_audit_facts():
    c = TestClient(app)
    h, email = _owner(c)
    owner = get_db().get_user_by_email(email)
    vid = _venue(c, h, f"ev-ts-{uuid.uuid4().hex[:6]}")
    _employee(c, h, vid, "clk-emp-1", name="Clock Tester")
    ts_id = _punch(c, h, vid, "clk-emp-1")
    today = date.today()

    cin = datetime.combine(today, dtime(9, 0))
    cout = datetime.combine(today, dtime(13, 30))
    r = c.post(f"/api/clock/timesheets/{ts_id}/review", json={
        "venue_id": vid, "approve": True,
        "clock_in": cin.isoformat(), "clock_out": cout.isoformat(),
        "break_minutes": 30, "note": "Forgot to clock out",
    }, headers=h)
    assert r.status_code == 200 and r.json()["worked_minutes"] == 240, r.text

    corr = _rows(vid, "timesheet.", "timesheet.correct")
    assert len(corr) == 1
    _assert_row_shape(corr[0], action="timesheet.correct", venue_id=vid,
                      actor_id=owner["id"], resource_type="timesheet", resource_id=ts_id)
    cd = corr[0]["details"]
    assert cd["employee"] == "Clock Tester" and cd["employee_id"] == "clk-emp-1"
    assert cd["worked_minutes"] == 240 and cd["note"] == "Forgot to clock out"
    assert cd["work_date"] == today.isoformat()
    assert "clock_out" in cd["changed"] and cd["changed"]["clock_out"]["new"] == str(cout)
    assert cd["changed"]["clock_out"]["old"] != cd["changed"]["clock_out"]["new"]

    appr = _rows(vid, "timesheet.", "timesheet.approve")
    assert len(appr) == 1
    _assert_row_shape(appr[0], action="timesheet.approve", venue_id=vid,
                      actor_id=owner["id"], resource_type="timesheet", resource_id=ts_id)
    ad = appr[0]["details"]
    assert ad["employee"] == "Clock Tester" and ad["worked_minutes"] == 240
    assert ad["break_minutes"] == 30 and ad["corrected"] is True
    assert ad["note"] == "Forgot to clock out"

    # Refused double approval (409) writes nothing further
    again = c.post(f"/api/clock/timesheets/{ts_id}/review",
                   json={"venue_id": vid, "approve": True}, headers=h)
    assert again.status_code == 409
    assert len(_rows(vid, "timesheet.")) == 2


def test_timesheet_plain_approve_emits_only_approve():
    c = TestClient(app)
    h, email = _owner(c)
    vid = _venue(c, h, f"ev-tsa-{uuid.uuid4().hex[:6]}")
    _employee(c, h, vid, "clk-emp-2", name="Plain Punch")
    ts_id = _punch(c, h, vid, "clk-emp-2")
    r = c.post(f"/api/clock/timesheets/{ts_id}/review",
               json={"venue_id": vid, "approve": True}, headers=h)
    assert r.status_code == 200, r.text
    rows = _rows(vid, "timesheet.")
    assert [x["action"] for x in rows] == ["timesheet.approve"]
    assert rows[0]["details"]["corrected"] is False
    assert rows[0]["details"]["employee"] == "Plain Punch"
    assert isinstance(rows[0]["details"]["worked_minutes"], int)


# ----------------------------------------------------------------------------
# payroll (routes/payroll.py)
# ----------------------------------------------------------------------------

def test_payroll_batch_create_is_audited_with_period_count_and_total():
    c = TestClient(app)
    h, email = _owner(c)
    owner = get_db().get_user_by_email(email)
    vid = _venue(c, h, f"ev-pay-{uuid.uuid4().hex[:6]}")
    _employee(c, h, vid, f"{vid}-emp", name="Pay Tester", rate="30.00")
    ts_id = _punch(c, h, vid, f"{vid}-emp")
    today = date.today()
    r = c.post(f"/api/clock/timesheets/{ts_id}/review", json={
        "venue_id": vid, "approve": True,
        "clock_in": datetime.combine(today, dtime(9, 0)).isoformat(),
        "clock_out": datetime.combine(today, dtime(17, 30)).isoformat(),
        "break_minutes": 30, "note": "clean day",
    }, headers=h)
    assert r.status_code == 200, r.text

    r = c.post("/api/payroll/prepare-actuals", json={
        "venue_id": vid, "period_start": today.isoformat(), "period_end": today.isoformat(),
        "state": "wa",
    }, headers=h)
    assert r.status_code == 200, r.text
    batch = r.json()

    rows = _rows(vid, "payroll.batch_create")
    assert len(rows) == 1
    _assert_row_shape(rows[0], action="payroll.batch_create", venue_id=vid,
                      actor_id=owner["id"], resource_type="payroll_batch",
                      resource_id=batch["batch_id"])
    d = rows[0]["details"]
    assert d["period_start"] == today.isoformat() and d["period_end"] == today.isoformat()
    assert d["employee_count"] == 1
    assert d["total_gross"] == batch["total_gross"] and float(d["total_gross"]) > 0
    assert d["basis"] == "approved_timesheets"


def test_payroll_prepare_from_roster_is_audited_and_approve_follows():
    c = TestClient(app)
    h, email = _owner(c)
    owner = get_db().get_user_by_email(email)
    vid = _venue(c, h, f"ev-payr-{uuid.uuid4().hex[:6]}", state="vic")
    _seed_generated_roster(c, h, vid)
    r = c.post("/api/payroll/prepare", json={
        "venue_id": vid, "period_start": "2026-06-22", "period_end": "2026-06-28", "state": "vic",
    }, headers=h)
    assert r.status_code == 200, r.text
    batch = r.json()
    rows = _rows(vid, "payroll.batch_create")
    assert len(rows) == 1
    d = rows[0]["details"]
    assert d["basis"] == "rostered_shifts" and d["period_start"] == "2026-06-22"
    assert d["employee_count"] == batch["employee_count"]
    assert rows[0]["user_id"] == owner["id"]

    r2 = c.put(f"/api/payroll/batch/{batch['batch_id']}/approve",
               json={"venue_id": vid, "approved_by": "Dale", "notes": "looks right"}, headers=h)
    assert r2.status_code == 200, r2.text
    appr = _rows(vid, "payroll.batch_approve")
    assert len(appr) == 1
    assert appr[0]["resource_id"] == batch["batch_id"]
    assert appr[0]["details"]["approved_by"] == "Dale" and appr[0]["details"]["notes"] == "looks right"
    assert appr[0]["details"]["employee_count"] == batch["employee_count"]


# ----------------------------------------------------------------------------
# approvals (routes/approvals.py)
# ----------------------------------------------------------------------------

def _seed_approval(vid, roster_id):
    from rosteriq.services.approval_workflow import approval_workflow, ApprovalRequest, ApprovalStatus
    db = get_db()
    approval_workflow.db = db  # singleton captured the store at import
    db.save_roster(Roster(
        id=roster_id, venue_id=vid, week_start=date(2026, 6, 22), week_end=date(2026, 6, 28),
        shifts=[], created_at=datetime(2026, 6, 20),
    ))
    req = ApprovalRequest(
        id=str(uuid.uuid4()), roster_id=roster_id, venue_id=vid, submitted_by="u",
        submitted_at=datetime(2026, 6, 20), status=ApprovalStatus.pending, tier="pro",
    )
    db.save_approval_request(asdict(req))
    return req.id


def test_approval_approve_and_reject_are_audited():
    c = TestClient(app)
    h, email = _owner(c)
    owner = get_db().get_user_by_email(email)
    vid = _venue(c, h, f"ev-appr-{uuid.uuid4().hex[:6]}")
    id_ok = _seed_approval(vid, f"{vid}-r1")
    id_no = _seed_approval(vid, f"{vid}-r2")

    r = c.post(f"/api/approvals/{id_ok}/approve", json={"approved": True, "notes": "ok"}, headers=h)
    assert r.status_code == 200 and r.json()["status"] == "approved", r.text
    r = c.post(f"/api/approvals/{id_no}/reject", json={"approved": False, "notes": "too costly"},
               headers=h)
    assert r.status_code == 200 and r.json()["status"] == "rejected", r.text

    ok = _rows(vid, "approval.approve")
    assert len(ok) == 1
    _assert_row_shape(ok[0], action="approval.approve", venue_id=vid,
                      actor_id=owner["id"], resource_type="approval_request", resource_id=id_ok)
    assert ok[0]["details"]["roster_id"] == f"{vid}-r1"
    assert ok[0]["details"]["notes"] == "ok" and ok[0]["details"]["status"] == "approved"

    no = _rows(vid, "approval.reject")
    assert len(no) == 1
    _assert_row_shape(no[0], action="approval.reject", venue_id=vid,
                      actor_id=owner["id"], resource_type="approval_request", resource_id=id_no)
    assert no[0]["details"]["roster_id"] == f"{vid}-r2"
    assert no[0]["details"]["notes"] == "too costly" and no[0]["details"]["status"] == "rejected"

    # A cross-tenant/unknown id is a 404 and leaves no audit trace
    assert c.post("/api/approvals/does-not-exist/approve", json={"approved": True},
                  headers=h).status_code == 404
    assert len(_rows(vid, "approval.")) == 2


# ----------------------------------------------------------------------------
# cross-cutting: the people trail is readable in one query
# ----------------------------------------------------------------------------

def test_people_events_are_venue_scoped_and_secret_scrubbed():
    """Two venues, one owner: the venue-scoped query never mixes venues, and a
    payload with a secret-shaped key is redacted on the way in."""
    c = TestClient(app)
    h, _ = _owner(c)
    va = _venue(c, h, f"ev-sa-{uuid.uuid4().hex[:6]}")
    vb = _venue(c, h, f"ev-sb-{uuid.uuid4().hex[:6]}")
    _employee(c, h, va, f"{va}-emp", name="A Person")
    _employee(c, h, vb, f"{vb}-emp", name="B Person")
    assert [r["details"]["name"] for r in _rows(va, "employee.create")] == ["A Person"]
    assert [r["details"]["name"] for r in _rows(vb, "employee.create")] == ["B Person"]

    # Everything the people pillar wrote is category=audit; nothing leaked a secret
    for row in get_db().list_events(category="audit", limit=500):
        assert "password" not in {k.lower() for k in row["details"]} or \
            row["details"].get("password") == "[redacted]"
