"""
Regressions from the 2026-08-02 full production sweep. Each test encodes a
CRITICAL or WARN finding so it can never quietly return:

  C1  /rosters/generate scheduled OTHER venues' employees (cross-tenant leak)
  C2  auto-schedule 500'd on three stacked API-drift bugs
  C3  staff self-registration was a dead end (linked:false forever)
  C4  /api/payroll/history had no venue scoping
  W4  roster generator ignored approved leave
  W5  AI chat leaked <think> reasoning
  W6  single-day payroll periods were flagged invalid
"""

import uuid
from datetime import date, timedelta

from fastapi.testclient import TestClient

from rosteriq.api import app
from rosteriq.database import get_db


def _owner(c):
    email = f"sr{uuid.uuid4().hex[:8]}@x.com"
    c.post("/api/auth/register", json={"email": email, "password": "Passw0rd!234", "name": "O"})
    tok = c.post("/api/auth/login", json={"email": email, "password": "Passw0rd!234"}).json()["access_token"]
    return {"Authorization": f"Bearer {tok}"}, email


def _venue(c, h, vid):
    c.post("/venues", json={
        "id": vid, "name": "SR Venue", "state": "wa", "max_labour_pct": 30,
        "tanda_org_id": "", "created_at": "2026-07-01T00:00:00",
    }, headers=h)


def _employee(c, h, vid, suffix, email=None):
    body = {
        "id": f"{vid}-{suffix}", "name": f"Emp {suffix}", "employment_type": "casual",
        "award_level": "level_2", "state": "wa", "venue_id": vid,
        "hourly_base_rate": "31.50", "skills": ["bar"],
        "created_at": "2026-07-01T00:00:00", "updated_at": "2026-07-01T00:00:00",
    }
    if email:
        body["email"] = email
    c.post("/employees", json=body, headers=h)
    return f"{vid}-{suffix}"


def _next_monday():
    today = date.today()
    return today - timedelta(days=today.weekday()) + timedelta(days=7)


def test_c1_roster_generation_never_uses_foreign_employees():
    """Venue B's staff must NEVER appear in venue A's generated roster."""
    c = TestClient(app)
    h, _ = _owner(c)
    _venue(c, h, "sr-gen-a")
    _venue(c, h, "sr-gen-b")
    a_emp = _employee(c, h, "sr-gen-a", "e1")
    foreign = {_employee(c, h, "sr-gen-b", f"f{i}") for i in range(3)}

    monday = _next_monday()
    r = c.post("/rosters/generate", json={
        "venue_id": "sr-gen-a", "week_start": monday.isoformat(),
    }, headers=h)
    assert r.status_code == 200, r.text
    used = {s["employee_id"] for s in r.json()["shifts"]}
    assert used and used <= {a_emp}, f"foreign employees rostered: {used & foreign}"


def test_w4_approved_leave_excluded_from_generation():
    c = TestClient(app)
    h, _ = _owner(c)
    vid = "sr-leave-gen"
    _venue(c, h, vid)
    staff_email = f"sr{uuid.uuid4().hex[:8]}@x.com"
    emp = _employee(c, h, vid, "e1", email=staff_email)
    _employee(c, h, vid, "e2")

    # Approve leave for e1 over the whole generated week
    staff_h, _ = _owner(c)  # separate account
    db = get_db()
    rec = db.get_user_by_email
    monday = _next_monday()
    sh = TestClient(app)
    # request as the staff member (register with the employee email)
    s = TestClient(app)
    s.post("/api/auth/register", json={"email": staff_email, "password": "Passw0rd!234", "name": "S"})
    stok = s.post("/api/auth/login", json={"email": staff_email, "password": "Passw0rd!234"}).json()["access_token"]
    s_h = {"Authorization": f"Bearer {stok}"}
    lr = s.post("/api/me/leave", json={
        "start_date": monday.isoformat(),
        "end_date": (monday + timedelta(days=6)).isoformat(),
        "reason": "Holiday",
    }, headers=s_h)
    assert lr.status_code == 200, lr.text
    dec = c.post(f"/api/leave/{lr.json()['request_id']}/decide", json={
        "venue_id": vid, "approve": True}, headers=h)
    assert dec.status_code == 200

    r = c.post("/rosters/generate", json={
        "venue_id": vid, "week_start": monday.isoformat()}, headers=h)
    assert r.status_code == 200, r.text
    used = {s_["employee_id"] for s_ in r.json()["shifts"]}
    assert emp not in used, "employee rostered onto approved leave"


def test_c3_staff_self_registration_auto_links():
    """A staff member who self-registers with the email their manager put on
    their employee record must link (and durably gain the venue)."""
    c = TestClient(app)
    h, _ = _owner(c)
    vid = "sr-autolink"
    _venue(c, h, vid)
    staff_email = f"sr{uuid.uuid4().hex[:8]}@x.com"
    _employee(c, h, vid, "e1", email=staff_email)

    s = TestClient(app)
    s.post("/api/auth/register", json={"email": staff_email, "password": "Passw0rd!234", "name": "S"})
    # Force the production reality: staff role, NO venues granted
    db = get_db()
    rec = db.get_user_by_email(staff_email)
    rec["role"] = "staff"
    rec["venue_ids"] = []
    db.save_user(rec)
    stok = s.post("/api/auth/login", json={"email": staff_email, "password": "Passw0rd!234"}).json()["access_token"]
    s_h = {"Authorization": f"Bearer {stok}"}

    prof = s.get("/api/me/profile", headers=s_h).json()
    assert prof["linked"] is True and prof["venue_id"] == vid
    # The grant is durable on the user record
    rec2 = db.get_user_by_email(staff_email)
    assert vid in rec2["venue_ids"]
    # And the rest of the portal now works
    assert s.get("/api/me/shifts", headers=s_h).status_code == 200


def test_c4_payroll_history_is_venue_scoped():
    c = TestClient(app)
    h, email = _owner(c)
    vid = "sr-payhist"
    _venue(c, h, vid)
    db = get_db()
    rec = db.get_user_by_email(email)
    rec["role"] = "manager"
    rec["venue_ids"] = [vid]
    db.save_user(rec)
    h = {"Authorization": f"Bearer {c.post('/api/auth/login', json={'email': email, 'password': 'Passw0rd!234'}).json()['access_token']}"}

    assert c.get(f"/api/payroll/history?venue_id={vid}", headers=h).status_code == 200
    assert c.get("/api/payroll/history?venue_id=demo-venue-001", headers=h).status_code == 403
    assert c.get("/api/payroll/history", headers=h).status_code == 422  # venue_id required


def test_c2_auto_schedule_generates():
    """The auto-scheduler endpoint must produce a roster, not a 500 (it had
    three stacked API-drift crashes), and owners must not be locked out."""
    c = TestClient(app)
    h, _ = _owner(c)
    vid = "sr-autoschedule"
    _venue(c, h, vid)
    for i in range(3):
        _employee(c, h, vid, f"e{i}")
    monday = _next_monday()
    c.post("/forecasts/generate", json={
        "venue_id": vid, "week_start": monday.isoformat()}, headers=h)

    r = c.post(f"/api/v1/venues/{vid}/auto-schedule", json={
        "week_start": monday.isoformat(), "strategy": "balanced"}, headers=h)
    assert r.status_code == 200, r.text
    assert r.json()["total_shifts"] > 0
    p = c.get(f"/api/v1/venues/{vid}/schedule-preview?week_start={monday.isoformat()}",
              headers=h)
    assert p.status_code == 200


def test_w5_think_tags_stripped():
    from rosteriq.ai_agent import _strip_reasoning
    assert _strip_reasoning("<think>internal plan</think>The roster is ready.") == "The roster is ready."
    assert _strip_reasoning("no tags here") == "no tags here"
    assert _strip_reasoning("<think>only thoughts</think>") == "only thoughts"
    assert _strip_reasoning("<THINK>x</THINK>ok") == "ok"


def test_w6_single_day_payroll_period_is_valid():
    from datetime import date as d
    from rosteriq.services.payroll_export import PayrollExporter, PayrollBatch
    batch = PayrollBatch(venue_id="x", period_start=d(2026, 8, 1), period_end=d(2026, 8, 1))
    errors = PayrollExporter(get_db()).validate_batch(batch)
    assert "Period start must be before period end" not in errors
