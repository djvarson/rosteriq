"""
Staff portal: email-linked identity, my-shifts visibility, my hours, and the
leave request lifecycle (request -> manager decide), with honest no-link
messaging and venue scoping.
"""

import uuid
from datetime import date, datetime, time as dtime, timedelta

from fastapi.testclient import TestClient

from rosteriq.api import app
from rosteriq.database import get_db
from rosteriq.models import Roster, Shift, ShiftStatus


def _register_login(c, email):
    c.post("/api/auth/register", json={"email": email, "password": "Passw0rd!234", "name": "U"})
    return {"Authorization": f"Bearer {c.post('/api/auth/login', json={'email': email, 'password': 'Passw0rd!234'}).json()['access_token']}"}


def _setup(c, owner_h, vid, staff_email):
    c.post("/venues", json={
        "id": vid, "name": "SP Venue", "state": "wa", "max_labour_pct": 30,
        "tanda_org_id": "", "created_at": "2026-07-01T00:00:00",
    }, headers=owner_h)
    c.post("/employees", json={
        "id": f"{vid}-emp", "name": "Portal Tester", "employment_type": "casual",
        "award_level": "level_2", "state": "wa", "venue_id": vid,
        "hourly_base_rate": "31.50", "skills": ["bar"], "email": staff_email,
        "created_at": "2026-07-01T00:00:00", "updated_at": "2026-07-01T00:00:00",
    }, headers=owner_h)


def _scope_staff(c, email, vid):
    db = get_db()
    rec = db.get_user_by_email(email)
    rec["venue_ids"] = [vid]
    rec["role"] = "staff"
    db.save_user(rec)


def test_profile_links_by_email_and_no_link_is_honest():
    c = TestClient(app)
    owner_h = _register_login(c, f"o{uuid.uuid4().hex[:8]}@x.com")
    staff_email = f"s{uuid.uuid4().hex[:8]}@x.com"
    _setup(c, owner_h, "sp-venue-1", staff_email)

    staff_h = _register_login(c, staff_email)
    _scope_staff(c, staff_email, "sp-venue-1")

    prof = c.get("/api/me/profile", headers=staff_h).json()
    assert prof["linked"] is True and prof["name"] == "Portal Tester"

    # An account with no matching employee gets a clear message, not a blank
    stranger_h = _register_login(c, f"x{uuid.uuid4().hex[:8]}@x.com")
    p2 = c.get("/api/me/profile", headers=stranger_h).json()
    assert p2["linked"] is False and "Ask your manager" in p2["message"]


def test_my_shifts_shows_only_mine():
    c = TestClient(app)
    owner_h = _register_login(c, f"o{uuid.uuid4().hex[:8]}@x.com")
    staff_email = f"s{uuid.uuid4().hex[:8]}@x.com"
    vid = "sp-venue-2"
    _setup(c, owner_h, vid, staff_email)
    c.post("/employees", json={
        "id": f"{vid}-other", "name": "Someone Else", "employment_type": "casual",
        "award_level": "level_2", "state": "wa", "venue_id": vid,
        "hourly_base_rate": "31.50",
        "created_at": "2026-07-01T00:00:00", "updated_at": "2026-07-01T00:00:00",
    }, headers=owner_h)

    today = date.today()
    week_start = today - timedelta(days=today.weekday())
    tomorrow = today + timedelta(days=1)
    shift_day = tomorrow if tomorrow <= week_start + timedelta(days=6) else today
    db = get_db()
    db.save_roster(Roster(
        id="sp-roster", venue_id=vid, week_start=week_start,
        week_end=week_start + timedelta(days=6),
        shifts=[
            Shift(id="sp-s1", employee_id=f"{vid}-emp", date=shift_day,
                  start_time=dtime(17, 0), end_time=dtime(23, 0),
                  break_minutes=30, status=ShiftStatus.scheduled, role="bar"),
            Shift(id="sp-s2", employee_id=f"{vid}-other", date=shift_day,
                  start_time=dtime(9, 0), end_time=dtime(17, 0),
                  break_minutes=30, status=ShiftStatus.scheduled, role="floor"),
        ],
        total_cost=None, created_at=datetime(2026, 7, 1),
    ))

    staff_h = _register_login(c, staff_email)
    _scope_staff(c, staff_email, vid)
    mine = c.get("/api/me/shifts", headers=staff_h).json()
    assert mine["count"] == 1
    assert mine["shifts"][0]["start"] == "17:00" and mine["shifts"][0]["role"] == "bar"


def test_leave_lifecycle():
    c = TestClient(app)
    owner_h = _register_login(c, f"o{uuid.uuid4().hex[:8]}@x.com")
    staff_email = f"s{uuid.uuid4().hex[:8]}@x.com"
    vid = "sp-venue-3"
    _setup(c, owner_h, vid, staff_email)
    staff_h = _register_login(c, staff_email)
    _scope_staff(c, staff_email, vid)

    nxt = date.today() + timedelta(days=7)
    # Past dates rejected
    past = c.post("/api/me/leave", json={
        "start_date": (date.today() - timedelta(days=3)).isoformat(),
        "end_date": date.today().isoformat(),
    }, headers=staff_h)
    assert past.status_code == 422

    r = c.post("/api/me/leave", json={
        "start_date": nxt.isoformat(), "end_date": (nxt + timedelta(days=2)).isoformat(),
        "reason": "Family trip",
    }, headers=staff_h)
    assert r.status_code == 200, r.text
    req_id = r.json()["request_id"]

    # Overlapping second pending request rejected
    dup = c.post("/api/me/leave", json={
        "start_date": (nxt + timedelta(days=1)).isoformat(),
        "end_date": (nxt + timedelta(days=3)).isoformat(),
    }, headers=staff_h)
    assert dup.status_code == 409

    # Manager sees it pending, approves with a note
    pend = c.get(f"/api/leave?venue_id={vid}&status=pending", headers=owner_h).json()
    assert pend["count"] == 1 and pend["requests"][0]["employee_name"] == "Portal Tester"
    dec = c.post(f"/api/leave/{req_id}/decide", json={
        "venue_id": vid, "approve": True, "note": "Enjoy!",
    }, headers=owner_h)
    assert dec.status_code == 200 and dec.json()["status"] == "approved"

    # Double-decide rejected; staff sees the outcome
    dbl = c.post(f"/api/leave/{req_id}/decide", json={"venue_id": vid, "approve": False},
                 headers=owner_h)
    assert dbl.status_code == 409
    mine = c.get("/api/me/leave", headers=staff_h).json()
    assert mine["requests"][0]["status"] == "approved"
    assert mine["requests"][0]["decision_note"] == "Enjoy!"


def test_manager_leave_endpoints_are_venue_scoped():
    c = TestClient(app)
    owner_h = _register_login(c, f"o{uuid.uuid4().hex[:8]}@x.com")
    vid = "sp-venue-4"
    _setup(c, owner_h, vid, f"s{uuid.uuid4().hex[:8]}@x.com")
    outsider = _register_login(c, f"m{uuid.uuid4().hex[:8]}@x.com")
    db = get_db()
    rec = db.get_user_by_email([k for k in [None]][0] or "")  # placeholder no-op
    # Scope outsider to a different venue
    for u in db.list_users():
        if u["email"].startswith("m") and u["role"] == "staff":
            u["venue_ids"] = ["other-venue"]
            u["role"] = "manager"
            db.save_user(u)
    assert c.get(f"/api/leave?venue_id={vid}", headers=outsider).status_code == 403
