"""
Shift covers can't double-book, partial preference updates keep the rest,
and generating a roster fills forecast gaps day by day.

* Claiming a co-worker's shift that overlaps one of your own on the same day
  was accepted, and approving it moved the shift onto a person already working
  — the Try Demo cover (Emma claiming James's bar shift) did exactly that.
* A prefs update sent one section and silently reset the others to defaults.
* A week with a forecast for just one day (the demo seeds today's) rostered
  only that day.
"""

import uuid
from datetime import date, datetime, time as dtime, timedelta

from fastapi.testclient import TestClient

from rosteriq.api import app
from rosteriq.database import get_db
from rosteriq.models import DemandForecast, Roster, Shift, ShiftStatus

PW = "Passw0rd!234"


def _login(c, email):
    c.post("/api/auth/register", json={"email": email, "password": PW, "name": "U"})
    tok = c.post("/api/auth/login", json={"email": email, "password": PW}).json()["access_token"]
    return {"Authorization": f"Bearer {tok}"}


def _venue(c):
    email = f"cg{uuid.uuid4().hex[:8]}@x.com"
    h = _login(c, email)
    vid = f"cg-{uuid.uuid4().hex[:6]}"
    assert c.post("/venues", json={"id": vid, "name": vid, "state": "wa", "max_labour_pct": 30,
                                   "tanda_org_id": "", "created_at": "2026-07-01T00:00:00"},
                  headers=h).status_code in (200, 201)
    return _login(c, email), vid


def _staff(c, h, vid, suffix, email=None):
    body = {"id": f"{vid}-{suffix}", "venue_id": vid, "name": f"Staff {suffix}", "employment_type": "casual",
            "award_level": "level_2", "state": "wa", "hourly_base_rate": "31.50", "skills": ["bar"],
            "created_at": "2026-07-01T00:00:00", "updated_at": "2026-07-01T00:00:00"}
    if email:
        body["email"] = email
    assert c.post("/employees", json=body, headers=h).status_code == 200
    return body["id"]


def _staff_login(c, email, vid):
    h = _login(c, email)
    db = get_db()
    rec = db.get_user_by_email(email)
    rec["venue_ids"], rec["role"] = [vid], "staff"
    db.save_user(rec)
    return h


def _shift(sid, emp, day, start, end):
    return Shift(id=sid, employee_id=emp, date=day, start_time=start, end_time=end,
                 break_minutes=30, status=ShiftStatus.scheduled, role="bar")


def test_a_cover_that_would_double_book_is_refused_at_claim_and_at_approval():
    c = TestClient(app)
    _login(c, f"cg_boot_{uuid.uuid4().hex[:6]}@x.com")          # platform owner out of the way
    mgr, vid = _venue(c)
    a_email, b_email = f"a{uuid.uuid4().hex[:8]}@x.com", f"b{uuid.uuid4().hex[:8]}@x.com"
    a = _staff(c, mgr, vid, "a", a_email)
    b = _staff(c, mgr, vid, "b", b_email)
    day = date.today() + timedelta(days=1)
    ws = day - timedelta(days=day.weekday())
    db = get_db()
    roster = Roster(id=f"{vid}-r", venue_id=vid, week_start=ws, week_end=ws + timedelta(days=6),
                    shifts=[_shift("cg-a-eve", a, day, dtime(17, 0), dtime(23, 0)),
                            _shift("cg-b-day", b, day, dtime(11, 0), dtime(18, 0))],
                    total_cost=None, created_at=datetime(2026, 7, 1))
    db.save_roster(roster)
    a_h, b_h = _staff_login(c, a_email, vid), _staff_login(c, b_email, vid)

    cover = c.post("/api/me/shifts/cg-a-eve/cover", json={"reason": "Sick"}, headers=a_h).json()["cover_id"]
    r = c.post(f"/api/me/cover/{cover}/claim", headers=b_h)
    assert r.status_code == 409 and "overlaps" in r.text, r.text        # B works 11-18, the cover is 17-23

    # B's day shift ends before the cover starts: the claim goes through...
    roster = db.get_roster(f"{vid}-r")
    roster.shifts[1].end_time = dtime(16, 0)
    db.save_roster(roster)
    assert c.post(f"/api/me/cover/{cover}/claim", headers=b_h).status_code == 200
    # ...but if B is then rostered across it, approval is refused and nothing moves
    roster = db.get_roster(f"{vid}-r")
    roster.shifts.append(_shift("cg-b-late", b, day, dtime(20, 0), dtime(23, 30)))
    db.save_roster(roster)
    r = c.post(f"/api/cover/{cover}/decide", json={"venue_id": vid, "approve": True}, headers=mgr)
    assert r.status_code == 409 and "overlaps" in r.text, r.text
    assert next(s for s in db.get_roster(f"{vid}-r").shifts if s.id == "cg-a-eve").employee_id == a


def test_the_demo_cover_is_claimable_by_the_staff_phone():
    c = TestClient(app)
    c.post("/api/auth/register", json={"email": "cg_demo_boot@x.com", "password": PW, "name": "B"})
    staff = {"Authorization": "Bearer " + c.post("/api/auth/demo?as=staff").json()["access_token"]}
    board = c.get("/api/me/cover", headers=staff).json()
    assert board["linked"] and len(board["up_for_grabs"]) == 1, board
    r = c.post(f"/api/me/cover/{board['up_for_grabs'][0]['id']}/claim", headers=staff)
    assert r.status_code == 200, r.text


def test_a_partial_preferences_update_keeps_the_other_sections():
    c = TestClient(app)
    h = _login(c, f"cg_prefs_{uuid.uuid4().hex[:6]}@x.com")
    assert c.put("/api/notifications/preferences", json={"channels": {"email": False, "sms": True, "push": True}},
                 headers=h).status_code == 200
    r = c.put("/api/notifications/preferences",
              json={"quiet_hours": {"enabled": True, "start": "23:00", "end": "06:00"}}, headers=h)
    assert r.status_code == 200, r.text
    got = c.get("/api/notifications/preferences", headers=h).json()
    assert got["channels"] == {"email": False, "sms": True, "push": True}, got
    assert got["quiet_hours"]["start"] == "23:00"


def test_generate_fills_forecast_gaps_day_by_day():
    c = TestClient(app)
    _login(c, f"cg_boot2_{uuid.uuid4().hex[:6]}@x.com")
    mgr, vid = _venue(c)
    for i in range(3):
        _staff(c, mgr, vid, f"g{i}")
    monday = date(2026, 10, 12)
    get_db().add_forecasts([DemandForecast(id=f"{vid}-fc-{hr}", venue_id=vid, date=monday, hour=hr,
                                           predicted_covers=40.0, confidence=0.8, model_version="test")
                            for hr in range(11, 22)])
    r = c.post("/rosters/generate", json={"venue_id": vid, "week_start": monday.isoformat()}, headers=mgr)
    assert r.status_code == 200, r.text
    days = {s["date"] for s in r.json()["shifts"]}
    assert len(days) > 1, days                                  # not just the one forecast day
    kept = [f for f in get_db().get_forecasts(vid, monday, monday) if f.model_version == "test"]
    assert len(kept) == 11                                      # the real day's forecast is untouched
