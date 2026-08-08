"""
Roster import from an export: employee matching (email/name), date + time
format tolerance (AU DD/MM, ISO, 12h/24h), week grouping (Mon-Sun), merge on
re-paste, honest skips for unmatched staff / bad dates, and the guards.
"""

import uuid
from datetime import date, timedelta

from fastapi.testclient import TestClient

from rosteriq.api import app
from rosteriq.database import get_db


def _owner(c):
    email = f"ri{uuid.uuid4().hex[:8]}@x.com"
    c.post("/api/auth/register", json={"email": email, "password": "Passw0rd!234", "name": "O"})
    tok = c.post("/api/auth/login", json={"email": email, "password": "Passw0rd!234"}).json()["access_token"]
    return {"Authorization": f"Bearer {tok}"}


def _venue_with_staff(c, h, vid):
    c.post("/venues", json={
        "id": vid, "name": "RI Venue", "state": "wa", "max_labour_pct": 30,
        "tanda_org_id": "", "created_at": "2026-07-01T00:00:00",
    }, headers=h)
    c.post("/api/setup/import-staff", json={
        "venue_id": vid,
        "content": "Name,Email\nEmma Thompson,emma@x.com\nJames Wilson,james@x.com\n",
    }, headers=h)
    return vid


def test_import_roster_matches_staff_parses_formats_and_groups_weeks():
    c = TestClient(app)
    h = _owner(c)
    vid = _venue_with_staff(c, h, "ri-venue-1")

    # Two shifts same week (match by email + by name), AU date, 12h + 24h times,
    # plus a shift in the next week -> two weekly rosters
    content = (
        "Employee,Email,Date,Start,End,Role,Break\n"
        "Emma Thompson,emma@x.com,03/08/2026,9:00am,5:30pm,Floor,30\n"
        "James Wilson,,04/08/2026,17:00,23:00,Bar,30\n"       # match by name (no email)
        "Emma Thompson,emma@x.com,10/08/2026,11:00,19:00,Floor,30\n"  # next week
    )
    r = c.post("/api/setup/import-roster", json={"venue_id": vid, "content": content}, headers=h)
    assert r.status_code == 200, r.text
    out = r.json()
    assert out["shifts_imported"] == 3 and out["weeks"] == 2 and out["skipped_count"] == 0

    db = get_db()
    rosters = [x for x in db.list_rosters() if x.venue_id == vid]
    assert len(rosters) == 2
    wk1 = db.get_roster(f"imported-{vid}-2026-08-03")
    assert wk1 and len(wk1.shifts) == 2
    emma = [s for s in wk1.shifts if s.date == date(2026, 8, 3)][0]
    assert emma.start_time.hour == 9 and emma.end_time.hour == 17 and emma.end_time.minute == 30
    assert emma.role == "Floor" and emma.break_minutes == 30
    jw = [s for s in wk1.shifts if s.date == date(2026, 8, 4)][0]
    assert jw.start_time.hour == 17 and jw.end_time.hour == 23


def test_import_roster_reports_unmatched_and_bad_dates():
    c = TestClient(app)
    h = _owner(c)
    vid = _venue_with_staff(c, h, "ri-venue-skip")
    content = (
        "Name,Date,Start,End\n"
        "Nobody Here,03/08/2026,09:00,17:00\n"     # unmatched staff
        "Emma Thompson,not-a-date,09:00,17:00\n"   # bad date
        "Emma Thompson,05/08/2026,huh,17:00\n"     # bad time
        "Emma Thompson,05/08/2026,09:00,17:00\n"   # good
    )
    out = c.post("/api/setup/import-roster", json={"venue_id": vid, "content": content}, headers=h).json()
    assert out["shifts_imported"] == 1
    reasons = " ".join(s["reason"] for s in out["skipped"])
    assert "no matching staff" in reasons and "bad date" in reasons and "bad start/end time" in reasons


def test_import_roster_merges_on_repaste():
    c = TestClient(app)
    h = _owner(c)
    vid = _venue_with_staff(c, h, "ri-venue-merge")
    first = "Name,Date,Start,End\nEmma Thompson,03/08/2026,09:00,17:00\n"
    c.post("/api/setup/import-roster", json={"venue_id": vid, "content": first}, headers=h)
    second = "Name,Date,Start,End\nJames Wilson,03/08/2026,17:00,23:00\n"
    c.post("/api/setup/import-roster", json={"venue_id": vid, "content": second}, headers=h)
    wk = get_db().get_roster(f"imported-{vid}-2026-08-03")
    assert wk and len(wk.shifts) == 2  # merged, not clobbered


def test_import_roster_guards():
    c = TestClient(app)
    h = _owner(c)
    # No staff yet -> 422 with guidance
    vid = "ri-venue-nostaff"
    c.post("/venues", json={
        "id": vid, "name": "RI", "state": "wa", "max_labour_pct": 30,
        "tanda_org_id": "", "created_at": "2026-07-01T00:00:00",
    }, headers=h)
    r = c.post("/api/setup/import-roster", json={
        "venue_id": vid, "content": "Name,Date,Start,End\nX,03/08/2026,09:00,17:00\n"}, headers=h)
    assert r.status_code == 422 and "staff first" in r.json().get("detail", "").lower() or \
        "staff first" in str(r.json())

    # Missing required columns -> 422
    vid2 = _venue_with_staff(c, h, "ri-venue-cols")
    r2 = c.post("/api/setup/import-roster", json={
        "venue_id": vid2, "content": "Name,Role\nEmma Thompson,Floor\n"}, headers=h)
    assert r2.status_code == 422


def test_import_roster_is_venue_scoped():
    c = TestClient(app)
    h = _owner(c)
    vid = _venue_with_staff(c, h, "ri-venue-scope")
    other = f"ri{uuid.uuid4().hex[:8]}@x.com"
    c.post("/api/auth/register", json={"email": other, "password": "Passw0rd!234", "name": "S"})
    db = get_db()
    rec = db.get_user_by_email(other)
    rec["venue_ids"] = ["not-this-one"]; rec["role"] = "manager"; db.save_user(rec)
    sh = {"Authorization": f"Bearer {c.post('/api/auth/login', json={'email': other, 'password': 'Passw0rd!234'}).json()['access_token']}"}
    assert c.post("/api/setup/import-roster", json={
        "venue_id": vid, "content": "Name,Date,Start,End\nEmma,03/08/2026,9,17\n"},
        headers=sh).status_code == 403
