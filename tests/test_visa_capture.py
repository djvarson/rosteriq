"""
Work-rights capture: visa status, expiry, and the recorded fortnight cap.

Integrity pin: RosterIQ never hard-codes what a visa legally allows — every
check runs against what the MANAGER recorded from their own VEVO check.

Pins:
* Employee round-trips visa_status (normalised), visa_expiry, cap
* cap outside 1-152 is rejected; blank stays None
* visa_alerts: expired < today, expiring within 28 days, sorted urgent-first
* fortnight_cap_flags: this week + stored previous week vs recorded cap;
  employees without a cap are never flagged
* /rosters/generate returns visa_flags + visa_expiry_alerts in words
* briefing leads with an expired visa
* staff importer maps Visa / Visa Expiry columns (D/M/Y and ISO dates;
  garbage dates import as None, never a guess)
"""

import uuid
from datetime import date, datetime, time, timedelta

import pytest
from fastapi.testclient import TestClient

from rosteriq.api import app
from rosteriq.database import get_db
from rosteriq.models import Employee, Roster, Shift, ShiftStatus
from rosteriq.services.visa import fortnight_cap_flags, visa_alerts

PW = "Passw0rd!234"
TODAY = date(2026, 8, 31)


def _login(c, email):
    c.post("/api/auth/register", json={"email": email, "password": PW, "name": "U"})
    tok = c.post("/api/auth/login", json={"email": email, "password": PW}).json()["access_token"]
    return {"Authorization": f"Bearer {tok}"}


def _world():
    c = TestClient(app)
    tag = uuid.uuid4().hex[:6]
    h = _login(c, f"visa_{tag}@x.com")
    vid = f"visa-{tag}"
    r = c.post("/venues", json={"id": vid, "name": vid, "state": "wa", "max_labour_pct": 30,
                                "tanda_org_id": "", "created_at": "2026-07-01T00:00:00"},
               headers=h)
    assert r.status_code in (200, 201), r.text
    return c, h, vid


def _emp(vid, eid, name, **kw):
    return Employee(id=eid, venue_id=vid, name=name, employment_type="casual",
                    award_level="level_2", state="wa", hourly_base_rate="31.50",
                    created_at=datetime(2026, 7, 1), updated_at=datetime(2026, 7, 1), **kw)


def test_employee_visa_fields_round_trip_via_api():
    c, h, vid = _world()
    r = c.post("/employees", json={
        "id": f"{vid}-e1", "venue_id": vid, "name": "Mei Chen",
        "employment_type": "casual", "award_level": "level_2", "state": "wa",
        "hourly_base_rate": "31.50", "visa_status": " Student ",
        "visa_expiry": "2027-03-01", "visa_work_limit_fortnight": 48,
        "created_at": "2026-07-01T00:00:00", "updated_at": "2026-07-01T00:00:00",
    }, headers=h)
    assert r.status_code == 200, r.text
    emps = {e["id"]: e for e in c.get(f"/employees?venue_id={vid}", headers=h).json()["items"]}
    e = emps[f"{vid}-e1"]
    assert e["visa_status"] == "student"
    assert e["visa_expiry"] == "2027-03-01"
    assert float(e["visa_work_limit_fortnight"]) == 48.0


def test_cap_range_is_validated():
    with pytest.raises(Exception):
        _emp("v", "e", "X", visa_work_limit_fortnight=0)
    with pytest.raises(Exception):
        _emp("v", "e", "X", visa_work_limit_fortnight=200)
    assert _emp("v", "e", "X").visa_work_limit_fortnight is None


def test_visa_alerts_expired_and_expiring():
    emps = [
        _emp("v", "e1", "Expired Erin", visa_expiry=TODAY - timedelta(days=3)),
        _emp("v", "e2", "Soon Sam", visa_expiry=TODAY + timedelta(days=10)),
        _emp("v", "e3", "Fine Fred", visa_expiry=TODAY + timedelta(days=200)),
        _emp("v", "e4", "No Visa Nora"),
    ]
    alerts = visa_alerts(emps, today=TODAY)
    assert [a["employee_id"] for a in alerts] == ["e1", "e2"]
    assert alerts[0]["kind"] == "expired" and alerts[0]["days"] == -3
    assert alerts[1]["kind"] == "expiring" and alerts[1]["days"] == 10


def _shift(eid, d, start, end):
    return Shift(id=f"s-{uuid.uuid4().hex[:8]}", employee_id=eid, date=d,
                 start_time=time(start), end_time=time(end),
                 status=ShiftStatus.scheduled, role="bar")


def _roster(vid, week_start, shifts):
    return Roster(id=f"r-{uuid.uuid4().hex[:8]}", venue_id=vid,
                  week_start=week_start, week_end=week_start + timedelta(days=6),
                  shifts=shifts, created_at=datetime(2026, 8, 1))


def test_fortnight_cap_uses_both_weeks_and_skips_uncapped():
    wk = date(2026, 8, 24)
    capped = _emp("v", "cap1", "Mei Chen", visa_work_limit_fortnight=48)
    free = _emp("v", "free1", "Local Lou")
    # this week: 3x10h = 30h each; prev week: 2x10h = 20h each
    this_week = _roster("v", wk, [_shift(e, wk + timedelta(days=i), 9, 19)
                                  for e in ("cap1", "free1") for i in range(3)])
    prev_week = _roster("v", wk - timedelta(days=7),
                        [_shift(e, wk - timedelta(days=7) + timedelta(days=i), 9, 19)
                         for e in ("cap1", "free1") for i in range(2)])
    flags = fortnight_cap_flags(this_week, [capped, free], prev_week)
    assert len(flags) == 1
    f = flags[0]
    assert f["employee_id"] == "cap1"
    assert f["rostered_fortnight_hours"] == 50.0 and f["over_by_hours"] == 2.0
    assert "48" in f["message"] and "Mei Chen" in f["message"]

    # without the previous week, 30h alone is under the cap — no flag
    assert fortnight_cap_flags(this_week, [capped, free], None) == []


def test_briefing_leads_with_expired_visa():
    c, h, vid = _world()
    db = get_db()
    db.save_employee(_emp(vid, f"{vid}-x", "Expired Erin",
                          visa_status="student",
                          visa_expiry=date.today() - timedelta(days=5)))
    brief = c.get(f"/api/briefing?venue_id={vid}", headers=h).json()
    first = (brief.get("attention") or [""])[0]
    assert "Expired Erin" in first and "visa expired" in first


def test_staff_importer_maps_visa_columns():
    c, h, vid = _world()
    content = ("Name,Email,Pay Rate,Role,Visa Status,Visa Expiry\n"
               "Mei Chen,mei@x.com,31.50,bar,Student,01/03/2027\n"
               "Lou Park,lou@x.com,33.00,kitchen,,not-a-date\n")
    r = c.post("/api/setup/import-staff", json={"venue_id": vid, "content": content},
               headers=h)
    assert r.status_code == 200, r.text
    emps = {e.name: e for e in get_db().get_employees(vid)}
    assert emps["Mei Chen"].visa_status == "student"
    assert emps["Mei Chen"].visa_expiry == date(2027, 3, 1)
    assert emps["Lou Park"].visa_expiry is None  # garbage never guesses


def test_generate_roster_reports_visa_flags_in_words():
    c, h, vid = _world()
    db = get_db()
    always = {d: [{"start": "09:00", "end": "23:00"}]
              for d in ("monday", "tuesday", "wednesday", "thursday",
                        "friday", "saturday", "sunday")}
    # A tiny recorded cap the generator is certain to exceed across a week
    db.save_employee(_emp(vid, f"{vid}-m", "Mei Chen", skills=["bar"],
                          visa_status="student", visa_work_limit_fortnight=10,
                          availability=always))
    db.save_employee(_emp(vid, f"{vid}-l", "Local Lou", skills=["bar"],
                          availability=always))
    r = c.post("/rosters/generate",
               json={"venue_id": vid, "week_start": "2026-09-07"}, headers=h)
    assert r.status_code == 200, r.text
    body = r.json()
    mei_hours = sum(
        (datetime.fromisoformat(f"2026-01-01T{s['end_time']}") -
         datetime.fromisoformat(f"2026-01-01T{s['start_time']}")).total_seconds() / 3600
        for s in body["shifts"] if s["employee_id"] == f"{vid}-m")
    flags = body.get("visa_flags", [])
    if mei_hours > 10:
        assert flags and flags[0]["employee_id"] == f"{vid}-m"
        assert "Mei Chen" in flags[0]["message"] and "10" in flags[0]["message"]
    else:
        assert flags == []  # not rostered past the cap -> nothing to say


# ---------------------------------------------------------------------------
# Review-hardening pins
# ---------------------------------------------------------------------------

def _mk_staff(c, vid, email):
    h = _login(c, email)
    db = get_db()
    rec = db.get_user_by_email(email)
    rec["venue_ids"] = [vid]
    rec["role"] = "staff"
    db.save_user(rec)
    return h


def test_staff_callers_never_see_colleague_visa_fields():
    c, h, vid = _world()
    get_db().save_employee(_emp(vid, f"{vid}-m", "Mei Chen", visa_status="student",
                                visa_expiry=date(2027, 3, 1),
                                visa_work_limit_fortnight=48))
    staff_h = _mk_staff(c, vid, f"visa_staff_{uuid.uuid4().hex[:6]}@x.com")

    mine = {e["id"]: e for e in c.get(f"/employees?venue_id={vid}",
                                      headers=staff_h).json()["items"]}
    e = mine[f"{vid}-m"]
    assert e["visa_status"] is None and e["visa_expiry"] is None
    assert e["visa_work_limit_fortnight"] is None

    single = c.get(f"/employees/{vid}-m", headers=staff_h).json()
    assert single["visa_status"] is None

    boss = {e["id"]: e for e in c.get(f"/employees?venue_id={vid}",
                                      headers=h).json()["items"]}
    assert boss[f"{vid}-m"]["visa_status"] == "student"  # managers still see it


def test_briefing_hides_visa_alerts_from_staff():
    c, h, vid = _world()
    get_db().save_employee(_emp(vid, f"{vid}-x", "Expired Erin",
                                visa_expiry=date.today() - timedelta(days=5)))
    staff_h = _mk_staff(c, vid, f"visa_sb_{uuid.uuid4().hex[:6]}@x.com")
    brief = c.get(f"/api/briefing?venue_id={vid}", headers=staff_h).json()
    assert not any("visa" in a.lower() for a in brief.get("attention", []))


def test_sync_preserve_helper_keeps_recorded_visa():
    from rosteriq.services.visa import preserve_recorded_work_rights
    _, _, vid = _world()
    db = get_db()
    db.save_employee(_emp(vid, "deputy-9", "Mei Chen", visa_status="student",
                          visa_expiry=date(2027, 3, 1), visa_work_limit_fortnight=48))
    resynced = _emp(vid, "deputy-9", "Mei Chen")   # sync knows nothing of visas
    merged = preserve_recorded_work_rights(db, resynced)
    assert merged.visa_status == "student"
    assert merged.visa_expiry == date(2027, 3, 1)
    assert merged.visa_work_limit_fortnight == 48.0
    # a sync that DOES carry visa data wins over the stale local record
    fresh = _emp(vid, "deputy-9", "Mei Chen", visa_status="permanent_resident")
    assert preserve_recorded_work_rights(db, fresh).visa_status == "permanent_resident"


def test_fortnight_check_uses_newest_prev_week_roster():
    c, h, vid = _world()
    db = get_db()
    always = {d: [{"start": "09:00", "end": "23:00"}]
              for d in ("monday", "tuesday", "wednesday", "thursday",
                        "friday", "saturday", "sunday")}
    db.save_employee(_emp(vid, f"{vid}-m", "Mei Chen", skills=["bar"],
                          visa_work_limit_fortnight=100, availability=always))
    wk2 = date(2026, 9, 14)
    wk1 = wk2 - timedelta(days=7)
    # stale first draft: 90h for Mei; newest regeneration: 4h
    stale = _roster(vid, wk1, [_shift(f"{vid}-m", wk1 + timedelta(days=i), 9, 19)
                               for i in range(7)] +
                              [_shift(f"{vid}-m", wk1 + timedelta(days=i), 0, 20)
                               for i in range(1)])
    stale.created_at = datetime(2026, 9, 1, 8, 0)
    fresh = _roster(vid, wk1, [_shift(f"{vid}-m", wk1, 9, 13)])
    fresh.created_at = datetime(2026, 9, 1, 9, 0)
    db.save_roster(stale)
    db.save_roster(fresh)

    r = c.post("/rosters/generate", json={"venue_id": vid, "week_start": str(wk2)},
               headers=h)
    assert r.status_code == 200, r.text
    body = r.json()
    mei_hours = sum(
        (datetime.fromisoformat(f"2026-01-01T{s['end_time']}") -
         datetime.fromisoformat(f"2026-01-01T{s['start_time']}")).total_seconds() / 3600
        for s in body["shifts"] if s["employee_id"] == f"{vid}-m")
    flags = body.get("visa_flags", [])
    # against the NEWEST prev week (4h), Mei only flags if this week alone
    # pushes past 96h — impossible; against the stale 90h draft she'd flag
    # at just 10h. No flag = the newest roster was used.
    if mei_hours <= 96:
        assert flags == [], flags


def test_anonymise_scrubs_visa_data():
    _, _, vid = _world()
    db = get_db()
    db.save_employee(_emp(vid, f"{vid}-a", "Gone Person", visa_status="student",
                          visa_expiry=date(2027, 1, 1), visa_work_limit_fortnight=48))
    db.anonymise_employee(f"{vid}-a")
    emp = db.get_employee(f"{vid}-a")
    assert emp.visa_status is None and emp.visa_expiry is None
    assert emp.visa_work_limit_fortnight is None
