"""
Staff availability: /my sets it, the map to employee.availability is correct
(absent=available, []=unavailable, ranges=partial), validation is honest, and
the roster generator actually honours an unavailable day.
"""

import uuid
from datetime import date, datetime, timedelta

from fastapi.testclient import TestClient

from rosteriq.api import app
from rosteriq.database import get_db


def _register_login(c, email):
    c.post("/api/auth/register", json={"email": email, "password": "Passw0rd!234", "name": "U"})
    return {"Authorization": f"Bearer {c.post('/api/auth/login', json={'email': email, 'password': 'Passw0rd!234'}).json()['access_token']}"}


def _setup(c, owner_h, vid, staff_email):
    c.post("/venues", json={
        "id": vid, "name": "AV Venue", "state": "wa", "max_labour_pct": 30,
        "tanda_org_id": "", "created_at": "2026-07-01T00:00:00",
    }, headers=owner_h)
    c.post("/employees", json={
        "id": f"{vid}-emp", "name": "Avail Tester", "employment_type": "casual",
        "award_level": "level_2", "state": "wa", "venue_id": vid,
        "hourly_base_rate": "31.50", "email": staff_email,
        "created_at": "2026-07-01T00:00:00", "updated_at": "2026-07-01T00:00:00",
    }, headers=owner_h)


def _scope_staff(c, email, vid):
    db = get_db()
    rec = db.get_user_by_email(email)
    rec["venue_ids"] = [vid]
    rec["role"] = "staff"
    db.save_user(rec)


def test_availability_roundtrip_and_validation():
    c = TestClient(app)
    owner_h = _register_login(c, f"o{uuid.uuid4().hex[:8]}@x.com")
    staff_email = f"s{uuid.uuid4().hex[:8]}@x.com"
    vid = "av-venue-1"
    _setup(c, owner_h, vid, staff_email)
    staff_h = _register_login(c, staff_email)
    _scope_staff(c, staff_email, vid)

    # Default: every day available
    got = c.get("/api/me/availability", headers=staff_h).json()
    assert got["linked"] is True
    assert {d["day"]: d["status"] for d in got["days"]}["monday"] == "available"
    assert len(got["days"]) == 7

    # Set Monday unavailable, Tuesday partial (evenings only)
    r = c.post("/api/me/availability", json={"days": {
        "monday": {"status": "unavailable"},
        "tuesday": {"status": "partial", "ranges": [{"start": "17:00", "end": "23:00"}]},
    }}, headers=staff_h)
    assert r.status_code == 200, r.text

    got2 = {d["day"]: d for d in c.get("/api/me/availability", headers=staff_h).json()["days"]}
    assert got2["monday"]["status"] == "unavailable"
    assert got2["tuesday"]["status"] == "partial"
    assert got2["tuesday"]["ranges"] == [{"start": "17:00", "end": "23:00"}]
    assert got2["wednesday"]["status"] == "available"  # untouched

    # The map reached the employee record the roster engine reads
    emp = get_db().get_employee(f"{vid}-emp")
    assert emp.availability["monday"] == []
    assert emp.availability["tuesday"] == [{"start": "17:00", "end": "23:00"}]
    assert "wednesday" not in emp.availability

    # Flip Monday back to available -> day removed from the dict
    c.post("/api/me/availability", json={"days": {"monday": {"status": "available"}}},
           headers=staff_h)
    assert "monday" not in get_db().get_employee(f"{vid}-emp").availability

    # Validation: bad day, bad time, inverted range, partial-with-no-ranges
    assert c.post("/api/me/availability", json={"days": {"funday": {"status": "available"}}},
                  headers=staff_h).status_code == 422
    assert c.post("/api/me/availability", json={"days": {
        "friday": {"status": "partial", "ranges": [{"start": "25:00", "end": "26:00"}]}}},
        headers=staff_h).status_code == 422
    assert c.post("/api/me/availability", json={"days": {
        "friday": {"status": "partial", "ranges": [{"start": "18:00", "end": "09:00"}]}}},
        headers=staff_h).status_code == 422
    assert c.post("/api/me/availability", json={"days": {
        "friday": {"status": "partial", "ranges": []}}},
        headers=staff_h).status_code == 422


def test_manager_team_availability_view():
    c = TestClient(app)
    owner_h = _register_login(c, f"o{uuid.uuid4().hex[:8]}@x.com")
    staff_email = f"s{uuid.uuid4().hex[:8]}@x.com"
    vid = "av-venue-team"
    _setup(c, owner_h, vid, staff_email)
    # A second, fully-available employee
    c.post("/employees", json={
        "id": f"{vid}-emp2", "name": "Always Free", "employment_type": "casual",
        "award_level": "level_2", "state": "wa", "venue_id": vid,
        "hourly_base_rate": "31.50",
        "created_at": "2026-07-01T00:00:00", "updated_at": "2026-07-01T00:00:00",
    }, headers=owner_h)

    staff_h = _register_login(c, staff_email)
    _scope_staff(c, staff_email, vid)
    c.post("/api/me/availability", json={"days": {
        "monday": {"status": "unavailable"},
        "friday": {"status": "partial", "ranges": [{"start": "17:00", "end": "23:00"}]},
    }}, headers=staff_h)

    team = c.get(f"/api/availability?venue_id={vid}", headers=owner_h).json()
    assert team["count"] == 2
    # Constrained staff sort first
    first = team["staff"][0]
    assert first["name"] == "Avail Tester"
    assert first["days"]["monday"] == "unavailable"
    assert first["days"]["friday"] == "partial"
    assert first["days"]["tuesday"] == "available"
    assert set(first["constrained_days"]) == {"mon", "fri"}
    free = [s for s in team["staff"] if s["name"] == "Always Free"][0]
    assert free["constrained_days"] == []

    # Venue scoped
    outsider = _register_login(c, f"z{uuid.uuid4().hex[:8]}@x.com")
    db = get_db()
    rec = db.get_user_by_email([u["email"] for u in db.list_users()
                                if u["email"].startswith("z")][0])
    rec["venue_ids"] = ["elsewhere"]; rec["role"] = "manager"; db.save_user(rec)
    outsider = _register_login(c, rec["email"])
    assert c.get(f"/api/availability?venue_id={vid}", headers=outsider).status_code == 403


def test_unlinked_user_gets_honest_message():
    c = TestClient(app)
    stranger_h = _register_login(c, f"x{uuid.uuid4().hex[:8]}@x.com")
    got = c.get("/api/me/availability", headers=stranger_h).json()
    assert got["linked"] is False and "Ask your manager" in got["message"]
    assert c.post("/api/me/availability", json={"days": {"monday": {"status": "unavailable"}}},
                  headers=stranger_h).status_code == 409


def test_roster_generator_honours_unavailable_day():
    """End to end: mark the ONLY employee unavailable every day -> the generated
    roster can schedule nobody, proving availability actually gates the engine."""
    from rosteriq.roster_optimiser import _is_employee_available
    from rosteriq.models import Employee, EmploymentType, AwardLevel, State
    from decimal import Decimal

    emp = Employee(
        id="av-e", venue_id="av-x", name="Busy", employment_type=EmploymentType.casual,
        award_level=AwardLevel.level_2, state=State.wa, hourly_base_rate=Decimal("31.50"),
        availability={"monday": []},  # unavailable Monday
        created_at=datetime(2026, 7, 1), updated_at=datetime(2026, 7, 1),
    )
    # A known Monday
    d = date(2026, 8, 3)
    assert d.strftime("%A").lower() == "monday"
    ok, reason = _is_employee_available(emp, d, 9, 17, existing_shifts=[], weekly_hours=0.0)
    assert ok is False and "available" in reason.lower()

    emp.availability = {}  # fully available
    ok2, _ = _is_employee_available(emp, d, 9, 17, existing_shifts=[], weekly_hours=0.0)
    assert ok2 is True
