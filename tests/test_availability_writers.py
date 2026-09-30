"""
Everything that WRITES availability produces the shape the shared rule reads,
and the main generator reads it the same way as everything else.

* roster_optimiser (v1, behind /rosters/generate) read "23:59" as 11:59pm, so
  a night staffer available to close was refused a shift ending at midnight.
* The staff app stored times as typed ("9"), which readers took as no limit.
* PUT /api/staff/availability wrote {"monday": {"available": false}}, which
  every reader treats as available all day. Retired.
* Integration re-syncs (Deputy/MYOB) replaced availability, skills and the
  linking email with the external system's blanks.
"""

import uuid
from datetime import date, datetime
from decimal import Decimal

from fastapi.testclient import TestClient

from rosteriq.api import app
from rosteriq.database import get_db
from rosteriq.models import Employee
from rosteriq.roster_optimiser import _is_employee_available

PW = "Passw0rd!234"


def _emp(eid="aw-e1", venue="aw-v", **kw):
    base = dict(id=eid, venue_id=venue, name="Night Owl", employment_type="casual", award_level="level_2",
                state="wa", hourly_base_rate=Decimal("31.50"), created_at=datetime(2026, 7, 1),
                updated_at=datetime(2026, 7, 1))
    base.update(kw)
    return Employee(**base)


def test_generator_reads_2359_as_close():
    e = _emp(availability={"friday": [{"start": "15:00", "end": "23:59"}]})
    fri = date(2026, 10, 9)
    assert _is_employee_available(e, fri, 17, 24, [], 0.0)[0] is True
    assert _is_employee_available(e, fri, 12, 18, [], 0.0)[0] is False


def _staff_session(c):
    email = f"aw{uuid.uuid4().hex[:8]}@x.com"
    c.post("/api/auth/register", json={"email": f"aw_boot_{uuid.uuid4().hex[:6]}@x.com", "password": PW, "name": "B"})
    db = get_db()
    vid = f"aw-{uuid.uuid4().hex[:6]}"
    db.save_employee(_emp(eid=f"{vid}-e", venue=vid, email=email))
    c.post("/api/auth/register", json={"email": email, "password": PW, "name": "S"})
    rec = db.get_user_by_email(email)
    rec["venue_ids"], rec["role"] = [vid], "staff"
    db.save_user(rec)
    tok = c.post("/api/auth/login", json={"email": email, "password": PW}).json()["access_token"]
    return {"Authorization": f"Bearer {tok}"}, f"{vid}-e"


def test_staff_app_stores_times_as_hh_mm():
    c = TestClient(app)
    h, eid = _staff_session(c)
    r = c.post("/api/me/availability", headers=h,
               json={"days": {"monday": {"status": "partial", "ranges": [{"start": "9", "end": "17:30"}]}}})
    assert r.status_code == 200, r.text
    assert get_db().get_employee(eid).availability["monday"] == [{"start": "09:00", "end": "17:30"}]


def test_legacy_availability_writer_is_retired():
    c = TestClient(app)
    h, eid = _staff_session(c)
    r = c.put("/api/staff/availability", headers=h,
              json={"availability": {"monday": {"available": False}}, "preferred_hours_per_week": 60})
    assert r.status_code == 410
    e = get_db().get_employee(eid)
    assert not e.availability and e.max_hours_per_week != 60


def test_a_resync_keeps_what_rosteriq_owns():
    from rosteriq.services.visa import save_synced_employee
    db = get_db()
    db.save_employee(_emp(eid="myob_abc", venue="aw-sync", availability={"monday": []}, skills=["bar"],
                          email="owl@x.com", created_at=datetime(2026, 1, 2)))
    synced = _emp(eid="myob_abc", venue="aw-sync", availability={}, skills=[], email=None,
                  hourly_base_rate=Decimal("33.00"), created_at=datetime(2026, 9, 30))
    assert save_synced_employee(db, synced)
    e = db.get_employee("myob_abc")
    assert e.hourly_base_rate == Decimal("33.00")                 # the external system's own field wins
    assert e.availability == {"monday": []} and e.skills == ["bar"] and e.email == "owl@x.com"
    assert e.created_at == datetime(2026, 1, 2)
