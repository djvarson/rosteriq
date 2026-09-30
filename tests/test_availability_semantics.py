"""
One meaning of staff availability across the product.

The staff app writes (routes/staff_portal.py): a day that isn't listed =
available all day; [] = unavailable; ranges = partly available. The generator
already read it that way, but the publisher's conflict check treated an
unlisted day as UNAVAILABLE (critical) — so anyone who marked a single day off
made every generated roster they were on unpublishable.
"""

from datetime import date, datetime, time
from decimal import Decimal

from fastapi.testclient import TestClient

from rosteriq.api import app
from rosteriq.database import get_db
from rosteriq.models import Employee, Shift, ShiftStatus
from rosteriq.services.conflict_detector import ConflictDetector, ConflictSeverity, ConflictType
from rosteriq.services.demo import DEMO_VENUE_ID


def _emp(avail):
    return Employee(id="av-e1", venue_id="av-v", name="Av", employment_type="casual", award_level="level_2",
                    state="wa", hourly_base_rate=Decimal("31.50"), availability=avail,
                    created_at=datetime(2026, 7, 1), updated_at=datetime(2026, 7, 1))


def _availability_conflicts(emp, day):
    shift = Shift(id="av-s1", employee_id=emp.id, date=day, start_time=time(10, 0), end_time=time(16, 0),
                  break_minutes=30, status=ShiftStatus.scheduled, role="floor")
    det = ConflictDetector()
    det.conflicts = []
    det._check_availability_violation(shift, emp)
    return [c for c in det.conflicts if c.conflict_type == ConflictType.AVAILABILITY_VIOLATION]


def test_an_unlisted_day_is_available():
    emp = _emp({"monday": []})                         # only Monday marked off
    assert _availability_conflicts(emp, date(2026, 10, 6)) == []          # Tuesday
    monday = _availability_conflicts(emp, date(2026, 10, 5))
    assert monday and monday[0].severity == ConflictSeverity.CRITICAL


def test_demo_rosters_publish_as_seeded_and_as_generated():
    c = TestClient(app)
    c.post("/api/auth/register", json={"email": "av_boot@x.com", "password": "Passw0rd!234", "name": "B"})
    h = {"Authorization": "Bearer " + c.post("/api/auth/demo").json()["access_token"]}
    seeded = [rid for rid, r in get_db()._rosters.items() if r.venue_id == DEMO_VENUE_ID]
    assert seeded
    for rid in seeded:
        r = c.post(f"/api/v1/rosters/{rid}/publish", json={"skip_approval": True}, headers=h).json()
        assert r.get("success") is True, r
    gen = c.post("/rosters/generate", json={"venue_id": DEMO_VENUE_ID, "week_start": "2026-10-05"}, headers=h).json()
    r = c.post(f"/api/v1/rosters/{gen['id']}/publish", json={"skip_approval": True}, headers=h).json()
    assert r.get("success") is True, r
