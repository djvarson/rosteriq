"""
Roster coverage gaps: the generator/roster is compared against forecast demand
at each day's peak hour, and shortfalls are surfaced loudly (never a silent
understaffed day) — the safety net for staff marking themselves unavailable.
"""

import uuid
from datetime import date, datetime, time as dtime, timedelta

from fastapi.testclient import TestClient

from rosteriq.api import app
from rosteriq.database import get_db
from rosteriq.models import (
    Roster, Shift, ShiftStatus, DemandForecast,
)
from rosteriq.roster_optimiser import compute_coverage_gaps


def _forecast(vid, d, hour, covers):
    return DemandForecast(id=f"f-{hour}", venue_id=vid, date=d, hour=hour,
                          predicted_covers=covers, confidence=0.9,
                          model_version="test")


def test_compute_coverage_gaps_flags_shortfall():
    vid = "cov-x"
    d = date(2026, 8, 3)  # Monday
    # Peak at 19:00 needs ~5 staff (100 covers / 20 default per staff)
    forecasts = [_forecast(vid, d, h, 20) for h in range(11, 18)]
    forecasts.append(_forecast(vid, d, 19, 100))  # dinner peak

    # Roster only 2 people across the peak
    roster = Roster(
        id="cov-r", venue_id=vid, week_start=d, week_end=d + timedelta(days=6),
        shifts=[
            Shift(id="c1", employee_id="e1", date=d, start_time=dtime(17, 0),
                  end_time=dtime(23, 0), break_minutes=30, status=ShiftStatus.scheduled, role="bar"),
            Shift(id="c2", employee_id="e2", date=d, start_time=dtime(17, 0),
                  end_time=dtime(23, 0), break_minutes=30, status=ShiftStatus.scheduled, role="floor"),
        ],
        total_cost=None, created_at=datetime(2026, 7, 1),
    )
    result = compute_coverage_gaps(roster, forecasts, covers_per_staff=20.0)
    assert result["fully_covered"] is False
    assert result["shortfall_count"] == 1
    monday = [g for g in result["days"] if g["date"] == d.isoformat()][0]
    assert monday["peak_hour"] == "19:00"
    assert monday["required"] == 5 and monday["rostered"] == 2
    assert monday["gap"] == 3 and monday["status"] == "short"
    assert result["total_missing_staff"] == 3


def test_compute_coverage_gaps_all_covered():
    vid = "cov-y"
    d = date(2026, 8, 3)
    forecasts = [_forecast(vid, d, 19, 40)]  # needs 2
    roster = Roster(
        id="cov-r2", venue_id=vid, week_start=d, week_end=d + timedelta(days=6),
        shifts=[
            Shift(id="c1", employee_id="e1", date=d, start_time=dtime(17, 0),
                  end_time=dtime(23, 0), break_minutes=0, status=ShiftStatus.scheduled, role="bar"),
            Shift(id="c2", employee_id="e2", date=d, start_time=dtime(17, 0),
                  end_time=dtime(23, 0), break_minutes=0, status=ShiftStatus.scheduled, role="floor"),
        ],
        total_cost=None, created_at=datetime(2026, 7, 1),
    )
    result = compute_coverage_gaps(roster, forecasts, covers_per_staff=20.0)
    assert result["fully_covered"] is True
    assert result["shortfall_count"] == 0
    assert result["days"][0]["status"] == "covered"


def test_coverage_endpoint_scoped_and_reports():
    c = TestClient(app)
    email = f"cov{uuid.uuid4().hex[:8]}@x.com"
    c.post("/api/auth/register", json={"email": email, "password": "Passw0rd!234", "name": "O"})
    h = {"Authorization": f"Bearer {c.post('/api/auth/login', json={'email': email, 'password': 'Passw0rd!234'}).json()['access_token']}"}
    vid = "cov-venue"
    c.post("/venues", json={
        "id": vid, "name": "Cov", "state": "wa", "max_labour_pct": 30,
        "tanda_org_id": "", "created_at": "2026-07-01T00:00:00",
    }, headers=h)

    d = date(2026, 8, 3)
    db = get_db()
    # Seed a peak forecast + a thin roster directly
    db.add_forecasts([_forecast(vid, d, 19, 100)])
    db.save_roster(Roster(
        id="cov-live", venue_id=vid, week_start=d, week_end=d + timedelta(days=6),
        shifts=[Shift(id="cl1", employee_id="e1", date=d, start_time=dtime(17, 0),
                      end_time=dtime(23, 0), break_minutes=0,
                      status=ShiftStatus.scheduled, role="bar")],
        total_cost=None, created_at=datetime(2026, 7, 1),
    ))

    r = c.get(f"/rosters/cov-live/coverage?covers_per_staff=20", headers=h)
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["fully_covered"] is False and body["shortfall_count"] == 1
    assert body["days"][0]["required"] == 5 and body["days"][0]["rostered"] == 1

    # Unknown roster -> 404
    assert c.get("/rosters/nope/coverage", headers=h).status_code == 404

    # Another tenant can't read this roster's coverage
    other = f"cov{uuid.uuid4().hex[:8]}@x.com"
    c.post("/api/auth/register", json={"email": other, "password": "Passw0rd!234", "name": "S"})
    rec = db.get_user_by_email(other)
    rec["venue_ids"] = ["someone-else"]
    rec["role"] = "manager"
    db.save_user(rec)
    sh = {"Authorization": f"Bearer {c.post('/api/auth/login', json={'email': other, 'password': 'Passw0rd!234'}).json()['access_token']}"}
    assert c.get("/rosters/cov-live/coverage", headers=sh).status_code == 403
