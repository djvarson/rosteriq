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


def test_lunch_covered_dinner_short_is_flagged():
    """The regression that mattered: a day fine at its busiest hour but short
    at another hour must NOT report fully covered (old peak-only bug)."""
    vid = "cov-lunch"
    d = date(2026, 8, 3)
    forecasts = [
        _forecast(vid, d, 12, 100),   # lunch: needs 5 at 20/staff
        _forecast(vid, d, 19, 95),    # dinner: needs 5
    ]
    roster = Roster(
        id="cov-ld", venue_id=vid, week_start=d, week_end=d + timedelta(days=6),
        shifts=(
            [Shift(id=f"l{i}", employee_id=f"e{i}", date=d, start_time=dtime(11, 0),
                   end_time=dtime(15, 0), break_minutes=0, status=ShiftStatus.scheduled,
                   role="floor") for i in range(5)]  # 5 lunch staff, gone by dinner
            + [Shift(id=f"dn{i}", employee_id=f"d{i}", date=d, start_time=dtime(17, 0),
                     end_time=dtime(21, 0), break_minutes=0, status=ShiftStatus.scheduled,
                     role="floor") for i in range(2)]  # only 2 dinner staff
        ),
        total_cost=None, created_at=datetime(2026, 7, 1),
    )
    result = compute_coverage_gaps(roster, forecasts, covers_per_staff=20.0)
    assert result["fully_covered"] is False
    day = result["days"][0]
    assert day["peak_hour"] == "19:00"  # the WORST hour, not the busiest
    assert day["required"] == 5 and day["rostered"] == 2 and day["gap"] == 3


def test_overnight_shift_covers_post_midnight_peak():
    """A 23:00-03:00 shift covers a 1am demand hour on the NEXT day."""
    vid = "cov-night"
    d = date(2026, 8, 3)
    nxt = d + timedelta(days=1)
    forecasts = [_forecast(vid, nxt, 1, 60)]  # 1am next day needs 3 at 20/staff
    roster = Roster(
        id="cov-on", venue_id=vid, week_start=d, week_end=d + timedelta(days=6),
        shifts=[Shift(id=f"n{i}", employee_id=f"e{i}", date=d, start_time=dtime(23, 0),
                      end_time=dtime(3, 0), break_minutes=0, status=ShiftStatus.scheduled,
                      role="bar") for i in range(3)],
        total_cost=None, created_at=datetime(2026, 7, 1),
    )
    result = compute_coverage_gaps(roster, forecasts, covers_per_staff=20.0)
    row = [r for r in result["days"] if r["date"] == nxt.isoformat()][0]
    assert row["rostered"] == 3 and row["status"] == "covered"


def test_sub_hour_shift_not_treated_as_24h():
    """A 09:15-09:45 shift covers only hour 9 — never the 7pm peak (old bug
    treated equal-hour shifts as a 24-hour span, masking shortfalls)."""
    vid = "cov-sub"
    d = date(2026, 8, 3)
    forecasts = [_forecast(vid, d, 19, 100)]  # dinner needs 5
    roster = Roster(
        id="cov-sh", venue_id=vid, week_start=d, week_end=d + timedelta(days=6),
        shifts=[Shift(id="s1", employee_id="e1", date=d, start_time=dtime(9, 15),
                      end_time=dtime(9, 45), break_minutes=0,
                      status=ShiftStatus.scheduled, role="floor")],
        total_cost=None, created_at=datetime(2026, 7, 1),
    )
    result = compute_coverage_gaps(roster, forecasts, covers_per_staff=20.0)
    assert result["days"][0]["rostered"] == 0 and result["days"][0]["status"] == "short"


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
