"""
Sweep INFO-tier hardening: accidental-punch warning and named allowances
in the accountant CSV.
"""

import uuid
from datetime import date, datetime, time as dtime

from fastapi.testclient import TestClient

from rosteriq.api import app


def _owner(c):
    email = f"hd{uuid.uuid4().hex[:8]}@x.com"
    c.post("/api/auth/register", json={"email": email, "password": "Passw0rd!234", "name": "O"})
    tok = c.post("/api/auth/login", json={"email": email, "password": "Passw0rd!234"}).json()["access_token"]
    return {"Authorization": f"Bearer {tok}"}


def _setup(c, h, vid):
    c.post("/venues", json={
        "id": vid, "name": "HD Venue", "state": "wa", "max_labour_pct": 30,
        "tanda_org_id": "", "created_at": "2026-07-01T00:00:00",
    }, headers=h)
    c.post("/employees", json={
        "id": f"{vid}-emp", "name": "Hard Ening", "employment_type": "casual",
        "award_level": "level_2", "state": "wa", "venue_id": vid,
        "hourly_base_rate": "30.00",
        "created_at": "2026-07-01T00:00:00", "updated_at": "2026-07-01T00:00:00",
    }, headers=h)


def test_zero_minute_punch_warns_but_is_never_blocked():
    c = TestClient(app)
    h = _owner(c)
    vid = "hd-venue-punch"
    _setup(c, h, vid)
    c.post("/api/clock/in", json={"venue_id": vid, "employee_id": f"{vid}-emp"}, headers=h)
    out = c.post("/api/clock/out", json={"venue_id": vid, "employee_id": f"{vid}-emp"},
                 headers=h).json()
    assert out["status"] == "clocked_out"          # the punch is a fact, kept
    assert "accidental punch" in out["warning"]    # but flagged loudly


def test_payroll_csv_names_every_allowance():
    c = TestClient(app)
    h = _owner(c)
    vid = "hd-venue-csv"
    _setup(c, h, vid)
    c.post("/api/clock/in", json={"venue_id": vid, "employee_id": f"{vid}-emp"}, headers=h)
    ts_id = c.post("/api/clock/out", json={"venue_id": vid, "employee_id": f"{vid}-emp"},
                   headers=h).json()["timesheet_id"]
    today = date.today()
    c.post(f"/api/clock/timesheets/{ts_id}/review", json={
        "venue_id": vid, "approve": True,
        "clock_in": datetime.combine(today, dtime(9, 0)).isoformat(),
        "clock_out": datetime.combine(today, dtime(17, 30)).isoformat(),
        "break_minutes": 30, "note": "hardening test",
    }, headers=h)
    batch = c.post("/api/payroll/prepare-actuals", json={
        "venue_id": vid, "period_start": today.isoformat(),
        "period_end": today.isoformat(), "state": "wa",
    }, headers=h).json()

    text = c.get(f"/api/payroll/batch/{batch['batch_id']}/csv?venue_id={vid}",
                 headers=h).text
    assert "Allowance detail" in text
    # If any allowance dollars exist in the batch they must be named in the
    # detail column (type + amount), never just a bare total
    emp_line = [l for l in text.splitlines() if l.startswith("Hard Ening")][0]
    cols = emp_line.split(",")
    allowances_total = cols[9]
    if allowances_total not in ("", "0", "0.00"):
        assert "$" in cols[8], f"allowance total {allowances_total} has no named detail: {emp_line}"
