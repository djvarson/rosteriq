"""
Payroll from approved timesheets: unapproved punches block the batch loudly
(named, not counted), approved actuals price the batch, and the accountant
CSV downloads with the sign-off warning.
"""

import uuid
from datetime import date, datetime, time as dtime

from fastapi.testclient import TestClient

from rosteriq.api import app


def _owner(c):
    email = f"pa{uuid.uuid4().hex[:8]}@x.com"
    c.post("/api/auth/register", json={"email": email, "password": "Passw0rd!234", "name": "O"})
    tok = c.post("/api/auth/login", json={"email": email, "password": "Passw0rd!234"}).json()["access_token"]
    return {"Authorization": f"Bearer {tok}"}


def _setup(c, h, vid):
    c.post("/venues", json={
        "id": vid, "name": "PA Venue", "state": "wa", "max_labour_pct": 30,
        "tanda_org_id": "", "created_at": "2026-07-01T00:00:00",
    }, headers=h)
    c.post("/employees", json={
        "id": f"{vid}-emp", "name": "Pay Tester", "employment_type": "casual",
        "award_level": "level_2", "state": "wa", "venue_id": vid,
        "hourly_base_rate": "30.00",
        "created_at": "2026-07-01T00:00:00", "updated_at": "2026-07-01T00:00:00",
    }, headers=h)


def _punch_and_approve(c, h, vid, approve=True):
    """Clock in/out, then (optionally) approve corrected to a clean 9:00-17:30
    with a 30m break = 8.0 paid hours."""
    c.post("/api/clock/in", json={"venue_id": vid, "employee_id": f"{vid}-emp"}, headers=h)
    ts_id = c.post("/api/clock/out", json={"venue_id": vid, "employee_id": f"{vid}-emp"},
                   headers=h).json()["timesheet_id"]
    if approve:
        today = date.today()
        r = c.post(f"/api/clock/timesheets/{ts_id}/review", json={
            "venue_id": vid, "approve": True,
            "clock_in": datetime.combine(today, dtime(9, 0)).isoformat(),
            "clock_out": datetime.combine(today, dtime(17, 30)).isoformat(),
            "break_minutes": 30, "note": "clean day for payroll test",
        }, headers=h)
        assert r.status_code == 200, r.text
    return ts_id


def test_unapproved_timesheets_block_payroll_by_name():
    c = TestClient(app)
    h = _owner(c)
    vid = "pa-venue-block"
    _setup(c, h, vid)
    _punch_and_approve(c, h, vid, approve=False)  # closed, never approved

    today = date.today().isoformat()
    r = c.post("/api/payroll/prepare-actuals", json={
        "venue_id": vid, "period_start": today, "period_end": today, "state": "wa",
    }, headers=h)
    assert r.status_code == 409
    body = r.json()
    message = body.get("detail") or body.get("error", {}).get("message", "")
    assert "Pay Tester" in message and "awaiting approval" in message


def test_approved_actuals_price_the_batch_and_csv_downloads():
    c = TestClient(app)
    h = _owner(c)
    vid = "pa-venue-ok"
    _setup(c, h, vid)
    _punch_and_approve(c, h, vid, approve=True)

    today = date.today().isoformat()
    r = c.post("/api/payroll/prepare-actuals", json={
        "venue_id": vid, "period_start": today, "period_end": today, "state": "wa",
    }, headers=h)
    assert r.status_code == 200, r.text
    batch = r.json()
    assert batch["employee_count"] == 1
    assert float(batch["total_gross"]) > 0        # 8h priced by the award engine
    assert float(batch["total_super"]) > 0

    csv_r = c.get(f"/api/payroll/batch/{batch['batch_id']}/csv?venue_id={vid}", headers=h)
    assert csv_r.status_code == 200
    assert "text/csv" in csv_r.headers["content-type"]
    assert "attachment" in csv_r.headers.get("content-disposition", "")
    text = csv_r.text
    assert "Pay Tester" in text
    assert "NOT a payment file" in text           # advisor sign-off warning
    assert "TOTALS" in text

    # Wrong venue can't fetch the batch csv
    other = _owner(c)
    c.post("/venues", json={
        "id": "pa-other", "name": "Other", "state": "wa", "max_labour_pct": 30,
        "tanda_org_id": "", "created_at": "2026-07-01T00:00:00",
    }, headers=other)
    assert c.get(f"/api/payroll/batch/{batch['batch_id']}/csv?venue_id=pa-other",
                 headers=other).status_code == 404


def test_empty_period_is_honest():
    c = TestClient(app)
    h = _owner(c)
    vid = "pa-venue-empty"
    _setup(c, h, vid)
    r = c.post("/api/payroll/prepare-actuals", json={
        "venue_id": vid, "period_start": "2026-01-05", "period_end": "2026-01-11",
        "state": "wa",
    }, headers=h)
    assert r.status_code == 422
    body = r.json()
    message = body.get("detail") or body.get("error", {}).get("message", "")
    assert "timeclock" in message
