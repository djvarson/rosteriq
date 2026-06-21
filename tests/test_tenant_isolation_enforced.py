"""
Proves tenant isolation is ENFORCED: a non-owner user scoped to venue A cannot
read or act on venue B's data (dashboard, connections, employees, payroll), while
they can still use venue A, and a platform owner can access everything.

Before this, any authenticated user could pass any venue_id and reach another
venue's data — the dominant pre-launch security gap.
"""

import uuid

from fastapi.testclient import TestClient

from rosteriq.api import app
from rosteriq.database import get_db


def _register(c, email):
    return c.post("/api/auth/register",
                  json={"email": email, "password": "Passw0rd!234", "name": "U"})


def _login(c, email):
    r = c.post("/api/auth/login", json={"email": email, "password": "Passw0rd!234"})
    body = r.json()
    return body.get("access_token") or body.get("tokens", {}).get("access_token")


def _venue(c, headers, vid):
    return c.post("/venues", json={
        "id": vid, "name": vid, "state": "vic", "max_labour_pct": 30,
        "tanda_org_id": "", "created_at": "2026-06-20T00:00:00",
    }, headers=headers)


def test_non_owner_cannot_reach_other_venue():
    c = TestClient(app)

    # First user bootstraps as owner; create two venues.
    owner_email = f"owner_{uuid.uuid4().hex[:8]}@x.com"
    _register(c, owner_email)
    oh = {"Authorization": f"Bearer {_login(c, owner_email)}"}
    assert _venue(c, oh, "venA").status_code == 200
    assert _venue(c, oh, "venB").status_code == 200

    # Second user is staff; scope them to venA only (set venue_ids + manager role).
    staff_email = f"staff_{uuid.uuid4().hex[:8]}@x.com"
    _register(c, staff_email)
    db = get_db()
    rec = db.get_user_by_email(staff_email)
    rec["venue_ids"] = ["venA"]
    rec["role"] = "manager"
    db.save_user(rec)
    sh = {"Authorization": f"Bearer {_login(c, staff_email)}"}

    # --- venA (theirs): allowed ---
    assert c.get("/dashboard/venA/overview", headers=sh).status_code == 200
    assert c.get("/api/connections/venue/venA", headers=sh).status_code == 200

    # --- venB (NOT theirs): denied with 403 across surfaces ---
    assert c.get("/dashboard/venB/overview", headers=sh).status_code == 403
    assert c.get("/api/connections/venue/venB", headers=sh).status_code == 403
    assert c.post("/api/connections/custom/connect",
                  json={"venue_id": "venB", "api_key": "k"}, headers=sh).status_code == 403
    assert c.get("/employees", params={"venue_id": "venB"}, headers=sh).status_code == 403
    assert c.post("/api/payroll/prepare", json={
        "venue_id": "venB", "period_start": "2026-06-22", "period_end": "2026-06-28",
        "state": "vic",
    }, headers=sh).status_code == 403

    # --- the broader endpoint cluster found in review: all must 403 for venB ---
    assert c.post("/rosters/generate-daily",
                  json={"venue_id": "venB", "target_date": "2026-06-22"}, headers=sh).status_code == 403
    assert c.get("/forecasts/required-staff",
                 params={"venue_id": "venB", "target_date": "2026-06-22"}, headers=sh).status_code == 403
    assert c.post("/employees", json={
        "id": "x", "name": "X", "employment_type": "casual", "award_level": "level_1",
        "state": "vic", "venue_id": "venB", "hourly_base_rate": "30.00",
        "created_at": "2025-01-01T00:00:00", "updated_at": "2025-01-01T00:00:00",
    }, headers=sh).status_code == 403
    assert c.post("/api/notifications/digest/venB",
                  params={"manager_email": "attacker@x.com"}, headers=sh).status_code == 403

    # --- owner (platform admin): can reach both ---
    assert c.get("/dashboard/venB/overview", headers=oh).status_code == 200
    # GET /venues is scoped: the non-owner sees only venA
    seen = {v["id"] for v in c.get("/venues", headers=sh).json()["items"]}
    assert seen <= {"venA"}, f"non-owner saw other venues: {seen}"


def test_employee_list_is_scoped_for_non_owner():
    """A non-owner listing employees with no venue_id sees only their venues'."""
    c = TestClient(app)
    owner_email = f"owner_{uuid.uuid4().hex[:8]}@x.com"
    _register(c, owner_email)
    oh = {"Authorization": f"Bearer {_login(c, owner_email)}"}
    _venue(c, oh, "venA")
    _venue(c, oh, "venB")
    # one employee in each venue
    for vid in ("venA", "venB"):
        c.post("/employees", json={
            "id": f"e-{vid}", "name": f"E {vid}", "employment_type": "full_time",
            "award_level": "level_1", "state": "vic", "venue_id": vid,
            "hourly_base_rate": "30.00",
            "created_at": "2025-01-01T00:00:00", "updated_at": "2025-01-01T00:00:00",
        }, headers=oh)

    staff_email = f"staff_{uuid.uuid4().hex[:8]}@x.com"
    _register(c, staff_email)
    db = get_db()
    rec = db.get_user_by_email(staff_email)
    rec["venue_ids"] = ["venA"]
    rec["role"] = "manager"
    db.save_user(rec)
    sh = {"Authorization": f"Bearer {_login(c, staff_email)}"}

    r = c.get("/employees", headers=sh)  # no venue_id -> only their venues
    assert r.status_code == 200
    venue_ids = {e["venue_id"] for e in r.json()["items"]}
    assert venue_ids <= {"venA"}, f"non-owner saw other venues' employees: {venue_ids}"
