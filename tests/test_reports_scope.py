"""
Compliance / hours / penalty reports are venue-private.

routes/reports.py had no venue gate on any of its six endpoints, and filtered
employees with `... or True` — so any signed-in user (including the public
demo) could pull any venue's reports, and even a venue's OWN report listed
every employee on the platform.
"""

import uuid

from fastapi.testclient import TestClient

from rosteriq.api import app

PW = "Passw0rd!234"
ENDPOINTS = [
    "/api/reports/compliance/{v}",
    "/api/reports/compliance/{v}/pdf",
    "/api/reports/compliance/{v}/csv",
    "/api/reports/hours/{v}",
    "/api/reports/penalties/{v}",
    "/api/reports/certifications/{v}",
]


def _login(c, email):
    c.post("/api/auth/register", json={"email": email, "password": PW, "name": "U"})
    r = c.post("/api/auth/login", json={"email": email, "password": PW})
    return {"Authorization": f"Bearer {r.json()['access_token']}"}


def _venue(c, staff_name):
    tag = uuid.uuid4().hex[:6]
    h = _login(c, f"rs_{tag}@x.com")
    vid = f"rs-{tag}"
    assert c.post("/venues", json={"id": vid, "name": vid, "state": "wa", "max_labour_pct": 30,
                                   "tanda_org_id": "", "created_at": "2026-07-01T00:00:00"},
                  headers=h).status_code in (200, 201)
    assert c.post("/employees", json={
        "id": f"{vid}-e0", "venue_id": vid, "name": staff_name, "employment_type": "casual",
        "award_level": "level_2", "state": "wa", "hourly_base_rate": "31.50",
        "created_at": "2026-07-01T00:00:00", "updated_at": "2026-07-01T00:00:00"},
        headers=h).status_code == 200
    return h, vid


def test_reports_refuse_other_venues():
    c = TestClient(app)
    _login(c, f"rs_boot_{uuid.uuid4().hex[:6]}@x.com")
    a_h, _ = _venue(c, "Alice Own")
    _, b_vid = _venue(c, "Bob Neighbour")
    for ep in ENDPOINTS:
        r = c.get(ep.format(v=b_vid), headers=a_h)
        assert r.status_code == 403, (ep, r.status_code, r.text[:200])


def test_demo_cannot_read_a_real_venues_reports():
    c = TestClient(app)
    _login(c, f"rs_boot_{uuid.uuid4().hex[:6]}@x.com")
    _, b_vid = _venue(c, "Bob Neighbour")
    r = c.post("/api/auth/demo")
    assert r.status_code == 200, r.text
    demo_h = {"Authorization": f"Bearer {r.json()['access_token']}"}
    for ep in ENDPOINTS:
        assert c.get(ep.format(v=b_vid), headers=demo_h).status_code == 403, ep


def test_own_report_lists_only_own_staff():
    c = TestClient(app)
    _login(c, f"rs_boot_{uuid.uuid4().hex[:6]}@x.com")
    a_h, a_vid = _venue(c, "Alice Own")
    _venue(c, "Bob Neighbour")
    r = c.get(f"/api/reports/certifications/{a_vid}", headers=a_h)
    assert r.status_code == 200, r.text
    assert "Bob Neighbour" not in r.text
    r = c.get(f"/api/reports/compliance/{a_vid}/csv", headers=a_h)
    assert r.status_code == 200, r.text
    assert "Bob Neighbour" not in r.text


# ---------------------------------------------------------------------------
# Roster cost simulator: same class of hole (no venue gate, platform-wide
# employee list feeding the pay-rate pricing).
# ---------------------------------------------------------------------------

def _generate(c, h, vid):
    r = c.post("/rosters/generate", json={"venue_id": vid, "week_start": "2026-10-05"}, headers=h)
    assert r.status_code == 200, r.text
    return r.json()["id"]


def test_simulator_refuses_other_venues_rosters():
    c = TestClient(app)
    _login(c, f"rs_boot_{uuid.uuid4().hex[:6]}@x.com")
    a_h, a_vid = _venue(c, "Alice Own")
    b_h, b_vid = _venue(c, "Bob Neighbour")
    b_roster = _generate(c, b_h, b_vid)
    a_roster = _generate(c, a_h, a_vid)
    body = {"changes": []}
    for path, payload in (("simulate", body), ("find-savings", {"target_savings_pct": 10})):
        r = c.post(f"/api/v1/rosters/{b_roster}/{path}", json=payload, headers=a_h)
        assert r.status_code == 404, (path, r.status_code, r.text[:200])
    # own roster still works
    r = c.post(f"/api/v1/rosters/{a_roster}/simulate", json=body, headers=a_h)
    assert r.status_code == 200, r.text[:300]
