"""
A roster can be published WITHOUT a connected rostering integration.

Publishing used to be impossible end-to-end: the publish route used user.id (the
attribute is user_id) and the publisher called store methods that didn't exist
(get_roster_state / update_roster_state / get_roster_state_history / publication
events) plus list_employees(venue_id) (the method takes no arg). This pins the
fixed, connector-independent publish path.
"""

import uuid

from fastapi.testclient import TestClient

from rosteriq.api import app


def _setup_owner(c):
    email = f"o{uuid.uuid4().hex[:8]}@x.com"
    c.post("/api/auth/register", json={"email": email, "password": "Passw0rd!234", "name": "O"})
    tok = c.post("/api/auth/login", json={"email": email, "password": "Passw0rd!234"}).json()["access_token"]
    return {"Authorization": f"Bearer {tok}"}


def _seed_roster(c, h, venue_id="pv"):
    c.post("/venues", json={
        "id": venue_id, "name": "PV", "state": "vic", "max_labour_pct": 30,
        "tanda_org_id": "", "created_at": "2026-06-20T00:00:00",
    }, headers=h)
    emps = [{
        "id": f"e{i}", "name": f"E{i}", "employment_type": "full_time",
        "award_level": "level_1", "state": "vic", "venue_id": venue_id,
        "hourly_base_rate": "30.00",
        "created_at": "2025-01-01T00:00:00", "updated_at": "2025-01-01T00:00:00",
    } for i in range(6)]
    c.post("/employees/bulk", json=emps, headers=h)
    return c.post("/rosters/generate", json={"venue_id": venue_id, "week_start": "2026-06-22"},
                  headers=h).json()["id"]


def test_publish_succeeds_without_integration():
    c = TestClient(app)
    h = _setup_owner(c)
    rid = _seed_roster(c, h)  # venue has NO tanda/deputy/hf integration

    r = c.post(f"/api/v1/rosters/{rid}/publish", json={"skip_approval": True}, headers=h)
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["success"] is True
    assert body["state"] == "published"


def test_republish_published_roster_is_rejected():
    c = TestClient(app)
    h = _setup_owner(c)
    rid = _seed_roster(c, h)
    c.post(f"/api/v1/rosters/{rid}/publish", json={"skip_approval": True}, headers=h)
    # second publish: roster is already published, not DRAFT -> not success
    r2 = c.post(f"/api/v1/rosters/{rid}/publish", json={"skip_approval": True}, headers=h)
    assert r2.status_code == 200
    assert r2.json()["success"] is False
