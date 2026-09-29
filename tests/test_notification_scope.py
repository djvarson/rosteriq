"""
Notification dispatch must never reach another venue's staff.

The hub resolved "everyone at the venue" with the store's platform-wide
list_employees(), and trusted caller-supplied employee ids — so a venue-A
manager pressing Send to Staff (or the public demo, whose DB role is
manager) messaged every employee on the platform. Targets are now always
bounded by the dispatching venue's own staff.
"""

import uuid

from fastapi.testclient import TestClient

from rosteriq.api import app

PW = "Passw0rd!234"


def _login(c, email):
    c.post("/api/auth/register", json={"email": email, "password": PW, "name": "U"})
    r = c.post("/api/auth/login", json={"email": email, "password": PW})
    return {"Authorization": f"Bearer {r.json()['access_token']}"}


def _venue_with_staff(c, n_staff):
    tag = uuid.uuid4().hex[:6]
    h = _login(c, f"ns_{tag}@x.com")
    vid = f"ns-{tag}"
    assert c.post("/venues", json={"id": vid, "name": vid, "state": "wa", "max_labour_pct": 30,
                                   "tanda_org_id": "", "created_at": "2026-07-01T00:00:00"},
                  headers=h).status_code in (200, 201)
    ids = []
    for i in range(n_staff):
        eid = f"{vid}-e{i}"
        r = c.post("/employees", json={
            "id": eid, "venue_id": vid, "name": f"Staff {i}", "employment_type": "casual",
            "award_level": "level_2", "state": "wa", "hourly_base_rate": "31.50",
            "created_at": "2026-07-01T00:00:00", "updated_at": "2026-07-01T00:00:00"}, headers=h)
        assert r.status_code == 200, r.text
        ids.append(eid)
    return h, vid, ids


def _dispatch(c, h, vid, targets=None):
    body = {"event_type": "ROSTER_PUBLISHED", "venue_id": vid, "payload": {"week": "t"}}
    if targets is not None:
        body["target_employee_ids"] = targets
    return c.post("/api/v1/notifications/dispatch", json=body, headers=h)


def test_send_to_staff_reaches_only_this_venue():
    c = TestClient(app)
    _login(c, f"ns_boot_{uuid.uuid4().hex[:6]}@x.com")   # platform owner out of the way
    a_h, a_vid, a_ids = _venue_with_staff(c, 1)
    _venue_with_staff(c, 2)                                # a neighbour venue with staff

    r = _dispatch(c, a_h, a_vid)
    assert r.status_code == 202, r.text
    assert r.json()["total_targets"] == len(a_ids) == 1


def test_foreign_employee_ids_are_dropped():
    c = TestClient(app)
    _login(c, f"ns_boot_{uuid.uuid4().hex[:6]}@x.com")
    a_h, a_vid, a_ids = _venue_with_staff(c, 1)
    _, _, b_ids = _venue_with_staff(c, 2)

    r = _dispatch(c, a_h, a_vid, targets=a_ids + b_ids)
    assert r.status_code == 202, r.text
    body = r.json()
    assert body["total_targets"] == 1
    assert body["skipped"]["outside_venue"] == 2
