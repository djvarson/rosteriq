"""
Communication hub: announcement lifecycle with read receipts, honest SMS
reporting when unconfigured, pinning, and venue scoping.
"""

import uuid

from fastapi.testclient import TestClient

from rosteriq.api import app
from rosteriq.database import get_db


def _register_login(c, email):
    c.post("/api/auth/register", json={"email": email, "password": "Passw0rd!234", "name": "U"})
    return {"Authorization": f"Bearer {c.post('/api/auth/login', json={'email': email, 'password': 'Passw0rd!234'}).json()['access_token']}"}


def _setup(c, owner_h, vid, staff_email):
    c.post("/venues", json={
        "id": vid, "name": "Comms Venue", "state": "wa", "max_labour_pct": 30,
        "tanda_org_id": "", "created_at": "2026-07-01T00:00:00",
    }, headers=owner_h)
    c.post("/employees", json={
        "id": f"{vid}-emp", "name": "Reader One", "employment_type": "casual",
        "award_level": "level_2", "state": "wa", "venue_id": vid,
        "hourly_base_rate": "31.50", "email": staff_email,
        "created_at": "2026-07-01T00:00:00", "updated_at": "2026-07-01T00:00:00",
    }, headers=owner_h)


def _scope_staff(c, email, vid):
    db = get_db()
    rec = db.get_user_by_email(email)
    rec["venue_ids"] = [vid]
    rec["role"] = "staff"
    db.save_user(rec)


def test_announcement_lifecycle_and_read_receipts():
    c = TestClient(app)
    owner_h = _register_login(c, f"o{uuid.uuid4().hex[:8]}@x.com")
    staff_email = f"s{uuid.uuid4().hex[:8]}@x.com"
    vid = "cm-venue-1"
    _setup(c, owner_h, vid, staff_email)
    staff_h = _register_login(c, staff_email)
    _scope_staff(c, staff_email, vid)

    r = c.post("/api/announcements", json={
        "venue_id": vid, "title": "New winter menu",
        "body": "Launches Monday — taste session 3pm Sunday.",
    }, headers=owner_h)
    assert r.status_code == 200, r.text
    ann_id = r.json()["announcement_id"]
    assert r.json()["sms_result"] is None  # SMS not requested

    # Staff sees it unread; marks it read; unread count drops
    feed = c.get("/api/me/announcements", headers=staff_h).json()
    assert feed["unread"] == 1 and feed["announcements"][0]["read"] is False
    assert c.post(f"/api/me/announcements/{ann_id}/read", headers=staff_h).status_code == 200
    feed2 = c.get("/api/me/announcements", headers=staff_h).json()
    assert feed2["unread"] == 0 and feed2["announcements"][0]["read"] is True

    # Manager sees the read receipt (1 of 1 staff)
    mgr = c.get(f"/api/announcements?venue_id={vid}", headers=owner_h).json()
    assert mgr["announcements"][0]["read_count"] == 1
    assert mgr["announcements"][0]["staff_count"] == 1

    # Pin floats it; unknown id 404s
    assert c.post(f"/api/announcements/{ann_id}/pin", json={
        "venue_id": vid, "pinned": True}, headers=owner_h).json()["status"] == "pinned"
    assert c.post("/api/announcements/ghost/pin", json={
        "venue_id": vid, "pinned": True}, headers=owner_h).status_code == 404


def test_sms_unconfigured_is_reported_honestly():
    """send_sms=true without Twilio creds must say so — never pretend it sent."""
    c = TestClient(app)
    owner_h = _register_login(c, f"o{uuid.uuid4().hex[:8]}@x.com")
    vid = "cm-venue-sms"
    _setup(c, owner_h, vid, f"s{uuid.uuid4().hex[:8]}@x.com")

    status = c.get(f"/api/sms/status?venue_id={vid}", headers=owner_h).json()
    assert status["configured"] is False  # no creds in test env

    r = c.post("/api/announcements", json={
        "venue_id": vid, "title": "Urgent", "body": "Flood in the cellar",
        "send_sms": True,
    }, headers=owner_h)
    assert r.status_code == 200
    sms = r.json()["sms_result"]
    assert sms["attempted"] is False and sms["sent"] == 0
    assert "not configured" in sms["reason"]


def test_announcements_are_venue_scoped():
    c = TestClient(app)
    owner_h = _register_login(c, f"o{uuid.uuid4().hex[:8]}@x.com")
    vid = "cm-venue-scope"
    _setup(c, owner_h, vid, f"s{uuid.uuid4().hex[:8]}@x.com")

    outsider_email = f"m{uuid.uuid4().hex[:8]}@x.com"
    outsider_h = _register_login(c, outsider_email)
    db = get_db()
    rec = db.get_user_by_email(outsider_email)
    rec["venue_ids"] = ["other-venue"]
    rec["role"] = "manager"
    db.save_user(rec)
    outsider_h = _register_login(c, outsider_email)

    assert c.get(f"/api/announcements?venue_id={vid}", headers=outsider_h).status_code == 403
    assert c.post("/api/announcements", json={
        "venue_id": vid, "title": "Nope", "body": "Nope",
    }, headers=outsider_h).status_code == 403
