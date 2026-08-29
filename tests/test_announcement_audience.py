"""
Announcement audience targeting: a notice for the bar shouldn't ping — or
text — the kitchen.

Pins:
* audience matches employee roles (skills), same rule as procedures'
  applies_to; empty audience = everyone (pre-feature behaviour intact)
* staff feed and unread counts only include announcements addressed to you
* read-receipt denominator counts only targeted staff ("2 of 2 bar read",
  never "2 of 12")
* SMS fan-out texts only the targeted staff
* an audience matching no current staff is a clear 422, not a silent
  message to nobody
* audience strings are normalised (trim/lowercase/dedupe)
* mark-read on an announcement not addressed to you is a 404
"""

import uuid

from fastapi.testclient import TestClient

from rosteriq.api import app
from rosteriq.database import get_db

PW = "Passw0rd!234"


def _login(c, email):
    c.post("/api/auth/register", json={"email": email, "password": PW, "name": "U"})
    tok = c.post("/api/auth/login", json={"email": email, "password": PW}).json()["access_token"]
    return {"Authorization": f"Bearer {tok}"}


def _world():
    """Owner + venue with one bar employee and one kitchen employee."""
    c = TestClient(app)
    tag = uuid.uuid4().hex[:6]
    owner_h = _login(c, f"aud_o_{tag}@x.com")
    vid = f"aud-{tag}"
    c.post("/venues", json={"id": vid, "name": vid, "state": "wa", "max_labour_pct": 30,
                            "tanda_org_id": "", "created_at": "2026-07-01T00:00:00"},
           headers=owner_h)
    emails = {}
    for role, phone in [("bar", "+61400000001"), ("kitchen", "+61400000002")]:
        emails[role] = f"aud_{role}_{tag}@x.com"
        r = c.post("/employees", json={
            "id": f"{vid}-{role}", "name": f"{role.title()} Person",
            "employment_type": "casual", "award_level": "level_2", "state": "wa",
            "venue_id": vid, "hourly_base_rate": "31.50", "email": emails[role],
            "phone": phone, "skills": [role],
            "created_at": "2026-07-01T00:00:00", "updated_at": "2026-07-01T00:00:00",
        }, headers=owner_h)
        assert r.status_code in (200, 201), r.text
    staff_h = {}
    for role in emails:
        staff_h[role] = _login(c, emails[role])
        db = get_db()
        rec = db.get_user_by_email(emails[role])
        rec["venue_ids"] = [vid]
        rec["role"] = "staff"
        db.save_user(rec)
    return c, owner_h, staff_h, vid


def _publish(c, h, vid, **kw):
    body = {"venue_id": vid, "title": "Keg change", "body": "Cellar door blocked til 3.", **kw}
    return c.post("/api/announcements", json=body, headers=h)


def test_targeted_announcement_reaches_only_its_audience():
    c, owner_h, staff_h, vid = _world()
    r = _publish(c, owner_h, vid, audience=[" BAR "])   # normalised
    assert r.status_code == 200, r.text
    ann_id = r.json()["announcement_id"]

    bar = c.get("/api/me/announcements", headers=staff_h["bar"]).json()
    assert bar["count"] == 1 and bar["unread"] == 1
    assert bar["announcements"][0]["audience"] == ["bar"]

    kitchen = c.get("/api/me/announcements", headers=staff_h["kitchen"]).json()
    assert kitchen["count"] == 0 and kitchen["unread"] == 0

    # the kitchen can't receipt a notice that was never theirs
    r = c.post(f"/api/me/announcements/{ann_id}/read", headers=staff_h["kitchen"])
    assert r.status_code == 404


def test_untargeted_announcement_still_reaches_everyone():
    c, owner_h, staff_h, vid = _world()
    assert _publish(c, owner_h, vid).status_code == 200
    for role in ("bar", "kitchen"):
        feed = c.get("/api/me/announcements", headers=staff_h[role]).json()
        assert feed["count"] == 1, role


def test_read_receipt_denominator_counts_only_targeted_staff():
    c, owner_h, staff_h, vid = _world()
    ann_id = _publish(c, owner_h, vid, audience=["bar"]).json()["announcement_id"]
    assert c.post(f"/api/me/announcements/{ann_id}/read",
                  headers=staff_h["bar"]).status_code == 200

    mgr = c.get(f"/api/announcements?venue_id={vid}", headers=owner_h).json()
    a = mgr["announcements"][0]
    assert a["audience"] == ["bar"]
    assert a["read_count"] == 1 and a["staff_count"] == 1  # 1 of 1 BAR, not 1 of 2


def test_audience_matching_nobody_is_a_clear_422():
    c, owner_h, _, vid = _world()
    r = _publish(c, owner_h, vid, audience=["cellar"])
    assert r.status_code == 422
    body = r.json()
    msg = body.get("detail") or body.get("error", {}).get("message", "")
    assert "cellar" in msg


def test_sms_fan_out_texts_only_the_targeted_section(monkeypatch):
    c, owner_h, _, vid = _world()
    texted = []

    class _FakeSms:
        is_configured = True

        async def send_detailed(self, phone, text):
            texted.append(phone)
            return True, "sent"

        def status(self):
            return {"configured": True, "provider": "fake"}

    import rosteriq.routes.comms as comms
    monkeypatch.setattr(comms, "get_sms_service", lambda: _FakeSms())

    r = _publish(c, owner_h, vid, send_sms=True, audience=["bar"])
    assert r.status_code == 200, r.text
    sms = r.json()["sms_result"]
    assert sms["attempted"] is True and sms["sent"] == 1
    assert texted == ["+61400000001"]  # the bar phone only


def test_venue_listing_hides_other_sections_notices_from_staff():
    """The manager listing is readable by staff (the demo account is one) —
    it must show them the same world their /my feed does."""
    c, owner_h, staff_h, vid = _world()
    _publish(c, owner_h, vid, audience=["bar"])
    _publish(c, owner_h, vid, title="All hands", body="Xmas rush briefing Tuesday.")

    mgr = c.get(f"/api/announcements?venue_id={vid}", headers=owner_h).json()
    assert mgr["count"] == 2  # managers see everything

    kitchen = c.get(f"/api/announcements?venue_id={vid}", headers=staff_h["kitchen"]).json()
    assert kitchen["count"] == 1
    assert kitchen["announcements"][0]["title"] == "All hands"

    bar = c.get(f"/api/announcements?venue_id={vid}", headers=staff_h["bar"]).json()
    assert bar["count"] == 2
    assert all(a["sms_result"] is None for a in bar["announcements"])
