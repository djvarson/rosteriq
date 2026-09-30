"""
Handover notes are venue-private.

GET /api/v1/shifts/{id}/handover had no gate, the three list endpoints
(by venue+date, unacknowledged, an employee's incoming) never checked the
venue, and create trusted the venue_id in the body rather than the shift's
own venue. Any signed-in user could read — or file notes against — another
venue's shifts.
"""

import uuid

from fastapi.testclient import TestClient

from rosteriq.api import app
from rosteriq.database import get_db

PW = "Passw0rd!234"


def _login(c, email):
    c.post("/api/auth/register", json={"email": email, "password": PW, "name": "U"})
    r = c.post("/api/auth/login", json={"email": email, "password": PW})
    return {"Authorization": f"Bearer {r.json()['access_token']}"}


def _venue_with_roster(c, tag):
    email = f"hs_{tag}_{uuid.uuid4().hex[:6]}@x.com"
    h = _login(c, email)
    vid = f"hs-{tag}-{uuid.uuid4().hex[:6]}"
    assert c.post("/venues", json={"id": vid, "name": vid, "state": "wa", "max_labour_pct": 30,
                                   "tanda_org_id": "", "created_at": "2026-07-01T00:00:00"},
                  headers=h).status_code in (200, 201)
    h = {"Authorization": f"Bearer {c.post('/api/auth/login', json={'email': email, 'password': PW}).json()['access_token']}"}
    assert c.post("/employees", json={
        "id": f"{vid}-e0", "venue_id": vid, "name": f"Staff {tag}", "employment_type": "casual",
        "award_level": "level_2", "state": "wa", "hourly_base_rate": "31.50",
        "created_at": "2026-07-01T00:00:00", "updated_at": "2026-07-01T00:00:00"}, headers=h).status_code == 200
    r = c.post("/rosters/generate", json={"venue_id": vid, "week_start": "2026-10-05"}, headers=h)
    assert r.status_code == 200, r.text
    db = get_db()
    sh = db.get_roster(r.json()["id"]).shifts[0]
    if hasattr(db, "_shifts"):          # MemoryStore keeps roster shifts inline; PG has a shifts table
        db._shifts[sh.id] = sh
    return h, vid, {"id": sh.id, "date": sh.date.isoformat()}


def _note(vid, author):
    return {"venue_id": vid, "author_id": author, "author_name": "Author",
            "general_notes": {"text": "Keg on line 3 is nearly out"}, "priority": "normal"}


def test_handover_notes_are_venue_private(monkeypatch):
    import rosteriq.services.handover_notes as hn
    monkeypatch.setattr(hn, "_service_instance", None)   # singleton pins the store it was built with
    c = TestClient(app)
    _login(c, f"hs_boot_{uuid.uuid4().hex[:6]}@x.com")          # platform owner out of the way
    a_h, a_vid, a_shift = _venue_with_roster(c, "a")
    b_h, b_vid, b_shift = _venue_with_roster(c, "b")

    r = c.post(f"/api/v1/shifts/{b_shift['id']}/handover", json=_note(b_vid, "b-mgr"), headers=b_h)
    assert r.status_code == 201, r.text
    day = b_shift["date"]

    # A can't read B's note by shift id, nor list B's notes
    assert c.get(f"/api/v1/shifts/{b_shift['id']}/handover", headers=a_h).status_code == 404
    assert c.get(f"/api/v1/venues/{b_vid}/handovers/{day}", headers=a_h).status_code == 403
    assert c.get(f"/api/v1/venues/{b_vid}/handovers/unacknowledged", headers=a_h).status_code == 403
    assert c.get(f"/api/v1/employees/{b_vid}-e0/incoming-handovers?venue_id={b_vid}",
                 headers=a_h).status_code == 403
    # A can't file a note against B's shift by naming its own venue
    r = c.post(f"/api/v1/shifts/{b_shift['id']}/handover", json=_note(a_vid, "a-mgr"), headers=a_h)
    assert r.status_code == 404, r.text

    # B still reads its own note
    r = c.get(f"/api/v1/shifts/{b_shift['id']}/handover", headers=b_h)
    assert r.status_code == 200 and "Keg on line 3" in r.text, r.text
    assert c.get(f"/api/v1/venues/{b_vid}/handovers/{day}", headers=b_h).status_code == 200
    # ...and its unacknowledged list (this route used to be swallowed by /{date} -> 400)
    r = c.get(f"/api/v1/venues/{b_vid}/handovers/unacknowledged", headers=b_h)
    assert r.status_code == 200 and isinstance(r.json(), list), r.text
