"""
Compliance checklists: seed defaults -> today's board -> run lifecycle with
temperature flagging -> completion guard -> history evidence trail.
"""

import uuid
from datetime import date

from fastapi.testclient import TestClient

from rosteriq.api import app


def _owner(c):
    email = f"ck{uuid.uuid4().hex[:8]}@x.com"
    c.post("/api/auth/register", json={"email": email, "password": "Passw0rd!234", "name": "O"})
    tok = c.post("/api/auth/login", json={"email": email, "password": "Passw0rd!234"}).json()["access_token"]
    return {"Authorization": f"Bearer {tok}"}


def _venue(c, h, vid):
    c.post("/venues", json={
        "id": vid, "name": "CK Venue", "state": "wa", "max_labour_pct": 30,
        "tanda_org_id": "", "created_at": "2026-07-01T00:00:00",
    }, headers=h)
    return vid


def test_seed_is_idempotent_and_lists():
    c = TestClient(app)
    h = _owner(c)
    vid = _venue(c, h, "ck-venue-seed")

    r = c.post("/api/checklists/templates/seed", json={"venue_id": vid}, headers=h)
    assert r.status_code == 200
    assert set(r.json()["created"]) == {"Opening", "Closing", "Food Safety (daily)"}

    # Second seed creates nothing new
    r2 = c.post("/api/checklists/templates/seed", json={"venue_id": vid}, headers=h)
    assert r2.json()["created"] == []

    tpls = c.get(f"/api/checklists/templates?venue_id={vid}", headers=h).json()
    assert tpls["count"] == 3


def test_run_lifecycle_with_temp_flagging():
    c = TestClient(app)
    h = _owner(c)
    vid = _venue(c, h, "ck-venue-run")
    c.post("/api/checklists/templates/seed", json={"venue_id": vid}, headers=h)

    board = c.get(f"/api/checklists/today?venue_id={vid}", headers=h).json()
    opening = [b for b in board["checklists"] if b["name"] == "Opening"][0]
    assert opening["status"] == "not_started"

    # Start the Opening run
    start = c.post("/api/checklists/runs/start",
                   json={"venue_id": vid, "template_id": opening["template_id"]}, headers=h)
    assert start.status_code == 200
    run = start.json()["run"]
    run_id = run["id"]

    # Starting again resumes, doesn't duplicate
    again = c.post("/api/checklists/runs/start",
                   json={"venue_id": vid, "template_id": opening["template_id"]}, headers=h)
    assert again.json()["status"] == "already_started"
    assert again.json()["run_id"] == run_id

    # A temp item without a reading -> 422
    temp_item = [i for i in run["items"] if i["type"] == "temp"][0]
    bad = c.post(f"/api/checklists/runs/{run_id}/item",
                 json={"venue_id": vid, "item_id": temp_item["id"], "done": True}, headers=h)
    assert bad.status_code == 422

    # Fridge at 9°C (limit 5) -> FLAGGED, not hidden
    warm = c.post(f"/api/checklists/runs/{run_id}/item",
                  json={"venue_id": vid, "item_id": temp_item["id"], "done": True, "value": 9.0},
                  headers=h)
    assert warm.status_code == 200
    assert warm.json()["flagged"] is True and warm.json()["flags_count"] == 1

    # Completing with items still undone -> 422 naming them
    inc = c.post(f"/api/checklists/runs/{run_id}/complete", json={"venue_id": vid}, headers=h)
    assert inc.status_code == 422

    # Finish every remaining item (temps get in-range readings)
    for i in run["items"]:
        if i["id"] == temp_item["id"]:
            continue
        payload = {"venue_id": vid, "item_id": i["id"], "done": True}
        if i["type"] == "temp":
            payload["value"] = (i.get("target_min") or 0 + 0)  # in-range: use lower bound
            payload["value"] = i.get("target_min") if i.get("target_min") is not None else 0
        c.post(f"/api/checklists/runs/{run_id}/item", json=payload, headers=h)

    done = c.post(f"/api/checklists/runs/{run_id}/complete", json={"venue_id": vid}, headers=h)
    assert done.status_code == 200, done.text
    assert done.json()["flags_count"] == 1  # the warm fridge stays on record

    # Double completion -> 409
    dbl = c.post(f"/api/checklists/runs/{run_id}/complete", json={"venue_id": vid}, headers=h)
    assert dbl.status_code == 409

    # History preserves the flagged reading as evidence
    today = date.today()
    hist = c.get(f"/api/checklists/runs?venue_id={vid}&start_date={today}&end_date={today}",
                 headers=h).json()
    assert hist["total_flags"] == 1
    flagged = hist["runs"][0]["flagged_items"]
    assert flagged and flagged[0]["value"] == 9.0

    # Board shows complete
    board2 = c.get(f"/api/checklists/today?venue_id={vid}", headers=h).json()
    opening2 = [b for b in board2["checklists"] if b["name"] == "Opening"][0]
    assert opening2["status"] == "complete" and opening2["flags_count"] == 1


def test_checklists_are_venue_scoped():
    c = TestClient(app)
    h = _owner(c)
    vid = _venue(c, h, "ck-venue-scope")
    c.post("/api/checklists/templates/seed", json={"venue_id": vid}, headers=h)

    from rosteriq.database import get_db
    other_email = f"ck{uuid.uuid4().hex[:8]}@x.com"
    c.post("/api/auth/register", json={"email": other_email, "password": "Passw0rd!234", "name": "S"})
    db = get_db()
    rec = db.get_user_by_email(other_email)
    rec["venue_ids"] = ["someone-elses-venue"]
    rec["role"] = "manager"
    db.save_user(rec)
    sh = {"Authorization": f"Bearer {c.post('/api/auth/login', json={'email': other_email, 'password': 'Passw0rd!234'}).json()['access_token']}"}

    assert c.get(f"/api/checklists/templates?venue_id={vid}", headers=sh).status_code == 403
    assert c.post("/api/checklists/templates/seed", json={"venue_id": vid}, headers=sh).status_code == 403
