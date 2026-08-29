"""
POST /venues update path is role-gated (2026-08-29 adversarial review).

The upsert branch used to pass for ANY tenant whose venue_ids contained the
venue — no role check — so a linked role-staff user could overwrite their
venue's whole VenueConfig (min_staff, max_labour_pct, name). Updates now
require manager/owner on top of venue membership; the first-venue bootstrap
create path is unchanged. min_staff keys are also normalised and bounded
because they feed section pills across the UI.
"""

import uuid

from fastapi.testclient import TestClient

from rosteriq.api import app
from rosteriq.database import get_db


def _register_login(c, email):
    c.post("/api/auth/register", json={"email": email, "password": "Passw0rd!234", "name": "U"})
    return {"Authorization": f"Bearer {c.post('/api/auth/login', json={'email': email, 'password': 'Passw0rd!234'}).json()['access_token']}"}


def _set_role(email, role, venue_ids=None):
    db = get_db()
    rec = db.get_user_by_email(email)
    rec["role"] = role
    if venue_ids is not None:
        rec["venue_ids"] = list(venue_ids)
    db.save_user(rec)


def _venue_payload(vid, name="Cafe", **extra):
    p = {
        "id": vid, "name": name, "state": "wa", "max_labour_pct": 30,
        "tanda_org_id": "", "created_at": "2026-07-01T00:00:00",
    }
    p.update(extra)
    return p


def _bootstrap_venue(c):
    """Fresh staff user creates a venue and comes out its manager."""
    email = f"vg{uuid.uuid4().hex[:8]}@x.com"
    h = _register_login(c, email)
    _set_role(email, "staff", venue_ids=[])
    vid = f"vg-venue-{uuid.uuid4().hex[:6]}"
    r = c.post("/venues", json=_venue_payload(vid, name="Original Name"), headers=h)
    assert r.status_code == 200, r.text
    return vid, email, h


def test_linked_staff_cannot_update_held_venue():
    """Role staff + venue membership must NOT be enough to rewrite the config."""
    c = TestClient(app)
    vid, _manager_email, manager_h = _bootstrap_venue(c)

    staff_email = f"vg{uuid.uuid4().hex[:8]}@x.com"
    staff_h = _register_login(c, staff_email)
    _set_role(staff_email, "staff", venue_ids=[vid])  # linked to the venue

    r = c.post("/venues", json=_venue_payload(
        vid, name="Pwned", max_labour_pct=99, min_staff={"kitchen": 999}), headers=staff_h)
    assert r.status_code == 403, f"expected 403, got {r.status_code}: {r.text[:200]}"

    # And nothing was written
    got = c.get(f"/venues/{vid}", headers=manager_h)
    assert got.status_code == 200
    assert got.json()["name"] == "Original Name"
    assert got.json()["max_labour_pct"] == 30


def test_manager_still_updates_held_venue():
    c = TestClient(app)
    vid, email, h = _bootstrap_venue(c)  # bootstrap promoted them to manager

    r = c.post("/venues", json=_venue_payload(
        vid, name="Renamed", max_labour_pct=28, min_staff={"kitchen": 2}), headers=h)
    assert r.status_code == 200, r.text
    got = c.get(f"/venues/{vid}", headers=h).json()
    assert got["name"] == "Renamed" and got["max_labour_pct"] == 28
    assert got["min_staff"] == {"kitchen": 2}


def test_owner_still_updates_any_venue():
    c = TestClient(app)
    vid, _email, _h = _bootstrap_venue(c)

    owner_email = f"vg{uuid.uuid4().hex[:8]}@x.com"
    owner_h = _register_login(c, owner_email)
    _set_role(owner_email, "owner", venue_ids=[])

    r = c.post("/venues", json=_venue_payload(vid, name="Owner Edit"), headers=owner_h)
    assert r.status_code == 200, r.text


def test_create_path_unaffected():
    """A fresh staff user with no venues can still bootstrap their first venue."""
    c = TestClient(app)
    vid, email, _h = _bootstrap_venue(c)
    rec = get_db().get_user_by_email(email)
    assert vid in rec["venue_ids"] and rec["role"] == "manager"


def test_min_staff_keys_normalised():
    """Keys are stripped and lowercased before they hit the section pills."""
    c = TestClient(app)
    vid, _email, h = _bootstrap_venue(c)

    r = c.post("/venues", json=_venue_payload(
        vid, min_staff={"  Kitchen ": 2, "BAR": 1}), headers=h)
    assert r.status_code == 200, r.text
    got = c.get(f"/venues/{vid}", headers=h).json()
    assert got["min_staff"] == {"kitchen": 2, "bar": 1}


def test_min_staff_bad_keys_rejected():
    c = TestClient(app)
    vid, _email, h = _bootstrap_venue(c)

    # Markup in a key (would land in UI pills), over-long keys, absurd counts
    for bad in (
        {"<script>alert(1)</script>": 1},
        {"k" * 41: 1},
        {"kitchen": -1},
        {"kitchen": 100000},
        {"   ": 1},
    ):
        r = c.post("/venues", json=_venue_payload(vid, min_staff=bad), headers=h)
        assert r.status_code == 422, f"{bad} -> {r.status_code}: {r.text[:200]}"
