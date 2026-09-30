"""
Admin backups must be downloadable: they are written to the container's own
disk, which Railway wipes on every deploy — a copy that isn't downloaded is
gone at the next ship. Owner only, and only real backup ids.
"""

import gzip
import json
import uuid

from fastapi.testclient import TestClient

from rosteriq.api import app

PW = "Passw0rd!234"


def _login(c, email):
    c.post("/api/auth/register", json={"email": email, "password": PW, "name": "U"})
    r = c.post("/api/auth/login", json={"email": email, "password": PW})
    return {"Authorization": f"Bearer {r.json()['access_token']}"}


def test_owner_creates_and_downloads_a_backup(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)                      # backups land in ./backups
    import rosteriq.services.backup as backup_mod
    monkeypatch.setattr(backup_mod, "_backup_service_instance", None)
    c = TestClient(app)
    owner = _login(c, f"bk_owner_{uuid.uuid4().hex[:6]}@x.com")   # first user = platform owner
    r = c.post("/api/v1/admin/backup", json={"backup_type": "full"}, headers=owner)
    assert r.status_code == 200, r.text
    bid = r.json()["id"]
    r = c.get(f"/api/v1/admin/backups/{bid}/download", headers=owner)
    assert r.status_code == 200, r.text[:200]
    assert r.headers["content-type"].startswith("application/gzip")
    assert "attachment" in r.headers.get("content-disposition", "")
    json.loads(gzip.decompress(r.content))          # a real, readable backup


def test_download_is_owner_only_and_id_checked():
    c = TestClient(app)
    owner = _login(c, f"bk_boot_{uuid.uuid4().hex[:6]}@x.com")   # first user = platform owner
    mgr = _login(c, f"bk_mgr_{uuid.uuid4().hex[:6]}@x.com")
    assert c.get("/api/v1/admin/backups/backup_20260101_000000_000000/download",
                  headers=mgr).status_code in (401, 403)
    # even the owner can only fetch real backup ids — never an arbitrary path
    for bad in ("..%2F..%2Fetc%2Fpasswd", "..", "backup_x", "not-a-backup",
                "backup_20260101_000000_000000.json"):
        r = c.get(f"/api/v1/admin/backups/{bad}/download", headers=owner)
        assert r.status_code == 404, (bad, r.status_code)


def test_legacy_staff_portal_sends_people_to_the_staff_app():
    """static/staff.html was a superseded staff portal full of dead controls;
    /my is the maintained staff app. Old links and bookmarks go there."""
    c = TestClient(app)
    r = c.get("/staff", follow_redirects=False)
    assert r.status_code in (301, 302, 307, 308) and r.headers["location"] == "/my"
    r = c.get("/static/staff.html")
    assert r.status_code == 200 and "url=/my" in r.text and "location.replace('/my')" in r.text
