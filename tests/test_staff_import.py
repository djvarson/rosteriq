"""
Staff import from any program's export: header mapping (Deputy/Tanda/sheet),
content inference without a header, delimiter sniffing, extra columns, dedup,
honest per-row skips, and venue scoping.
"""

import uuid

from fastapi.testclient import TestClient

from rosteriq.api import app
from rosteriq.database import get_db


def _owner(c):
    email = f"si{uuid.uuid4().hex[:8]}@x.com"
    c.post("/api/auth/register", json={"email": email, "password": "Passw0rd!234", "name": "O"})
    tok = c.post("/api/auth/login", json={"email": email, "password": "Passw0rd!234"}).json()["access_token"]
    return {"Authorization": f"Bearer {tok}"}


def _venue(c, h, vid):
    c.post("/venues", json={
        "id": vid, "name": "SI Venue", "state": "wa", "max_labour_pct": 30,
        "tanda_org_id": "", "created_at": "2026-07-01T00:00:00",
    }, headers=h)
    return vid


def test_import_with_header_and_extra_columns():
    c = TestClient(app)
    h = _owner(c)
    vid = _venue(c, h, "si-venue-hdr")
    # A realistic export: header, extra columns, quoted field, $ on the rate
    content = (
        "Employee Name,Email,Position,Pay Rate,Employment Type,Deputy ID\n"
        "Emma Thompson,emma@brass.com,Floor,\"$32.50\",Casual,10021\n"
        "James Wilson,james@brass.com,Bar,33.00,Part Time,10022\n"
        "Sarah Chen,sarah@brass.com,Kitchen,34,Full Time,10023\n"
    )
    r = c.post("/api/setup/import-staff", json={"venue_id": vid, "content": content}, headers=h)
    assert r.status_code == 200, r.text
    out = r.json()
    assert out["created_count"] == 3 and out["skipped_count"] == 0

    emps = {e.name: e for e in get_db().get_employees(vid)}
    assert set(emps) == {"Emma Thompson", "James Wilson", "Sarah Chen"}
    assert str(emps["Emma Thompson"].hourly_base_rate) == "32.50"
    assert emps["Emma Thompson"].email == "emma@brass.com"
    assert emps["Emma Thompson"].skills == ["Floor"]
    assert emps["James Wilson"].employment_type.value == "part_time"
    assert emps["Sarah Chen"].employment_type.value == "full_time"


def test_import_tab_delimited_and_first_last_name():
    c = TestClient(app)
    h = _owner(c)
    vid = _venue(c, h, "si-venue-tab")
    content = (
        "First Name\tLast Name\tEmail\tHourly\n"
        "Marcus\tJohnson\tmarcus@x.com\t31.50\n"
    )
    out = c.post("/api/setup/import-staff", json={"venue_id": vid, "content": content}, headers=h).json()
    assert out["created_count"] == 1
    emp = get_db().get_employees(vid)[0]
    assert emp.name == "Marcus Johnson" and emp.email == "marcus@x.com"


def test_import_without_header_infers_columns():
    c = TestClient(app)
    h = _owner(c)
    vid = _venue(c, h, "si-venue-nohdr")
    # No header: name, email (has @), rate (numeric) inferred by content
    content = (
        "Lisa Brown,lisa@x.com,32.00\n"
        "David Miller,dave@x.com,35\n"
        "Priya Nair\n"  # name only -> default rate
    )
    out = c.post("/api/setup/import-staff", json={"venue_id": vid, "content": content}, headers=h).json()
    assert out["created_count"] == 3
    emps = {e.name: e for e in get_db().get_employees(vid)}
    assert str(emps["Lisa Brown"].hourly_base_rate) == "32.00"
    assert str(emps["Priya Nair"].hourly_base_rate) == "31.50"  # default


def test_import_dedupes_and_reports_skips_honestly():
    c = TestClient(app)
    h = _owner(c)
    vid = _venue(c, h, "si-venue-dup")
    content = "Name,Email,Rate\nEmma T,emma@x.com,30\n"
    c.post("/api/setup/import-staff", json={"venue_id": vid, "content": content}, headers=h)

    # Re-import: same email -> skipped (not duplicated); a blank-name row -> skipped
    content2 = "Name,Email,Rate\nEmma T,emma@x.com,30\n,nobody@x.com,25\nNew Person,new@x.com,31\n"
    out = c.post("/api/setup/import-staff", json={"venue_id": vid, "content": content2}, headers=h).json()
    assert out["created_count"] == 1  # only New Person
    reasons = {s.get("reason") for s in out["skipped"]}
    assert "email already exists" in reasons and "no name" in reasons
    assert len(get_db().get_employees(vid)) == 2  # Emma + New Person, no dupe


def test_import_is_venue_scoped():
    c = TestClient(app)
    h = _owner(c)
    vid = _venue(c, h, "si-venue-scope")
    other = f"si{uuid.uuid4().hex[:8]}@x.com"
    c.post("/api/auth/register", json={"email": other, "password": "Passw0rd!234", "name": "S"})
    db = get_db()
    rec = db.get_user_by_email(other)
    rec["venue_ids"] = ["not-this-one"]; rec["role"] = "manager"; db.save_user(rec)
    sh = {"Authorization": f"Bearer {c.post('/api/auth/login', json={'email': other, 'password': 'Passw0rd!234'}).json()['access_token']}"}
    assert c.post("/api/setup/import-staff", json={
        "venue_id": vid, "content": "Name\nX\n"}, headers=sh).status_code == 403
