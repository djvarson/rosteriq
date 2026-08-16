"""
Event log read API (/api/events): owner platform-wide view, manager scoping,
category / since filters, summary counts, secret scrubbing, and the admin
logs endpoint reading DB events.
"""

import uuid
from datetime import datetime, timedelta

from fastapi.testclient import TestClient

from rosteriq.api import app
from rosteriq.database import get_db
from rosteriq.services.events import audit, security, error, record_event

PW = "Passw0rd!234"


def _register_login(c, email):
    c.post("/api/auth/register", json={"email": email, "password": PW, "name": "U"})
    tok = c.post("/api/auth/login", json={"email": email, "password": PW}).json()["access_token"]
    return {"Authorization": f"Bearer {tok}"}


def _venue(c, owner_h, vid):
    r = c.post("/venues", json={
        "id": vid, "name": "Events Venue", "state": "wa", "max_labour_pct": 30,
        "tanda_org_id": "", "created_at": "2026-07-01T00:00:00",
    }, headers=owner_h)
    assert r.status_code in (200, 201), r.text


def _manager_at(c, email, vid):
    h = _register_login(c, email)
    db = get_db()
    rec = db.get_user_by_email(email)
    rec["role"] = "manager"
    rec["venue_ids"] = [vid]
    db.save_user(rec)
    return h


def _world():
    """Owner + two venues + a manager scoped to venue A. Seeds a few events."""
    c = TestClient(app)
    owner_email = f"o{uuid.uuid4().hex[:8]}@x.com"
    owner_h = _register_login(c, owner_email)
    va = f"ev-a-{uuid.uuid4().hex[:6]}"
    vb = f"ev-b-{uuid.uuid4().hex[:6]}"
    _venue(c, owner_h, va)
    _venue(c, owner_h, vb)
    mgr_email = f"m{uuid.uuid4().hex[:8]}@x.com"
    mgr_h = _manager_at(c, mgr_email, va)
    db = get_db()
    owner = db.get_user_by_email(owner_email)
    mgr = db.get_user_by_email(mgr_email)
    # Seed directly through the spine (no request context -> user_id passed explicitly)
    record_event("audit", "timesheet.approve", venue_id=va, resource_type="timesheet",
                 resource_id="ts-1", user_id=owner["id"],
                 details={"employee_name": "Ben", "hours": 7.5}, db=db)
    record_event("audit", "roster.publish", venue_id=va, resource_type="roster",
                 resource_id="r-1", user_id=mgr["id"], details={"week": "2026-08-10"}, db=db)
    record_event("security", "auth.login_failed", venue_id=None, outcome="denied",
                 details={"email": "someone@x.com"}, db=db)
    record_event("error", "xero.bill_push", venue_id=vb, outcome="error",
                 details={"exception": "TimeoutError", "message": "boom"}, db=db)
    record_event("audit", "sop.publish", venue_id=vb, resource_type="sop",
                 resource_id="s-1", user_id=owner["id"], details={"title": "Fire evac"}, db=db)
    return c, owner_h, mgr_h, va, vb, owner, mgr


def test_owner_platform_wide_list_and_shape():
    c, owner_h, _, va, vb, owner, mgr = _world()
    r = c.get("/api/events", headers=owner_h)
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["count"] >= 5 and body["venue_id"] is None
    actions = {e["action"] for e in body["events"]}
    assert {"timesheet.approve", "roster.publish", "auth.login_failed", "xero.bill_push", "sop.publish"} <= actions
    ts = next(e for e in body["events"] if e["action"] == "timesheet.approve")
    assert ts["venue_id"] == va and ts["category"] == "audit" and ts["outcome"] == "ok"
    assert ts["user_id"] == owner["id"] and ts["user_name"] == "U"
    assert ts["resource_type"] == "timesheet" and ts["resource_id"] == "ts-1"
    assert ts["details"] == {"employee_name": "Ben", "hours": 7.5}  # meta keys stripped
    assert isinstance(ts["created_at"], str) and "correlation_id" in ts
    err = next(e for e in body["events"] if e["action"] == "xero.bill_push")
    assert err["category"] == "error" and err["outcome"] == "error" and err["venue_id"] == vb
    # newest first
    stamps = [e["created_at"] for e in body["events"]]
    assert stamps == sorted(stamps, reverse=True)


def test_owner_can_scope_to_one_venue():
    c, owner_h, _, va, vb, _, _ = _world()
    r = c.get(f"/api/events?venue_id={vb}", headers=owner_h)
    assert r.status_code == 200
    assert {e["venue_id"] for e in r.json()["events"]} == {vb}
    assert {e["action"] for e in r.json()["events"]} == {"xero.bill_push", "sop.publish"}


def test_manager_scoped_to_own_venue():
    c, _, mgr_h, va, vb, _, _ = _world()
    ok = c.get(f"/api/events?venue_id={va}", headers=mgr_h)
    assert ok.status_code == 200, ok.text
    assert {e["venue_id"] for e in ok.json()["events"]} == {va}
    assert {e["action"] for e in ok.json()["events"]} == {"timesheet.approve", "roster.publish"}
    # other venue -> 403
    assert c.get(f"/api/events?venue_id={vb}", headers=mgr_h).status_code == 403
    # no venue -> 403 for non-owner (platform-wide is owner-only)
    assert c.get("/api/events", headers=mgr_h).status_code == 403
    assert c.get("/api/events/summary", headers=mgr_h).status_code == 403
    assert c.get(f"/api/events/summary?venue_id={vb}", headers=mgr_h).status_code == 403


def test_unauthenticated_is_rejected():
    c = TestClient(app)
    assert c.get("/api/events").status_code == 401
    assert c.get("/api/events/summary").status_code == 401


def test_category_and_prefix_filters():
    c, owner_h, _, va, vb, _, _ = _world()
    sec = c.get("/api/events?category=security", headers=owner_h).json()
    assert sec["count"] >= 1 and all(e["category"] == "security" for e in sec["events"])
    assert any(e["action"] == "auth.login_failed" for e in sec["events"])
    errs = c.get("/api/events?category=error", headers=owner_h).json()
    assert {e["action"] for e in errs["events"]} == {"xero.bill_push"}
    aud = c.get(f"/api/events?venue_id={va}&category=audit", headers=owner_h).json()
    assert {e["action"] for e in aud["events"]} == {"timesheet.approve", "roster.publish"}
    pre = c.get("/api/events?action_prefix=timesheet.", headers=owner_h).json()
    assert pre["count"] == 1 and pre["events"][0]["action"] == "timesheet.approve"
    assert c.get("/api/events?category=bogus", headers=owner_h).status_code == 422


def test_since_filter_and_limit_cap():
    c, owner_h, _, va, _, owner, _ = _world()
    db = get_db()
    # An old row that must be excluded by since=24h
    old = datetime.utcnow() - timedelta(days=3)
    db.save_audit_log({"venue_id": va, "user_id": owner["id"], "action": "employee.update",
                       "resource_type": "employee", "resource_id": "e-old",
                       "details": {"category": "audit", "outcome": "ok"}, "created_at": old})
    recent = c.get(f"/api/events?venue_id={va}&since=24h", headers=owner_h).json()
    assert "employee.update" not in {e["action"] for e in recent["events"]}
    week = c.get(f"/api/events?venue_id={va}&since=7d", headers=owner_h).json()
    assert "employee.update" in {e["action"] for e in week["events"]}
    iso = (datetime.utcnow() - timedelta(days=5)).isoformat()
    week2 = c.get(f"/api/events?venue_id={va}&since={iso}", headers=owner_h).json()
    assert "employee.update" in {e["action"] for e in week2["events"]}
    assert c.get("/api/events?since=garbage", headers=owner_h).status_code == 422
    capped = c.get("/api/events?limit=9999", headers=owner_h).json()
    assert capped["limit"] == 500


def test_summary_counts():
    c, owner_h, mgr_h, va, vb, _, _ = _world()
    s = c.get("/api/events/summary?days=7", headers=owner_h).json()
    assert s["days"] == 7 and s["venue_id"] is None
    assert s["by_category"]["audit"] >= 3
    assert s["by_category"]["security"] >= 1
    assert s["by_category"]["error"] >= 1
    assert s["total"] == sum(s["by_category"].values())
    top = {t["action"]: t["count"] for t in s["top_actions"]}
    assert top["timesheet.approve"] == 1 and len(s["top_actions"]) <= 10
    m = c.get(f"/api/events/summary?venue_id={va}", headers=mgr_h).json()
    assert m["venue_id"] == va and m["by_category"] == {"audit": 2, "security": 0, "error": 0}
    assert m["total"] == 2


def test_details_never_contain_secrets():
    c, owner_h, _, va, _, owner, _ = _world()
    db = get_db()
    record_event("audit", "xero.connect", venue_id=va, user_id=owner["id"],
                 details={"token": "abc-super-secret", "password": "hunter2",
                          "tenant_name": "Demo Org"}, db=db)
    r = c.get(f"/api/events?venue_id={va}&action_prefix=xero.", headers=owner_h).json()
    ev = r["events"][0]
    assert ev["action"] == "xero.connect"
    assert ev["details"]["token"] == "[redacted]"
    assert ev["details"]["password"] == "[redacted]"
    assert ev["details"]["tenant_name"] == "Demo Org"
    # DB row itself is scrubbed too (not just the API view)
    rows = db.list_events(venue_id=va, action_prefix="xero.")
    assert rows[0]["details"]["token"] == "[redacted]"
    assert rows[0]["details"]["category"] == "audit" and rows[0]["user_id"] == owner["id"]


def test_wrappers_land_in_api():
    c, owner_h, _, va, _, owner, _ = _world()
    db = get_db()
    audit("employee.rate_change", va, "employee", "e-1", employee_name="Ava", before="30.00", after="32.50")
    security("auth.locked", venue_id=va, email="ava@x.com")
    error("payroll.batch_create", RuntimeError("bad batch"), venue_id=va)
    r = c.get(f"/api/events?venue_id={va}", headers=owner_h).json()
    by = {e["action"]: e for e in r["events"]}
    assert by["employee.rate_change"]["details"] == {"employee_name": "Ava", "before": "30.00", "after": "32.50"}
    assert by["auth.locked"]["category"] == "security" and by["auth.locked"]["outcome"] == "denied"
    assert by["payroll.batch_create"]["category"] == "error"
    assert by["payroll.batch_create"]["details"]["exception"] == "RuntimeError"


def test_admin_logs_endpoint_reads_db_events():
    c, owner_h, _, va, _, _, _ = _world()
    r = c.get("/api/v1/admin/logs?limit=50", headers=owner_h)
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["source"] == "db+buffer"
    assert isinstance(body["events"], list) and isinstance(body["buffer"], list)
    assert body["count"] >= 5
    assert any(e["action"] == "timesheet.approve" for e in body["events"])
    assert all(e["source"] == "db" for e in body["events"])
    # category filter passes through
    sec = c.get("/api/v1/admin/logs?category=security", headers=owner_h).json()
    assert all(e["category"] == "security" for e in sec["events"])
    # no auth -> 401
    assert c.get("/api/v1/admin/logs").status_code == 401
