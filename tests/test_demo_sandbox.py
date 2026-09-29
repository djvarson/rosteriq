"""
The public Try Demo is one shared sandbox anyone on the internet can drive.

* It must not create venues: an id it creates is squatted for the real
  venue whose name slugs to it, and every record it creates is shared.
* It must not add or edit staff records (a planted name + phone number is
  also an SMS target once texting is live).
* The demo venue never sends anything outward — no email, SMS or push, only
  in-app — so nobody can use the demo to message a real person.
"""

import asyncio
from unittest.mock import AsyncMock, MagicMock

from fastapi.testclient import TestClient

from rosteriq.api import app
from rosteriq.services.demo import DEMO_VENUE_ID, is_demo_identity
from rosteriq.services.notification_hub import NotificationEventType, NotificationHub


def _demo(c):
    c.post("/api/auth/register", json={"email": "sandbox_boot@x.com", "password": "Passw0rd!234", "name": "B"})
    r = c.post("/api/auth/demo")
    assert r.status_code == 200, r.text
    return {"Authorization": f"Bearer {r.json()['access_token']}"}


def test_demo_cannot_create_venues():
    c = TestClient(app)
    h = _demo(c)
    r = c.post("/venues", json={"id": "the-brass-monkey", "name": "The Brass Monkey", "state": "wa",
                                "max_labour_pct": 30, "tanda_org_id": "",
                                "created_at": "2026-07-01T00:00:00"}, headers=h)
    assert r.status_code == 403, r.text


def test_demo_cannot_add_staff():
    c = TestClient(app)
    h = _demo(c)
    r = c.post("/employees", json={
        "id": "demo-planted", "venue_id": DEMO_VENUE_ID, "name": "Planted Name",
        "employment_type": "casual", "award_level": "level_2", "state": "wa",
        "hourly_base_rate": "31.50", "phone": "+61400000000",
        "created_at": "2026-07-01T00:00:00", "updated_at": "2026-07-01T00:00:00"}, headers=h)
    assert r.status_code == 403, r.text


def test_demo_announcement_never_texts():
    c = TestClient(app)
    h = _demo(c)
    r = c.post("/api/announcements", json={"venue_id": DEMO_VENUE_ID, "title": "t", "body": "b",
                                           "send_sms": True}, headers=h)
    assert r.status_code == 200, r.text
    body = r.json()
    sms = body.get("sms_result") or body.get("announcement", {}).get("sms_result") or {}
    assert sms and sms.get("attempted") is False and "demo" in sms.get("reason", ""), body


def test_demo_venue_dispatch_is_in_app_only():
    async def run():
        hub = NotificationHub()
        emp = MagicMock(id="demo-staff-001", email="someone@real.example", phone="+61400000000")
        hub._db = MagicMock()
        hub._db.get_employees = MagicMock(return_value=[emp])
        hub._db.get_employee = MagicMock(return_value=emp)
        hub._prefs_service = MagicMock()
        hub._prefs_service.get_preferences = MagicMock(return_value={
            "channels": {"email": True, "sms": True, "push": True},
            "notification_types": {}, "quiet_hours": {"enabled": False}})
        hub._prefs_service.is_in_quiet_hours = MagicMock(return_value=False)
        hub._send_email_notification = AsyncMock(return_value=True)
        hub._send_sms_notification = AsyncMock(return_value=True)
        hub._send_push_notification = AsyncMock(return_value=True)
        await hub.dispatch(NotificationEventType.ROSTER_PUBLISHED, DEMO_VENUE_ID, {"week": "w"})
        hub._send_email_notification.assert_not_called()
        hub._send_sms_notification.assert_not_called()
        hub._send_push_notification.assert_not_called()
    asyncio.run(run())


def test_demo_identity_helper():
    assert is_demo_identity("demo-user") and is_demo_identity("demo-staff-user")
    assert is_demo_identity(email="Demo@RosterIQ.app")
    assert not is_demo_identity("someone-else", "someone@example.org")


# ---------------------------------------------------------------------------
# Auth exemption for OAuth callbacks is an explicit list, not a suffix match
# ---------------------------------------------------------------------------

def test_real_oauth_callbacks_stay_public():
    from rosteriq.middleware.tenant import OAUTH_CALLBACK_PATHS, TenantMiddleware
    for p in OAUTH_CALLBACK_PATHS:
        assert TenantMiddleware._is_exempt(p), p


def test_callback_valued_path_param_is_not_exempt():
    from rosteriq.middleware.tenant import TenantMiddleware
    for p in ("/api/analytics/summary/callback", "/api/v1/venues/callback", "/employees/callback"):
        assert not TenantMiddleware._is_exempt(p), p
    c = TestClient(app)
    r = c.get("/api/analytics/summary/callback")
    assert r.status_code == 401, (r.status_code, r.text[:200])


# ---------------------------------------------------------------------------
# Demo side effects are refused as a class (middleware), not endpoint by endpoint
# ---------------------------------------------------------------------------

def test_demo_cannot_reach_outbound_or_integration_writes():
    c = TestClient(app)
    h = _demo(c)
    attempts = [
        ("POST", "/api/notifications/test", {"email": "outsider@example.org"}),
        ("POST", "/api/notifications/test-sms", {"phone": "+61400000000"}),
        ("POST", f"/api/notifications/digest/{DEMO_VENUE_ID}?manager_email=outsider@example.org", {}),
        ("POST", "/api/push/subscribe", {"endpoint": "https://push.example/x", "keys": {"p256dh": "a", "auth": "b"}}),
        ("POST", f"/api/push/broadcast/{DEMO_VENUE_ID}", {"title": "t", "body": "b"}),
        ("POST", "/api/push/test", {"title": "t", "body": "b"}),
        ("POST", "/api/deputy/install-token", {"venue_id": DEMO_VENUE_ID, "access_token": "x", "subdomain": "y"}),
        ("POST", "/api/setup/import-staff", {"venue_id": DEMO_VENUE_ID, "content": "Name,Email\nX,x@example.org"}),
        ("POST", "/employees/bulk", [{"id": "b1", "venue_id": DEMO_VENUE_ID, "name": "Bulk"}]),
        ("PUT", "/api/staff/profile", {"name": "Renamed"}),
        ("PUT", f"/venues/{DEMO_VENUE_ID}", {"name": "Renamed venue"}),
    ]
    for method, path, body in attempts:
        r = c.request(method, path, json=body, headers=h)
        assert r.status_code == 403, (method, path, r.status_code, r.text[:160])


def test_demo_showcase_writes_still_work():
    c = TestClient(app)
    h = _demo(c)
    r = c.post("/rosters/generate", json={"venue_id": DEMO_VENUE_ID, "week_start": "2026-10-05"}, headers=h)
    assert r.status_code == 200, r.text[:200]


def test_resend_verification_sends_nothing():
    c = TestClient(app)
    h = _demo(c)
    assert c.post("/api/auth/resend-verification", headers=h).status_code == 501


def test_digest_reads_are_venue_gated():
    c = TestClient(app)
    c.post("/api/auth/register", json={"email": "dg_boot@x.com", "password": "Passw0rd!234", "name": "B"})
    def mgr(tag):
        e = f"dg_{tag}@x.com"
        c.post("/api/auth/register", json={"email": e, "password": "Passw0rd!234", "name": "M"})
        h = {"Authorization": "Bearer " + c.post("/api/auth/login", json={"email": e, "password": "Passw0rd!234"}).json()["access_token"]}
        c.post("/venues", json={"id": f"dg-{tag}", "name": f"Private {tag}", "state": "wa", "max_labour_pct": 30,
                                "tanda_org_id": "", "created_at": "2026-07-01T00:00:00"}, headers=h)
        return {"Authorization": "Bearer " + c.post("/api/auth/login", json={"email": e, "password": "Passw0rd!234"}).json()["access_token"]}
    a, _ = mgr("a"), mgr("b")
    assert c.get("/api/v1/venues/dg-b/digest/preview", headers=a).status_code == 403
    assert c.get("/api/v1/venues/dg-b/digest/history", headers=a).status_code == 403
    assert c.get("/api/v1/portfolio/digest?venue_ids=dg-a,dg-b", headers=a).status_code == 403


# ---------------------------------------------------------------------------
# Integration syncs never overwrite another venue's staff record
# ---------------------------------------------------------------------------

def test_synced_employee_never_overwrites_another_venues_record():
    from rosteriq.database import get_db
    from rosteriq.models import Employee
    from rosteriq.services.visa import save_synced_employee
    db = get_db()
    victim = Employee(id="deputy-1", venue_id="venue-victim", name="Victim Staff", employment_type="casual",
                      award_level="level_2", state="wa", hourly_base_rate="31.50",
                      visa_status="student", created_at="2026-07-01T00:00:00", updated_at="2026-07-01T00:00:00")
    db.save_employee(victim)
    incoming = Employee(id="deputy-1", venue_id="venue-attacker", name="Overwrite Attempt", employment_type="casual",
                        award_level="level_2", state="wa", hourly_base_rate="1.00",
                        created_at="2026-07-01T00:00:00", updated_at="2026-07-01T00:00:00")
    assert save_synced_employee(db, incoming) is False
    kept = db.get_employee("deputy-1")
    assert kept.venue_id == "venue-victim" and kept.name == "Victim Staff"
    assert incoming.visa_status is None          # no visa echo from the other venue
    # same-venue re-sync still upserts and keeps the recorded visa
    again = Employee(id="deputy-1", venue_id="venue-victim", name="Victim Renamed", employment_type="casual",
                     award_level="level_2", state="wa", hourly_base_rate="31.50",
                     created_at="2026-07-01T00:00:00", updated_at="2026-07-01T00:00:00")
    assert save_synced_employee(db, again) is True
    assert db.get_employee("deputy-1").name == "Victim Renamed"
    assert db.get_employee("deputy-1").visa_status == "student"
