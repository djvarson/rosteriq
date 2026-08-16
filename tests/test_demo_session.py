"""
Sandboxed demo session: "Try Demo" mints a scoped token for a seeded demo user
so the demo experience — including the auth-gated AI agent — works without a
real account, while the demo user is locked to the demo venue only.
"""

from fastapi.testclient import TestClient

from rosteriq.api import app
from rosteriq.database import get_db, MemoryStore
from rosteriq.services.demo import (
    seed_demo_environment, DEMO_USER_ID, DEMO_USER_EMAIL, DEMO_VENUE_ID,
)


def test_demo_endpoint_mints_token_and_seeds():
    c = TestClient(app)
    r = c.post("/api/auth/demo")
    assert r.status_code == 200, r.text
    body = r.json()
    assert body.get("demo") is True
    assert body.get("access_token")

    db = get_db()
    user = db.get_user_by_id(DEMO_USER_ID)
    assert user and user["email"] == DEMO_USER_EMAIL and user["role"] == "staff"
    assert user.get("venue_ids") == [DEMO_VENUE_ID]

    venue = db.get_venue(DEMO_VENUE_ID)
    assert venue and venue.name == "The Brass Monkey"

    staff = db.get_employees(DEMO_VENUE_ID)
    assert len(staff) == 6
    assert any(e.name == "Emma Thompson" for e in staff)


def test_demo_token_passes_auth_on_ai_endpoints():
    """The demo token authenticates — the AI endpoints no longer 401."""
    c = TestClient(app)
    token = c.post("/api/auth/demo").json()["access_token"]
    h = {"Authorization": f"Bearer {token}"}
    r = c.get("/api/ai/status", headers=h)
    assert r.status_code == 200  # was 401 without a session


def test_demo_user_is_locked_to_the_demo_venue():
    c = TestClient(app)
    token = c.post("/api/auth/demo").json()["access_token"]
    h = {"Authorization": f"Bearer {token}"}

    # A real/other venue is forbidden for the demo account.
    other = c.post("/api/ai/chat",
                   json={"venue_id": "some-other-venue", "message": "hi"}, headers=h)
    assert other.status_code == 403

    # The demo venue passes the scope guard (503 here only because no LLM key is
    # configured in tests — the point is it's NOT 401/403).
    demo = c.post("/api/ai/chat",
                  json={"venue_id": DEMO_VENUE_ID, "message": "hi"}, headers=h)
    assert demo.status_code not in (401, 403)


def test_seed_is_idempotent_and_venue_scoped():
    db = MemoryStore()
    seed_demo_environment(db)
    seed_demo_environment(db)  # second call is a no-op
    assert len(db.get_employees(DEMO_VENUE_ID)) == 6
    assert db.get_employees("nonexistent-venue") == []


def test_demo_roster_rolls_forward_each_week(monkeypatch):
    """The demo roster must follow the current week. Fixed ids used to collide
    with the previous week's rows (roster upsert didn't update the week, shift
    upsert was DO NOTHING), leaving the demo stuck a month in the past and the
    AI answering "$0 labour today"."""
    import datetime as _dt
    from rosteriq.services import demo as demo_mod

    db = MemoryStore()
    seed_demo_environment(db)
    from datetime import date as real_date, timedelta
    this_monday = real_date.today() - timedelta(days=real_date.today().weekday())

    # Simulate the demo being minted again NEXT week.
    next_week_today = real_date.today() + timedelta(days=7)

    class _FakeDate(_dt.date):
        @classmethod
        def today(cls):
            return next_week_today

    monkeypatch.setattr(demo_mod, "date", _FakeDate)
    seed_demo_environment(db)

    next_monday = this_monday + timedelta(days=7)
    current = db.get_rosters_by_date_range(DEMO_VENUE_ID, next_monday, next_monday + timedelta(days=6))
    assert current, "no roster seeded for the new current week"
    assert any(s.date == next_week_today for s in current[0].shifts), \
        "new week's roster has no shifts dated 'today'"


def test_demo_user_venue_ids_backfilled(monkeypatch):
    """A demo user row created before venue_ids persisted (empty list) 403'd on
    its own venue. The seeder now self-heals the scoping on existing users."""
    from datetime import datetime as _dtt
    db = MemoryStore()
    db.save_user({
        "id": DEMO_USER_ID, "email": DEMO_USER_EMAIL, "name": "Demo User",
        "password_hash": "", "role": "staff", "is_active": True,
        "venue_ids": [],  # the pre-fix state on production
        "created_at": _dtt.utcnow(),
    })
    seed_demo_environment(db)
    assert db.get_user_by_id(DEMO_USER_ID)["venue_ids"] == [DEMO_VENUE_ID]


def test_demo_as_staff_mints_linked_staff_identity():
    """`POST /api/auth/demo?as=staff` mints the SECOND demo identity (Emma
    Thompson) whose email sits on seeded employee demo-staff-001, so the staff
    phone portal links instead of dead-ending on linked:false."""
    from rosteriq.services.demo import DEMO_STAFF_USER_ID, DEMO_STAFF_EMAIL

    c = TestClient(app)
    r = c.post("/api/auth/demo?as=staff")
    assert r.status_code == 200, r.text
    token = r.json()["access_token"]

    db = get_db()
    user = db.get_user_by_id(DEMO_STAFF_USER_ID)
    assert user and user["email"] == DEMO_STAFF_EMAIL and user["role"] == "staff"
    assert user.get("venue_ids") == [DEMO_VENUE_ID]

    prof = c.get("/api/me/profile", headers={"Authorization": f"Bearer {token}"})
    assert prof.status_code == 200, prof.text
    body = prof.json()
    assert body.get("linked") is True, body
    assert body.get("name") == "Emma Thompson"
    assert body.get("venue_id") == DEMO_VENUE_ID
    assert body.get("employee_id") == "demo-staff-001"

    # The default (no ?as) demo is unchanged: still the venue-side demo user.
    # (/api/auth/me used to KeyError on the seeded row's missing last_login.)
    default_token = c.post("/api/auth/demo").json()["access_token"]
    me = c.get("/api/auth/me", headers={"Authorization": f"Bearer {default_token}"})
    assert me.status_code == 200, me.text
    assert me.json().get("email") == DEMO_USER_EMAIL
    me_staff = c.get("/api/auth/me", headers={"Authorization": f"Bearer {token}"})
    assert me_staff.status_code == 200, me_staff.text
    assert me_staff.json().get("email") == DEMO_STAFF_EMAIL
    assert me_staff.json().get("role") == "staff"


def test_demo_staff_employee_email_self_heals():
    """A demo-staff-001 seeded before the staff identity existed carries no
    email; the seeder must put it back so the staff demo links."""
    from rosteriq.services.demo import DEMO_STAFF_EMAIL

    db = MemoryStore()
    seed_demo_environment(db)
    emma = db.get_employee("demo-staff-001")
    assert emma.email == DEMO_STAFF_EMAIL
    emma.email = None
    db.save_employee(emma)
    assert db.get_employee("demo-staff-001").email is None
    seed_demo_environment(db)
    assert db.get_employee("demo-staff-001").email == DEMO_STAFF_EMAIL
