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
