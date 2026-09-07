"""
Tenant isolation for the shift-handover acknowledge endpoint.

POST /api/v1/handover/{note_id}/acknowledge carries only an employee_id in its
body (no venue_id), so before the 2026-09 fix any authenticated user could mark
ANOTHER tenant's handover note acknowledged — the last cross-tenant hole left
open by the 2026-08-30 authz overhaul (create_handover_note was already gated).

The fix resolves the note's OWN venue via the handover service and calls
enforce_venue_access(note.venue_id) BEFORE the handler's try block (the broad
`except Exception` there would otherwise convert the 403 into a 500).

These tests exercise the route's authorization directly. The handover service's
persistence layer is stubbed with a fake because the real store does not yet
implement the key/value methods the service calls (save_to_key/load_from_key
exist on neither MemoryStore nor PostgresStore — a separate, pre-existing gap);
the vulnerability being fixed is in the ROUTE, not the store, so faking the
persistence keeps the test focused on the gate and its placement.
"""

import uuid

import pytest
from fastapi.testclient import TestClient

import rosteriq.routes.handover as handover_route
from rosteriq.api import app
from rosteriq.database import get_db
from rosteriq.services.handover_notes import HandoverNote


def _register(c, email):
    return c.post("/api/auth/register",
                  json={"email": email, "password": "Passw0rd!234", "name": "U"})


def _login(c, email):
    r = c.post("/api/auth/login", json={"email": email, "password": "Passw0rd!234"})
    body = r.json()
    return body.get("access_token") or body.get("tokens", {}).get("access_token")


def _venue(c, headers, vid):
    return c.post("/venues", json={
        "id": vid, "name": vid, "state": "vic", "max_labour_pct": 30,
        "tanda_org_id": "", "created_at": "2026-06-20T00:00:00",
    }, headers=headers)


def _make_note(note_id, venue_id):
    return HandoverNote(
        id=note_id,
        shift_id=f"shift_{venue_id}",
        venue_id=venue_id,
        author_id="author1",
        author_name="Outgoing Staff",
        created_at="2026-06-20T00:00:00Z",
        sections={},
        priority="normal",
    )


class _FakeHandoverService:
    """In-memory stand-in for HandoverService with the two methods the
    acknowledge route touches. Mirrors the real contract: acknowledge_note
    raises ValueError when the note is unknown (→ 404)."""

    def __init__(self, notes):
        self._notes = {n.id: n for n in notes}

    def get_note(self, note_id):
        return self._notes.get(note_id)

    def acknowledge_note(self, note_id, employee_id):
        note = self._notes.get(note_id)
        if not note:
            raise ValueError(f"Handover note {note_id} not found")
        note.acknowledged_by = employee_id
        note.acknowledged_at = "2026-06-20T01:00:00Z"
        return note


def _scope_staff_to(email, venue_ids):
    db = get_db()
    rec = db.get_user_by_email(email)
    rec["venue_ids"] = venue_ids
    rec["role"] = "staff"
    db.save_user(rec)


def test_non_member_cannot_acknowledge_other_venue_handover(monkeypatch):
    c = TestClient(app)

    # First user bootstraps as owner; create two venues.
    owner_email = f"owner_{uuid.uuid4().hex[:8]}@x.com"
    _register(c, owner_email)
    oh = {"Authorization": f"Bearer {_login(c, owner_email)}"}
    assert _venue(c, oh, "venA").status_code == 200
    assert _venue(c, oh, "venB").status_code == 200

    # Second user is scoped to venA only.
    staff_email = f"staff_{uuid.uuid4().hex[:8]}@x.com"
    _register(c, staff_email)
    _scope_staff_to(staff_email, ["venA"])
    sh = {"Authorization": f"Bearer {_login(c, staff_email)}"}

    # A handover note owned by venB (NOT the staff user's venue).
    note = _make_note("hn_venB_1", "venB")
    fake = _FakeHandoverService([note])
    monkeypatch.setattr(handover_route, "get_handover_service", lambda: fake)

    # --- The attack: venA-scoped user tries to acknowledge venB's note ---
    r = c.post("/api/v1/handover/hn_venB_1/acknowledge",
               json={"employee_id": "intruder"}, headers=sh)
    # Must be a clean 403 — NOT a 500 (which is what would happen if the gate
    # were placed inside the try block, where `except Exception` catches it).
    assert r.status_code == 403, r.text

    # The 403 is a genuine block: the note stays unacknowledged.
    assert note.acknowledged_by is None
    assert note.acknowledged_at is None

    # --- Guardrail: the gate does not over-block a platform owner ---
    r = c.post("/api/v1/handover/hn_venB_1/acknowledge",
               json={"employee_id": "owner_ack"}, headers=oh)
    assert r.status_code == 200, r.text
    assert r.json()["acknowledged_by"] == "owner_ack"


def test_member_can_acknowledge_own_venue_handover(monkeypatch):
    c = TestClient(app)

    owner_email = f"owner_{uuid.uuid4().hex[:8]}@x.com"
    _register(c, owner_email)
    oh = {"Authorization": f"Bearer {_login(c, owner_email)}"}
    assert _venue(c, oh, "venA").status_code == 200

    staff_email = f"staff_{uuid.uuid4().hex[:8]}@x.com"
    _register(c, staff_email)
    _scope_staff_to(staff_email, ["venA"])
    sh = {"Authorization": f"Bearer {_login(c, staff_email)}"}

    note = _make_note("hn_venA_1", "venA")
    fake = _FakeHandoverService([note])
    monkeypatch.setattr(handover_route, "get_handover_service", lambda: fake)

    # A member of venA CAN acknowledge venA's own handover note.
    r = c.post("/api/v1/handover/hn_venA_1/acknowledge",
               json={"employee_id": "incoming1"}, headers=sh)
    assert r.status_code == 200, r.text
    assert r.json()["acknowledged_by"] == "incoming1"


def test_acknowledge_missing_note_is_404_not_403(monkeypatch):
    """A note that does not exist must return 404 — the venue gate must not turn
    a non-existent note into a 403, which would let a caller probe note IDs."""
    c = TestClient(app)

    owner_email = f"owner_{uuid.uuid4().hex[:8]}@x.com"
    _register(c, owner_email)
    oh = {"Authorization": f"Bearer {_login(c, owner_email)}"}
    assert _venue(c, oh, "venA").status_code == 200

    staff_email = f"staff_{uuid.uuid4().hex[:8]}@x.com"
    _register(c, staff_email)
    _scope_staff_to(staff_email, ["venA"])
    sh = {"Authorization": f"Bearer {_login(c, staff_email)}"}

    fake = _FakeHandoverService([])  # no notes at all
    monkeypatch.setattr(handover_route, "get_handover_service", lambda: fake)

    r = c.post("/api/v1/handover/hn_does_not_exist/acknowledge",
               json={"employee_id": "x"}, headers=sh)
    assert r.status_code == 404, r.text
