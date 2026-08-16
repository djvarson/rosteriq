"""
Tenant scoping for the ops routes: approvals (routes/approvals.py) and roster
publishing (routes/publishing.py).

Before this, both routers gated on ROLE only, never on venue: a venue-A manager
could list/approve/reject/escalate venue-B approval requests and
recall/archive/publish/auto-publish/inspect venue-B rosters by id.

Now:
- GET /api/approvals/pending?venue_id=B -> 403 for a venue-A manager, and the
  unfiltered list never contains other venues' approvals.
- approve/reject/escalate/diff by approval id -> 404 for another tenant's
  request (identical to a missing id, so ids are not an oracle).
- history/{roster_id} -> 404 for another tenant's roster.
- publish/recall/archive/auto-publish/state/transition by roster id -> 404 for
  another tenant's roster; own roster works; owner still passes everything.
- GET /api/v1/venues/{id}/publication-history -> 403 for another tenant's venue.
"""

import uuid
from dataclasses import asdict
from datetime import date, datetime

from fastapi.testclient import TestClient

from rosteriq.api import app
from rosteriq.database import get_db
from rosteriq.models import Roster
from rosteriq.services.approval_workflow import (
    approval_workflow,
    ApprovalRequest,
    ApprovalStatus,
)

PW = "Passw0rd!234"


# ----------------------------------------------------------------------------
# helpers
# ----------------------------------------------------------------------------


def _err(body):
    if not isinstance(body, dict):
        return None
    return body.get("detail") or body.get("error", {}).get("message")


def _register(c, email):
    return c.post("/api/auth/register", json={"email": email, "password": PW, "name": "U"})


def _login(c, email):
    r = c.post("/api/auth/login", json={"email": email, "password": PW})
    body = r.json()
    tok = body.get("access_token") or body.get("tokens", {}).get("access_token")
    return {"Authorization": f"Bearer {tok}"}


def _venue(c, headers, vid):
    r = c.post("/venues", json={
        "id": vid, "name": vid, "state": "wa", "max_labour_pct": 30,
        "tanda_org_id": "", "created_at": "2026-06-20T00:00:00",
    }, headers=headers)
    assert r.status_code == 200, r.text


def _manager_of(c, email, venue_id):
    """Register a user, make them a venue-scoped MANAGER of ``venue_id``, log in."""
    _register(c, email)
    db = get_db()
    rec = db.get_user_by_email(email)
    rec["role"] = "manager"
    rec["venue_ids"] = [venue_id]
    db.save_user(rec)
    return _login(c, email)


def _seed_roster(venue_id, rid, state=None):
    db = get_db()
    db.save_roster(Roster(
        id=rid, venue_id=venue_id,
        week_start=date(2026, 6, 22), week_end=date(2026, 6, 28),
        shifts=[], created_at=datetime(2026, 6, 20),
    ))
    if state:
        db.update_roster_state(rid, state, "seed", "test")
    return rid


def _seed_approval(venue_id, roster_id, submitted_by="u"):
    req = ApprovalRequest(
        id=str(uuid.uuid4()),
        roster_id=roster_id,
        venue_id=venue_id,
        submitted_by=submitted_by,
        submitted_at=datetime(2026, 6, 20),
        status=ApprovalStatus.pending,
        tier="pro",
    )
    get_db().save_approval_request(asdict(req))
    return req.id


def _setup():
    """Owner + two venues (venA/venB) + a manager scoped to each. Returns
    (client, owner_headers, mgrA_headers, mgrB_headers)."""
    c = TestClient(app)
    # The approval workflow singleton captured the DB store at import; point
    # it at the (per-test, reset) store the routes use.
    approval_workflow.db = get_db()

    owner_email = f"owner_{uuid.uuid4().hex[:8]}@x.com"
    _register(c, owner_email)  # first user bootstraps as owner
    oh = _login(c, owner_email)
    _venue(c, oh, "venA")
    _venue(c, oh, "venB")

    ah = _manager_of(c, f"mgra_{uuid.uuid4().hex[:8]}@x.com", "venA")
    bh = _manager_of(c, f"mgrb_{uuid.uuid4().hex[:8]}@x.com", "venB")
    return c, oh, ah, bh


# ----------------------------------------------------------------------------
# approvals
# ----------------------------------------------------------------------------


def test_pending_approvals_are_venue_scoped():
    c, oh, ah, bh = _setup()
    _seed_roster("venA", "rA")
    _seed_roster("venB", "rB")
    idA = _seed_approval("venA", "rA")
    idB = _seed_approval("venB", "rB")

    # Explicit filter on another tenant's venue -> 403
    r = c.get("/api/approvals/pending", params={"venue_id": "venB"}, headers=ah)
    assert r.status_code == 403, r.text

    # Own venue filter -> only own approval
    r = c.get("/api/approvals/pending", params={"venue_id": "venA"}, headers=ah)
    assert r.status_code == 200, r.text
    assert {a["id"] for a in r.json()["approvals"]} == {idA}

    # No filter -> manager A never sees venue B's approvals
    r = c.get("/api/approvals/pending", headers=ah)
    assert r.status_code == 200, r.text
    ids = {a["id"] for a in r.json()["approvals"]}
    assert idA in ids and idB not in ids, ids
    assert {a["venue_id"] for a in r.json()["approvals"]} == {"venA"}

    # Manager B symmetric
    r = c.get("/api/approvals/pending", headers=bh)
    assert {a["id"] for a in r.json()["approvals"]} == {idB}

    # Owner sees both
    r = c.get("/api/approvals/pending", headers=oh)
    assert r.status_code == 200, r.text
    assert {idA, idB} <= {a["id"] for a in r.json()["approvals"]}


def test_manager_cannot_approve_reject_escalate_other_tenants_approval():
    c, oh, ah, bh = _setup()
    _seed_roster("venA", "rA")
    _seed_roster("venB", "rB")
    idA = _seed_approval("venA", "rA")
    idB = _seed_approval("venB", "rB")

    # --- cross-tenant: 404 (not 403) so approval ids are not an oracle ---
    r = c.post(f"/api/approvals/{idB}/approve", json={"approved": True, "notes": "x"}, headers=ah)
    assert r.status_code == 404, r.text
    r = c.post(f"/api/approvals/{idB}/reject", json={"approved": False, "notes": "x"}, headers=ah)
    assert r.status_code == 404, r.text
    r = c.post(f"/api/approvals/{idB}/escalate", headers=ah)
    assert r.status_code == 404, r.text
    r = c.get(f"/api/approvals/{idB}/diff", params={"revision_a": 1, "revision_b": 2}, headers=ah)
    assert r.status_code == 404, r.text

    # Missing id and foreign id are indistinguishable
    missing = c.post("/api/approvals/does-not-exist/approve",
                     json={"approved": True}, headers=ah)
    assert missing.status_code == 404
    assert _err(missing.json()) == _err(
        c.post(f"/api/approvals/{idB}/approve", json={"approved": True}, headers=ah).json()
    ).replace(idB, "does-not-exist")

    # B's request is untouched
    assert get_db().get_approval_request(idB)["status"] == ApprovalStatus.pending

    # --- own venue: happy path ---
    r = c.post(f"/api/approvals/{idA}/escalate", headers=ah)
    assert r.status_code == 200, r.text
    assert r.json()["status"] == "escalated"
    r = c.post(f"/api/approvals/{idA}/approve", json={"approved": True, "notes": "ok"}, headers=ah)
    assert r.status_code == 200, r.text
    assert r.json()["status"] == "approved"

    # Manager B can reject their own
    r = c.post(f"/api/approvals/{idB}/reject", json={"approved": False, "notes": "no"}, headers=bh)
    assert r.status_code == 200, r.text
    assert r.json()["status"] == "rejected"

    # --- owner passes everything ---
    idB2 = _seed_approval("venB", "rB")
    r = c.post(f"/api/approvals/{idB2}/approve", json={"approved": True}, headers=oh)
    assert r.status_code == 200, r.text
    assert r.json()["status"] == "approved"


def test_approval_history_and_submit_are_venue_scoped():
    c, oh, ah, bh = _setup()
    _seed_roster("venA", "rA")
    _seed_roster("venB", "rB")
    _seed_approval("venA", "rA")
    _seed_approval("venB", "rB")

    # history for another tenant's roster -> 404
    r = c.get("/api/approvals/history/rB", headers=ah)
    assert r.status_code == 404, r.text
    # own roster history -> 200 with own records
    r = c.get("/api/approvals/history/rA", headers=ah)
    assert r.status_code == 200, r.text
    assert len(r.json()) == 1 and r.json()[0]["venue_id"] == "venA"
    # owner can read both
    assert c.get("/api/approvals/history/rB", headers=oh).status_code == 200

    # submit another tenant's roster -> 404; own -> 200
    r = c.post("/api/approvals/submit/rB", headers=ah)
    assert r.status_code == 404, r.text
    r = c.post("/api/approvals/submit/rA", headers=ah)
    assert r.status_code == 200, r.text
    assert r.json()["venue_id"] == "venA"


# ----------------------------------------------------------------------------
# publishing
# ----------------------------------------------------------------------------


def test_manager_cannot_operate_on_other_tenants_roster():
    c, oh, ah, bh = _setup()
    # venue A: one draft (publish), two published (recall / archive)
    _seed_roster("venA", "rA-draft")
    _seed_roster("venA", "rA-pub1", state="published")
    _seed_roster("venA", "rA-pub2", state="published")
    # venue B: one draft, one published
    _seed_roster("venB", "rB-draft")
    _seed_roster("venB", "rB-pub", state="published")

    # --- cross-tenant by roster id: all 404 for manager A ---
    assert c.post("/api/v1/rosters/rB-draft/publish",
                  json={"skip_approval": True}, headers=ah).status_code == 404
    assert c.post("/api/v1/rosters/rB-draft/auto-publish", headers=ah).status_code == 404
    assert c.post("/api/v1/rosters/rB-pub/recall",
                  json={"reason": "steal"}, headers=ah).status_code == 404
    assert c.post("/api/v1/rosters/rB-pub/archive", headers=ah).status_code == 404
    assert c.get("/api/v1/rosters/rB-pub/state", headers=ah).status_code == 404
    # publication history is a venue-path route -> 403
    assert c.get("/api/v1/venues/venB/publication-history", headers=ah).status_code == 403

    # Foreign id reads exactly like a missing id
    r_missing = c.get("/api/v1/rosters/nope/state", headers=ah)
    r_foreign = c.get("/api/v1/rosters/rB-pub/state", headers=ah)
    assert r_missing.status_code == r_foreign.status_code == 404
    assert _err(r_missing.json()) == _err(r_foreign.json()).replace("rB-pub", "nope")

    # B's rosters are untouched
    db = get_db()
    assert db.get_roster_state("rB-pub") == "published"
    assert db.get_roster_state("rB-draft") == "draft"

    # --- own venue: happy paths for manager A ---
    r = c.get("/api/v1/rosters/rA-pub1/state", headers=ah)
    assert r.status_code == 200, r.text
    assert r.json()["current_state"] == "published"

    r = c.post("/api/v1/rosters/rA-pub1/recall", json={"reason": "fix"}, headers=ah)
    assert r.status_code == 200, r.text
    assert db.get_roster_state("rA-pub1") == "draft"

    r = c.post("/api/v1/rosters/rA-pub2/archive", headers=ah)
    assert r.status_code == 200, r.text
    assert db.get_roster_state("rA-pub2") == "archived"

    r = c.post("/api/v1/rosters/rA-draft/publish", json={"skip_approval": True}, headers=ah)
    assert r.status_code == 200, r.text

    assert c.get("/api/v1/venues/venA/publication-history", headers=ah).status_code == 200

    # --- owner passes everything ---
    r = c.get("/api/v1/rosters/rB-pub/state", headers=oh)
    assert r.status_code == 200, r.text
    r = c.post("/api/v1/rosters/rB-pub/recall", json={"reason": "owner"}, headers=oh)
    assert r.status_code == 200, r.text
    assert c.get("/api/v1/venues/venB/publication-history", headers=oh).status_code == 200
    # manual transition is owner-only AND venue-checked; owner passes
    r = c.post("/api/v1/rosters/rB-draft/transition",
               json={"new_state": "archived", "reason": "admin"}, headers=oh)
    assert r.status_code == 200, r.text
