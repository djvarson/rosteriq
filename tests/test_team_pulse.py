"""
Team pulse: outstanding procedure acknowledgements and unanswered staff feed
posts reach the daily briefing's attention list AND the AI copilot's tool
through ONE shared engine — so the morning screen and the copilot can never
disagree, and neither can be silently "all clear" while staff are waiting.
"""

import json
import uuid

import pytest
from fastapi.testclient import TestClient

from rosteriq.api import app
from rosteriq.ai_agent import AgentContext, GEMINI_TOOLS
from rosteriq.database import get_db


def _login(c, email, name="U"):
    c.post("/api/auth/register", json={"email": email, "password": "Passw0rd!234", "name": name})
    tok = c.post("/api/auth/login", json={"email": email, "password": "Passw0rd!234"}).json()["access_token"]
    return {"Authorization": f"Bearer {tok}"}


def _setup(c, vid):
    owner = _login(c, f"tp{uuid.uuid4().hex[:8]}@x.com", "Owner")
    c.post("/venues", json={"id": vid, "name": "Pulse Venue", "state": "wa", "max_labour_pct": 30,
                            "tanda_org_id": "", "created_at": "2026-07-01T00:00:00"}, headers=owner)
    staff = []
    for i, nm in enumerate(["Ava Floor", "Ben Bar"], start=1):
        em = f"tp{uuid.uuid4().hex[:8]}@x.com"
        c.post("/employees", json={
            "id": f"{vid}-e{i}", "name": nm, "employment_type": "casual", "award_level": "level_2",
            "state": "wa", "venue_id": vid, "hourly_base_rate": "30.00", "email": em, "skills": ["floor"],
            "created_at": "2026-07-01T00:00:00", "updated_at": "2026-07-01T00:00:00"}, headers=owner)
        h = _login(c, em, nm)
        db = get_db(); rec = db.get_user_by_email(em); rec["venue_ids"] = [vid]; db.save_user(rec)
        staff.append(h)
    return owner, staff


def test_tool_registered():
    names = {fn["name"] for g in GEMINI_TOOLS
             for fn in g.get("function_declarations", g.get("functionDeclarations", []))}
    assert "get_team_pulse" in names


@pytest.mark.asyncio
async def test_briefing_and_ai_agree_on_outstanding_acks_and_unanswered_posts():
    c = TestClient(app)
    vid = "tp-venue-1"
    owner, (ava, ben) = _setup(c, vid)

    # Nothing yet -> briefing has no team items, tool reports zeros
    b0 = c.get(f"/api/briefing?venue_id={vid}", headers=owner).json()
    assert not any("acknowledgement" in a or "team feed" in a for a in b0["attention"])
    assert b0["team"] == {"outstanding_acks": 0, "documents_requiring_ack": 0, "feed_awaiting_reply": 0}

    # Publish a procedure applying to everyone; Ava acknowledges, Ben doesn't
    r = c.post("/api/sops/documents", json={"venue_id": vid, "title": "Glass breakage", "category": "sop",
                                            "body": "Stop. Sweep. Bin."}, headers=owner)
    doc_id = r.json()["document_id"]
    assert c.post(f"/api/me/sops/{doc_id}/acknowledge", headers=ava).status_code == 200

    # Ben posts to the feed; nobody has replied
    p = c.post("/api/feed/posts", json={"venue_id": vid, "body": "Can someone cover my Friday?"}, headers=ben)
    assert p.status_code == 200, p.text
    post_id = p.json()["id"]

    b1 = c.get(f"/api/briefing?venue_id={vid}", headers=owner).json()
    assert b1["team"]["outstanding_acks"] == 1
    assert b1["team"]["documents_requiring_ack"] == 1
    assert b1["team"]["feed_awaiting_reply"] == 1
    assert any("1 procedure acknowledgement(s) outstanding" in a and "Glass breakage" in a
               for a in b1["attention"])
    assert any("1 team feed post(s) from staff awaiting a reply" in a for a in b1["attention"])
    assert b1["all_clear"] is False

    # AI tool sees the SAME thing, with names
    ctx = AgentContext(vid)
    pulse = json.loads(await ctx.execute_tool("get_team_pulse", {}))
    assert pulse["procedures"]["outstanding_acks"] == 1
    assert pulse["procedures"]["outstanding"][0]["title"] == "Glass breakage"
    assert pulse["procedures"]["outstanding"][0]["waiting_on"] == ["Ben Bar"]
    assert pulse["team_feed"]["awaiting_reply"] == 1
    assert pulse["team_feed"]["posts"][0]["author"] == "Ben Bar"

    # Manager replies -> feed item clears; a staff reply would NOT have cleared it
    c.post(f"/api/feed/posts/{post_id}/comments", json={"body": "Ava said she can — approve via cover"}, headers=owner)
    # Ben acknowledges -> compliance clears
    assert c.post(f"/api/me/sops/{doc_id}/acknowledge", headers=ben).status_code == 200

    b2 = c.get(f"/api/briefing?venue_id={vid}", headers=owner).json()
    assert b2["team"] == {"outstanding_acks": 0, "documents_requiring_ack": 1, "feed_awaiting_reply": 0}
    assert not any("acknowledgement" in a or "team feed" in a for a in b2["attention"])
    pulse2 = json.loads(await ctx.execute_tool("get_team_pulse", {}))
    assert pulse2["procedures"]["fully_acknowledged"] == ["Glass breakage"]
    assert pulse2["team_feed"]["awaiting_reply"] == 0


def test_staff_reply_does_not_count_as_answered():
    c = TestClient(app)
    vid = "tp-venue-2"
    owner, (ava, ben) = _setup(c, vid)
    p = c.post("/api/feed/posts", json={"venue_id": vid, "body": "Where are the new dockets kept?"}, headers=ben)
    post_id = p.json()["id"]
    c.post(f"/api/feed/posts/{post_id}/comments", json={"body": "no idea sorry"}, headers=ava)
    b = c.get(f"/api/briefing?venue_id={vid}", headers=owner).json()
    assert b["team"]["feed_awaiting_reply"] == 1  # a colleague's shrug is not a manager answer


def test_reference_only_and_retired_docs_do_not_count():
    c = TestClient(app)
    vid = "tp-venue-3"
    owner, (ava, ben) = _setup(c, vid)
    c.post("/api/sops/documents", json={"venue_id": vid, "title": "Wine list notes", "category": "other",
                                        "body": "FYI", "requires_ack": False}, headers=owner)
    r = c.post("/api/sops/documents", json={"venue_id": vid, "title": "Old procedure", "category": "sop",
                                            "body": "x"}, headers=owner)
    old_id = r.json()["document_id"]
    c.put(f"/api/sops/documents/{old_id}", json={"active": False}, headers=owner)
    b = c.get(f"/api/briefing?venue_id={vid}", headers=owner).json()
    assert b["team"]["outstanding_acks"] == 0
    assert b["team"]["documents_requiring_ack"] == 0
