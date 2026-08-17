"""
Team feed: two-way posts by staff and managers, comments, toggling reactions,
manager-only pin, author-or-manager remove, and venue scoping.
"""

import uuid

from fastapi.testclient import TestClient

from rosteriq.api import app
from rosteriq.database import get_db

THUMBS = "\U0001F44D"
HEART = "❤️"


def _err(r):
    body = r.json()
    return body.get("detail") or (body.get("error") or {}).get("message")


def _register_login(c, email):
    c.post("/api/auth/register", json={"email": email, "password": "Passw0rd!234", "name": "U"})
    tok = c.post("/api/auth/login", json={"email": email, "password": "Passw0rd!234"}).json()["access_token"]
    return {"Authorization": f"Bearer {tok}"}


def _register_link_login(c, email, emp_id):
    """The real staff flow: self-register, enter the manager-issued join code
    (POST /api/me/link), log in again so the token carries the venue."""
    from rosteriq.services.linking import join_code
    h = _register_login(c, email)
    r = c.post("/api/me/link", json={"code": join_code(emp_id)}, headers=h)
    assert r.status_code == 200, r.text
    return _register_login(c, email)  # re-login (register is a no-op 4xx now)


def _venue(c, owner_h, vid):
    r = c.post("/venues", json={
        "id": vid, "name": "Feed Venue", "state": "wa", "max_labour_pct": 30,
        "tanda_org_id": "", "created_at": "2026-07-01T00:00:00",
    }, headers=owner_h)
    assert r.status_code in (200, 201), r.text


def _employee(c, owner_h, vid, emp_id, name, email):
    r = c.post("/employees", json={
        "id": emp_id, "name": name, "employment_type": "casual",
        "award_level": "level_2", "state": "wa", "venue_id": vid,
        "hourly_base_rate": "30.00", "email": email,
        "created_at": "2026-07-01T00:00:00", "updated_at": "2026-07-01T00:00:00",
    }, headers=owner_h)
    assert r.status_code in (200, 201), r.text


def _world():
    """Owner + venue + two staff (linked by email) + a second venue with its own staff."""
    c = TestClient(app)
    owner_h = _register_login(c, f"o{uuid.uuid4().hex[:8]}@x.com")
    vid = f"fd-{uuid.uuid4().hex[:6]}"
    _venue(c, owner_h, vid)
    s1_email = f"s1{uuid.uuid4().hex[:8]}@x.com"
    s2_email = f"s2{uuid.uuid4().hex[:8]}@x.com"
    _employee(c, owner_h, vid, f"{vid}-e1", "Ava Staff", s1_email)
    _employee(c, owner_h, vid, f"{vid}-e2", "Ben Staff", s2_email)
    s1_h = _register_link_login(c, s1_email, f"{vid}-e1")
    s2_h = _register_link_login(c, s2_email, f"{vid}-e2")
    return c, owner_h, vid, s1_h, s2_h


def _post(c, h, vid, body):
    r = c.post("/api/feed/posts", json={"venue_id": vid, "body": body}, headers=h)
    assert r.status_code == 200, r.text
    return r.json()


def _manager_at(c, email, vid):
    """Register a user and make them a manager of ``vid`` (store-level, like the house pattern)."""
    h = _register_login(c, email)
    db = get_db()
    rec = db.get_user_by_email(email)
    rec["role"] = "manager"
    rec["venue_ids"] = [vid]
    db.save_user(rec)
    return h


def test_staff_post_is_author_role_staff_with_employee_name():
    c, owner_h, vid, s1_h, _ = _world()
    p = _post(c, s1_h, vid, "Anyone able to swap my Sat close?")
    assert p["author_role"] == "staff"
    assert p["author_name"] == "Ava Staff"
    assert p["venue_id"] == vid and p["pinned"] is False and p["removed"] is False
    assert p["reaction_counts"] == {} and p["my_reactions"] == [] and p["comments"] == []
    assert isinstance(p["created_at"], str)


def test_manager_post_is_author_role_manager():
    c, owner_h, vid, _, _ = _world()
    p = _post(c, owner_h, vid, "Kitchen walkthrough 3pm Thursday")
    assert p["author_role"] == "manager"
    # Managers are named by their user name/email, never an employee record
    assert p["author_name"]

    mgr_email = f"m{uuid.uuid4().hex[:8]}@x.com"
    mgr_h = _manager_at(c, mgr_email, vid)
    p2 = _post(c, mgr_h, vid, "Manager two here")
    assert p2["author_role"] == "manager"


def test_listing_pinned_first_newest_first_and_hides_removed():
    c, owner_h, vid, s1_h, _ = _world()
    p1 = _post(c, s1_h, vid, "first")
    p2 = _post(c, owner_h, vid, "second")
    p3 = _post(c, s1_h, vid, "third")

    feed = c.get(f"/api/feed/posts?venue_id={vid}", headers=s1_h).json()
    assert feed["venue_id"] == vid and feed["count"] == 3
    assert [p["id"] for p in feed["posts"]] == [p3["id"], p2["id"], p1["id"]]

    # Pin the oldest -> it floats to the top; remove the newest -> hidden
    r = c.post(f"/api/feed/posts/{p1['id']}/pin", json={"pinned": True}, headers=owner_h)
    assert r.status_code == 200 and r.json()["status"] == "pinned"
    r = c.delete(f"/api/feed/posts/{p3['id']}", headers=owner_h)
    assert r.status_code == 200 and r.json()["status"] == "removed"

    feed = c.get(f"/api/feed/posts?venue_id={vid}", headers=owner_h).json()
    assert feed["count"] == 2
    assert [p["id"] for p in feed["posts"]] == [p1["id"], p2["id"]]
    assert feed["posts"][0]["pinned"] is True

    # Removed post is gone for per-post routes too
    assert c.post(f"/api/feed/posts/{p3['id']}/comments", json={"body": "hi"},
                  headers=owner_h).status_code == 404


def test_comment_appends_with_author_identity():
    c, owner_h, vid, s1_h, s2_h = _world()
    p = _post(c, s1_h, vid, "Swap my Sat close?")
    r = c.post(f"/api/feed/posts/{p['id']}/comments", json={"body": "I can take it"}, headers=s2_h)
    assert r.status_code == 200, r.text
    r2 = c.post(f"/api/feed/posts/{p['id']}/comments", json={"body": "Approved — thanks Ben"}, headers=owner_h)
    assert r2.status_code == 200, r2.text
    post = r2.json()
    assert post["comment_count"] == 2
    assert [cm["body"] for cm in post["comments"]] == ["I can take it", "Approved — thanks Ben"]
    assert post["comments"][0]["author_name"] == "Ben Staff"
    assert post["comments"][0]["author_role"] == "staff"
    assert post["comments"][1]["author_role"] == "manager"
    assert all(isinstance(cm["created_at"], str) and cm["id"] for cm in post["comments"])

    # Empty / over-long comment bodies are rejected by validation
    assert c.post(f"/api/feed/posts/{p['id']}/comments", json={"body": ""}, headers=s2_h).status_code == 422
    assert c.post(f"/api/feed/posts/{p['id']}/comments", json={"body": "x" * 1001}, headers=s2_h).status_code == 422


def test_react_toggles_on_then_off_with_counts():
    c, owner_h, vid, s1_h, s2_h = _world()
    p = _post(c, owner_h, vid, "New pass-through window opens Friday")

    r = c.post(f"/api/feed/posts/{p['id']}/react", json={"emoji": THUMBS}, headers=s1_h)
    assert r.status_code == 200, r.text
    assert r.json()["status"] == "added"
    assert r.json()["reaction_counts"] == {THUMBS: 1} and r.json()["my_reactions"] == [THUMBS]

    r = c.post(f"/api/feed/posts/{p['id']}/react", json={"emoji": THUMBS}, headers=s2_h)
    assert r.json()["reaction_counts"] == {THUMBS: 2} and r.json()["my_reactions"] == [THUMBS]

    r = c.post(f"/api/feed/posts/{p['id']}/react", json={"emoji": HEART}, headers=s2_h)
    assert r.json()["reaction_counts"] == {THUMBS: 2, HEART: 1}
    assert sorted(r.json()["my_reactions"]) == sorted([THUMBS, HEART])

    # Toggle off: s1 un-thumbs
    r = c.post(f"/api/feed/posts/{p['id']}/react", json={"emoji": THUMBS}, headers=s1_h)
    assert r.json()["status"] == "removed"
    assert r.json()["reaction_counts"] == {THUMBS: 1, HEART: 1} and r.json()["my_reactions"] == []

    # Listing shows per-caller my_reactions
    feed = c.get(f"/api/feed/posts?venue_id={vid}", headers=s2_h).json()
    post = feed["posts"][0]
    assert post["reaction_counts"] == {THUMBS: 1, HEART: 1}
    assert sorted(post["my_reactions"]) == sorted([THUMBS, HEART])
    feed_s1 = c.get(f"/api/feed/posts?venue_id={vid}", headers=s1_h).json()
    assert feed_s1["posts"][0]["my_reactions"] == []

    # Unknown emoji rejected with a message naming the allowed set
    r = c.post(f"/api/feed/posts/{p['id']}/react", json={"emoji": "🍕"}, headers=s1_h)
    assert r.status_code == 422
    assert THUMBS in (_err(r) or "")


def test_staff_cannot_pin_manager_can_and_it_reorders():
    c, owner_h, vid, s1_h, _ = _world()
    p1 = _post(c, s1_h, vid, "older")
    p2 = _post(c, s1_h, vid, "newer")

    r = c.post(f"/api/feed/posts/{p1['id']}/pin", json={"pinned": True}, headers=s1_h)
    assert r.status_code == 403, r.text
    assert "manager" in (_err(r) or "").lower()

    r = c.post(f"/api/feed/posts/{p1['id']}/pin", json={"pinned": True}, headers=owner_h)
    assert r.status_code == 200 and r.json()["post"]["pinned"] is True
    ids = [p["id"] for p in c.get(f"/api/feed/posts?venue_id={vid}", headers=s1_h).json()["posts"]]
    assert ids == [p1["id"], p2["id"]]

    # Unpin restores newest-first
    r = c.post(f"/api/feed/posts/{p1['id']}/pin", json={"pinned": False}, headers=owner_h)
    assert r.json()["status"] == "unpinned"
    ids = [p["id"] for p in c.get(f"/api/feed/posts?venue_id={vid}", headers=s1_h).json()["posts"]]
    assert ids == [p2["id"], p1["id"]]


def test_delete_author_yes_other_staff_no_manager_yes():
    c, owner_h, vid, s1_h, s2_h = _world()
    mine = _post(c, s1_h, vid, "my own post")
    theirs = _post(c, s2_h, vid, "ben's post")

    # Other staff cannot remove my post
    r = c.delete(f"/api/feed/posts/{mine['id']}", headers=s2_h)
    assert r.status_code == 403, r.text
    # Author can
    assert c.delete(f"/api/feed/posts/{mine['id']}", headers=s1_h).status_code == 200
    # Already-removed -> 404
    assert c.delete(f"/api/feed/posts/{mine['id']}", headers=s1_h).status_code == 404
    # Manager can remove someone else's
    assert c.delete(f"/api/feed/posts/{theirs['id']}", headers=owner_h).status_code == 200

    feed = c.get(f"/api/feed/posts?venue_id={vid}", headers=owner_h).json()
    assert feed["count"] == 0


def test_cross_venue_staff_is_denied():
    c, owner_h, vid, s1_h, _ = _world()
    other = f"fd-other-{uuid.uuid4().hex[:6]}"
    _venue(c, owner_h, other)
    other_email = f"x{uuid.uuid4().hex[:8]}@x.com"
    _employee(c, owner_h, other, f"{other}-e1", "Outsider", other_email)
    outsider_h = _register_link_login(c, other_email, f"{other}-e1")

    # Outsider (linked at the other venue) can post at their venue ...
    _post(c, outsider_h, other, "hello from other venue")
    # ... but not read or write ours
    assert c.get(f"/api/feed/posts?venue_id={vid}", headers=outsider_h).status_code == 403
    r = c.post("/api/feed/posts", json={"venue_id": vid, "body": "sneaky"}, headers=outsider_h)
    assert r.status_code == 403, r.text

    # Per-post routes on our venue's post are a 404 for the outsider (no existence leak)
    p = _post(c, s1_h, vid, "ours")
    assert c.post(f"/api/feed/posts/{p['id']}/comments", json={"body": "hi"}, headers=outsider_h).status_code == 404
    assert c.post(f"/api/feed/posts/{p['id']}/react", json={"emoji": THUMBS}, headers=outsider_h).status_code == 404
    assert c.delete(f"/api/feed/posts/{p['id']}", headers=outsider_h).status_code == 404

    # A manager scoped to a different venue is also refused (403 on the venue routes)
    mgr_h = _manager_at(c, f"m{uuid.uuid4().hex[:8]}@x.com", "not-this-one")
    assert c.get(f"/api/feed/posts?venue_id={vid}", headers=mgr_h).status_code == 403
    assert c.post(f"/api/feed/posts/{p['id']}/pin", json={"pinned": True}, headers=mgr_h).status_code == 404


def test_unlinked_user_gets_403_naming_the_fix():
    c, owner_h, vid, _, _ = _world()
    stray_h = _register_login(c, f"stray{uuid.uuid4().hex[:8]}@x.com")
    r = c.post("/api/feed/posts", json={"venue_id": vid, "body": "who am I"}, headers=stray_h)
    assert r.status_code == 403, r.text
    assert c.get(f"/api/feed/posts?venue_id={vid}", headers=stray_h).status_code == 403

    # A staff-role user who HAS venue access but no employee record is told how to link
    db = get_db()
    email = f"granted{uuid.uuid4().hex[:8]}@x.com"
    granted_h = _register_login(c, email)
    rec = db.get_user_by_email(email)
    rec["role"] = "staff"
    rec["venue_ids"] = [vid]
    db.save_user(rec)
    r = c.post("/api/feed/posts", json={"venue_id": vid, "body": "still unlinked"}, headers=granted_h)
    assert r.status_code == 403, r.text
    assert "No staff record matches your login email" in (_err(r) or "")


def test_body_validation_and_unknown_post():
    c, owner_h, vid, s1_h, _ = _world()
    assert c.post("/api/feed/posts", json={"venue_id": vid, "body": ""}, headers=s1_h).status_code == 422
    assert c.post("/api/feed/posts", json={"venue_id": vid, "body": "x" * 2001}, headers=s1_h).status_code == 422
    assert c.post("/api/feed/posts/ghost/comments", json={"body": "hi"}, headers=owner_h).status_code == 404
    assert c.post("/api/feed/posts/ghost/react", json={"emoji": THUMBS}, headers=owner_h).status_code == 404
    assert c.delete("/api/feed/posts/ghost", headers=owner_h).status_code == 404


def test_demo_seed_populates_feed_once():
    from rosteriq.services.demo import seed_demo_environment, DEMO_VENUE_ID
    db = get_db()
    seed_demo_environment(db)
    posts = db.list_feed_posts(DEMO_VENUE_ID)
    assert len(posts) == 2
    assert posts[0]["pinned"] is True and posts[0]["author_role"] == "manager"
    assert posts[0]["reactions"][THUMBS] and len(posts[0]["reactions"][THUMBS]) == 2
    assert posts[1]["author_role"] == "staff" and posts[1]["author_name"] == "James Wilson"
    assert posts[1]["comments"][0]["author_role"] == "manager"
    # Idempotent: seeding again does not duplicate
    seed_demo_environment(db)
    assert len(db.list_feed_posts(DEMO_VENUE_ID)) == 2


# ---------------------------------------------------------------------------
# Hardening: demo identity sandbox, NUL bytes, manager-on-roster identity,
# atomic comment append / reaction toggle in the store.
# ---------------------------------------------------------------------------

def test_demo_identity_never_auto_links_into_a_real_venue():
    """The staff-portal auto-link scans every venue by email and GRANTS the
    matching venue to the user. A real venue that puts demo@rosteriq.app on an
    employee record must NOT pull the public demo user into that tenant."""
    from rosteriq.services.auth import auth_service
    from rosteriq.services.demo import (
        seed_demo_environment, DEMO_USER_ID, DEMO_USER_EMAIL, DEMO_VENUE_ID,
    )
    c, owner_h, vid, _, _ = _world()
    db = get_db()
    seed_demo_environment(db)
    before = list((db.get_user_by_id(DEMO_USER_ID) or {}).get("venue_ids") or [])
    _employee(c, owner_h, vid, f"{vid}-trap", "Trap Employee", DEMO_USER_EMAIL)

    tok, _ = auth_service.create_access_token(DEMO_USER_ID, DEMO_USER_EMAIL, "staff")
    demo_h = {"Authorization": f"Bearer {tok}"}

    prof = c.get("/api/me/profile", headers=demo_h)
    assert prof.status_code == 200, prof.text
    body = prof.json()
    # Either unlinked, or linked ONLY at the demo venue — never at the real one
    if body.get("linked"):
        assert body["venue_id"] == DEMO_VENUE_ID
    assert body.get("venue_id") != vid

    # No durable grant leaked onto the demo user record
    rec = db.get_user_by_id(DEMO_USER_ID)
    assert vid not in (rec.get("venue_ids") or [])
    assert (rec.get("venue_ids") or []) == before

    # And the demo session cannot read or write the real venue's feed
    r = c.post("/api/feed/posts", json={"venue_id": vid, "body": "sneak"}, headers=demo_h)
    assert r.status_code == 403, r.text
    assert c.get(f"/api/feed/posts?venue_id={vid}", headers=demo_h).status_code == 403


def test_nul_bytes_are_stripped_from_post_and_comment_bodies():
    c, owner_h, vid, s1_h, _ = _world()
    p = _post(c, s1_h, vid, "hi\x00there")
    assert p["body"] == "hithere"
    assert get_db().get_feed_post(p["id"])["body"] == "hithere"

    r = c.post(f"/api/feed/posts/{p['id']}/comments", json={"body": "yo\x00u"}, headers=owner_h)
    assert r.status_code == 200, r.text
    assert r.json()["comments"][0]["body"] == "you"

    # A body that is nothing but NULs is empty after stripping -> validation 422
    assert c.post("/api/feed/posts", json={"venue_id": vid, "body": "\x00\x00"},
                  headers=s1_h).status_code == 422
    assert c.post(f"/api/feed/posts/{p['id']}/comments", json={"body": "\x00"},
                  headers=owner_h).status_code == 422


def test_manager_whose_email_is_on_an_employee_record_acts_as_manager():
    """A working manager who is ALSO on the roster (email on an employee
    record) posts/comments as ``manager`` but under the employee's name."""
    c, owner_h, vid, s1_h, _ = _world()
    mgr_email = f"mgr{uuid.uuid4().hex[:8]}@x.com"
    _employee(c, owner_h, vid, f"{vid}-mgr", "Morgan Manager", mgr_email)
    mgr_h = _manager_at(c, mgr_email, vid)

    p = _post(c, mgr_h, vid, "Working manager here")
    assert p["author_role"] == "manager"
    assert p["author_name"] == "Morgan Manager"

    sp = _post(c, s1_h, vid, "staff post")
    r = c.post(f"/api/feed/posts/{sp['id']}/comments", json={"body": "noted"}, headers=mgr_h)
    assert r.status_code == 200, r.text
    assert r.json()["comments"][0]["author_role"] == "manager"
    assert r.json()["comments"][0]["author_name"] == "Morgan Manager"

    # Manager powers still apply (pin someone else's post)
    assert c.post(f"/api/feed/posts/{sp['id']}/pin", json={"pinned": True},
                  headers=mgr_h).status_code == 200

    # A plain staff member with the same shape is still ``staff``
    plain = _post(c, s1_h, vid, "just staff")
    assert plain["author_role"] == "staff" and plain["author_name"] == "Ava Staff"


def test_comment_append_and_reaction_toggle_use_atomic_store_ops(monkeypatch):
    """Comments/reactions must not be whole-blob read-modify-write: the routes
    go through ``append_feed_comment`` / ``toggle_feed_reaction`` (jsonb ``||``
    and a row-locked toggle on Postgres). Guard: if either route falls back to
    ``save_feed_post`` the test fails."""
    from datetime import datetime
    c, owner_h, vid, s1_h, s2_h = _world()
    p = _post(c, owner_h, vid, "atomic")
    db = get_db()

    def _boom(*a, **k):
        raise AssertionError("whole-blob save_feed_post used for a comment/reaction write")
    monkeypatch.setattr(db, "save_feed_post", _boom)

    # Two comments from two people both land, in order (append, not replace)
    assert c.post(f"/api/feed/posts/{p['id']}/comments", json={"body": "one"}, headers=s1_h).status_code == 200
    r = c.post(f"/api/feed/posts/{p['id']}/comments", json={"body": "two"}, headers=s2_h)
    assert r.status_code == 200, r.text
    assert [cm["body"] for cm in r.json()["comments"]] == ["one", "two"]
    assert r.json()["comment_count"] == 2
    assert [cm["body"] for cm in db.get_feed_post(p["id"])["comments"]] == ["one", "two"]

    # Reaction toggle through the route: on, on (other user), off
    r = c.post(f"/api/feed/posts/{p['id']}/react", json={"emoji": THUMBS}, headers=s1_h)
    assert r.status_code == 200 and r.json()["status"] == "added"
    assert r.json()["reaction_counts"] == {THUMBS: 1} and r.json()["my_reactions"] == [THUMBS]
    r = c.post(f"/api/feed/posts/{p['id']}/react", json={"emoji": THUMBS}, headers=s2_h)
    assert r.json()["status"] == "added" and r.json()["reaction_counts"] == {THUMBS: 2}
    r = c.post(f"/api/feed/posts/{p['id']}/react", json={"emoji": THUMBS}, headers=s1_h)
    assert r.json()["status"] == "removed" and r.json()["reaction_counts"] == {THUMBS: 1}
    assert r.json()["my_reactions"] == []
    # The comments written earlier survived the reaction writes (no clobber)
    stored = db.get_feed_post(p["id"])
    assert [cm["body"] for cm in stored["comments"]] == ["one", "two"]
    assert len(stored["reactions"][THUMBS]) == 1

    # Store-level contract: both return the updated post; unknown id -> None
    monkeypatch.undo()
    upd = db.append_feed_comment(p["id"], {
        "id": "fc-store", "author_user_id": "u-x", "author_name": "X",
        "author_role": "staff", "body": "three", "created_at": datetime.utcnow(),
    })
    assert [cm["body"] for cm in upd["comments"]] == ["one", "two", "three"]
    assert db.append_feed_comment("ghost", {"id": "fc-g", "body": "x"}) is None

    post, state = db.toggle_feed_reaction(p["id"], HEART, "user-a")
    assert state == "added" and post["reactions"][HEART] == ["user-a"]
    post, state = db.toggle_feed_reaction(p["id"], HEART, "user-b")
    assert state == "added" and post["reactions"][HEART] == ["user-a", "user-b"]
    post, state = db.toggle_feed_reaction(p["id"], HEART, "user-a")
    assert state == "removed" and post["reactions"][HEART] == ["user-b"]
    post, state = db.toggle_feed_reaction(p["id"], HEART, "user-b")
    assert state == "removed" and HEART not in post["reactions"]
    assert THUMBS in post["reactions"]  # other emoji untouched
    assert db.toggle_feed_reaction("ghost", HEART, "u") == (None, None)

    # Removed / unknown posts still 404 through the routes
    assert c.delete(f"/api/feed/posts/{p['id']}", headers=owner_h).status_code == 200
    assert c.post(f"/api/feed/posts/{p['id']}/comments", json={"body": "late"}, headers=s1_h).status_code == 404
    assert c.post(f"/api/feed/posts/{p['id']}/react", json={"emoji": THUMBS}, headers=s1_h).status_code == 404
