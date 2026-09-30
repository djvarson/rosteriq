"""
The Try Demo reset wipes what visitors added to the shared demo venue — and
must never touch anything else.

Pins:
* every Postgres rule is a scoped DELETE bound to the demo venue / demo users,
  and none touches the venues or users tables (those are reset, not deleted)
* with a real venue's data alongside, a reset + reseed leaves EVERY non-demo
  record in the store unchanged
* visitor content is gone afterwards and the showcase is back
* the reset is throttled, never wipes a client still using the demo, and
  the seed only runs after a reset (or on an empty demo)
"""

import uuid
from datetime import datetime, timedelta

from fastapi.testclient import TestClient

from rosteriq.api import app
from rosteriq.database import get_db
from rosteriq.services.demo import DEMO_STAFF_USER_ID, DEMO_USER_ID, DEMO_VENUE_ID, seed_demo_environment
from rosteriq.services import demo_reset
from rosteriq.services.demo_reset import DU, PG_RULES, note_demo_activity, reset_and_seed, reset_demo_venue

PW = "Passw0rd!234"
D = DEMO_VENUE_ID


def test_every_postgres_rule_is_scoped_to_the_demo():
    for label, sql, params in PG_RULES:
        s = " ".join(sql.split()).upper()
        if label == "audit log detach":                 # the one UPDATE: re-scope, never delete
            assert s.startswith("UPDATE AUDIT_LOGS SET VENUE_ID = NULL") and s.endswith("WHERE VENUE_ID = %S"), label
        else:
            assert s.startswith("DELETE FROM "), label
        assert " WHERE " in s, label
        table = s.split()[2] if s.startswith("DELETE") else s.split()[1]
        assert table not in ("VENUES", "USERS"), label
        assert "%S" in s and params, label
        assert all(p == D or p == DU for p in params), (label, params)
        assert s.count("%S") == len(params), label


def _login(c, email):
    c.post("/api/auth/register", json={"email": email, "password": PW, "name": "U"})
    r = c.post("/api/auth/login", json={"email": email, "password": PW})
    return {"Authorization": f"Bearer {r.json()['access_token']}"}


def _real_venue(c):
    tag = uuid.uuid4().hex[:6]
    email = f"dr_real_{tag}@x.com"
    h = _login(c, email)
    vid = f"dr-real-{tag}"
    assert c.post("/venues", json={"id": vid, "name": "Real Pub", "state": "wa", "max_labour_pct": 30,
                                   "tanda_org_id": "", "created_at": "2026-07-01T00:00:00"},
                  headers=h).status_code in (200, 201)
    h = _login(c, email)
    for i in range(2):
        assert c.post("/employees", json={
            "id": f"{vid}-e{i}", "venue_id": vid, "name": f"Real Staff {i}", "employment_type": "casual",
            "award_level": "level_2", "state": "wa", "hourly_base_rate": "31.50",
            "created_at": "2026-07-01T00:00:00", "updated_at": "2026-07-01T00:00:00"}, headers=h).status_code == 200
    assert c.post("/rosters/generate", json={"venue_id": vid, "week_start": "2026-10-05"}, headers=h).status_code == 200
    assert c.post("/api/feed/posts", json={"venue_id": vid, "body": "Real venue post"}, headers=h).status_code in (200, 201)
    assert c.post("/api/announcements", json={"venue_id": vid, "title": "Real", "body": "Real news"},
                  headers=h).status_code == 200
    return h, vid


def _non_demo_snapshot(db, demo_rosters, demo_emps):
    """Every record in every store collection that does NOT belong to the demo."""
    def demo_owned(key, item):
        v = item.get("venue_id") if isinstance(item, dict) else getattr(item, "venue_id", None)
        if v == D:
            return True
        k = str(key)
        if k.startswith("demo:"):             # the reset's own throttle/activity stamps
            return True
        if k.startswith(D) or k in demo_rosters or k in demo_emps or k in DU:
            return True
        uid = item.get("user_id") if isinstance(item, dict) else None
        if isinstance(item, dict) and (item.get("details") or {}).get("demo_venue_id") == D:
            return True                                  # demo security rows the reset detached
        return uid in DU
    snap = {}
    for name, coll in vars(db).items():
        if isinstance(coll, dict):
            snap[name] = sorted(repr((k, v)) for k, v in coll.items() if not demo_owned(k, v))
        elif isinstance(coll, list):
            snap[name] = sorted(repr(x) for x in coll if not demo_owned(None, x))
    # login attempts / rate-limit input are appended by logins, not the reset
    snap.pop("_login_attempts", None)
    return snap


def test_reset_wipes_visitor_content_and_touches_nothing_else():
    c = TestClient(app)
    _login(c, f"dr_boot_{uuid.uuid4().hex[:6]}@x.com")            # platform owner out of the way
    real_h, real_vid = _real_venue(c)
    demo = {"Authorization": "Bearer " + c.post("/api/auth/demo").json()["access_token"]}
    staff = {"Authorization": "Bearer " + c.post("/api/auth/demo?as=staff").json()["access_token"]}

    # a visitor leaves their mark on the shared demo venue
    assert c.post("/api/feed/posts", json={"venue_id": D, "body": "VISITOR POST"}, headers=demo).status_code in (200, 201)
    assert c.post("/api/announcements", json={"venue_id": D, "title": "VISITOR", "body": "x"}, headers=demo).status_code == 200
    for path, body, h in [
        ("/api/menu/ingredients", {"venue_id": D, "name": "VISITOR INGREDIENT", "unit": "kg",
                                   "purchase_size": 1, "purchase_cost": 9}, demo),
        ("/api/checklists/templates/seed", {"venue_id": D}, demo),
        ("/api/inventory/stocktake/start", {"venue_id": D}, demo),
        ("/rosters/generate", {"venue_id": D, "week_start": "2026-10-12"}, demo),
        ("/api/me/leave", {"start_date": "2026-11-02", "end_date": "2026-11-03",
                           "leave_type": "annual", "reason": "VISITOR LEAVE"}, staff),
    ]:
        r = c.post(path, json=body, headers=h)
        assert r.status_code in (200, 201), (path, r.status_code, r.text[:160])

    db = get_db()
    demo_rosters = {k for k, r in db._rosters.items() if r.venue_id == D}
    demo_emps = {e.id for e in db._employees.values() if e.venue_id == D}
    before = _non_demo_snapshot(db, demo_rosters, demo_emps)

    report = reset_demo_venue(db, force=True)
    assert report is not None
    seed_demo_environment(db)

    demo_rosters |= {k for k, r in db._rosters.items() if r.venue_id == D}
    demo_emps |= {e.id for e in db._employees.values() if e.venue_id == D}
    after = _non_demo_snapshot(db, demo_rosters, demo_emps)
    changed = {n: sorted(set(before.get(n, [])) ^ set(after.get(n, []))) for n in set(before) | set(after)
               if before.get(n) != after.get(n)}
    assert not changed, f"non-demo data changed: {changed}"

    # the real venue is fully intact through its own API too
    r = c.get(f"/api/feed/posts?venue_id={real_vid}", headers=real_h)
    assert "Real venue post" in r.text, r.text[:200]

    # visitor content is gone; the showcase is back
    blob = repr([vars(db).get(n) for n in ("_feed_posts", "_announcements", "_ingredients", "_leave_requests")])
    for marker in ("VISITOR POST", "VISITOR INGREDIENT", "VISITOR LEAVE", "'title': 'VISITOR'"):
        assert marker not in blob, marker
    names = sorted(t["name"] for t in db._checklist_templates.values() if t.get("venue_id") == D)
    assert names == ["Closing", "Food Safety (daily)", "Opening"], names   # the seeded defaults, once each
    assert not [s for s in db._stocktakes.values() if s.get("venue_id") == D and s.get("status") == "open"]
    assert {p["id"] for p in db._feed_posts.values() if p.get("venue_id") == D} == {"demo-feed-001", "demo-feed-002"}
    assert len([e for e in db._employees.values() if e.venue_id == D]) == 6
    assert db.get_user_by_id(DEMO_USER_ID)["venue_ids"] == [D]


def test_reset_restores_the_shared_demo_account():
    db = get_db()
    seed_demo_environment(db)
    u = db.get_user_by_id(DEMO_USER_ID)
    u["name"] = "Renamed By A Visitor"
    u["api_key_hash"] = "planted"
    db.save_user(u)
    reset_demo_venue(db, force=True)
    u = db.get_user_by_id(DEMO_USER_ID)
    assert u["name"] == "Demo User" and not u.get("api_key_hash")
    assert db.get_user_by_id(DEMO_STAFF_USER_ID)["name"] == "Emma Thompson"


def test_reset_is_throttled():
    db = get_db()
    seed_demo_environment(db)
    t0 = datetime(2026, 9, 30, 10, 0, 0)
    assert reset_demo_venue(db, now=t0) is not None
    assert reset_demo_venue(db, now=t0 + timedelta(seconds=30)) is None
    assert reset_demo_venue(db, now=t0 + timedelta(seconds=61)) is not None


def _fresh_demo_state(db):
    for k in ("demo:last_reset", "demo:last_session", "demo:activity"):
        db.save_to_key(k, {})
    demo_reset._activity_written.clear()


A, B, C = "203.0.113.7", "198.51.100.9", "192.0.2.44"


def test_same_client_continuing_its_session_is_not_wiped():
    """A salesperson sets things up as manager, then opens the staff phone:
    the second Try Demo from the same client must not wipe the first's work —
    nor re-run the seed over it. A later visitor gets a clean demo."""
    db = get_db()
    _fresh_demo_state(db)
    t0 = datetime(2026, 9, 30, 10, 0, 0)
    assert reset_and_seed(db, A, now=t0)["status"] == "reset"
    db.save_announcement({"id": "an-pitch", "venue_id": D, "title": "Staff BBQ Friday", "body": "x",
                          "created_at": t0, "read_by": [], "pinned": False})
    post = db.get_feed_post("demo-feed-002")
    post["comments"] = [{"id": "c-pitch", "body": "pitch comment"}]
    db.save_feed_post(post)
    assert reset_and_seed(db, A, now=t0 + timedelta(minutes=40))["status"] == "kept"
    assert "an-pitch" in db._announcements
    assert db.get_feed_post("demo-feed-002")["comments"], "the seed re-asserted over the pitch"
    # A has gone quiet for 15 minutes: the next visitor gets a clean demo
    assert reset_and_seed(db, B, now=t0 + timedelta(minutes=55))["status"] == "reset"
    assert "an-pitch" not in db._announcements
    assert not db.get_feed_post("demo-feed-002")["comments"]


def test_a_visitor_mid_demo_is_not_wiped_by_a_newcomer():
    db = get_db()
    _fresh_demo_state(db)
    t0 = datetime(2026, 9, 30, 11, 0, 0)
    assert reset_and_seed(db, A, now=t0)["status"] == "reset"
    db.save_announcement({"id": "an-live", "venue_id": D, "title": "Mid-demo", "body": "x",
                          "created_at": t0, "read_by": [], "pinned": False})
    assert note_demo_activity(db, A, now=t0 + timedelta(minutes=20))      # A is still clicking around
    assert reset_and_seed(db, B, now=t0 + timedelta(minutes=25))["status"] == "kept"
    assert "an-live" in db._announcements
    # ...but a demo kept busy for over an hour still gets cleaned for a newcomer
    demo_reset._activity_written.clear()
    assert note_demo_activity(db, A, now=t0 + timedelta(minutes=65))
    assert reset_and_seed(db, C, now=t0 + timedelta(minutes=66))["status"] == "reset"
    assert "an-live" not in db._announcements


def test_an_empty_demo_is_seeded_even_without_a_reset():
    db = get_db()
    _fresh_demo_state(db)
    t0 = datetime(2026, 9, 30, 12, 0, 0)
    reset_and_seed(db, A, now=t0)
    for eid in [e.id for e in db._employees.values() if e.venue_id == D]:
        db._employees.pop(eid)
    assert reset_and_seed(db, A, now=t0 + timedelta(seconds=30))["status"] == "seeded"
    assert len([e for e in db._employees.values() if e.venue_id == D]) == 6


def test_a_visitor_waits_out_a_reset_in_progress():
    import threading
    db = get_db()
    _fresh_demo_state(db)
    demo_reset._lock.acquire()
    try:
        assert reset_and_seed(db, A, now=datetime(2026, 9, 30, 13, 0), wait_seconds=0)["status"] == "busy"
    finally:
        demo_reset._lock.release()
    demo_reset._lock.acquire()
    threading.Timer(0.3, demo_reset._lock.release).start()
    assert reset_and_seed(db, A, now=datetime(2026, 9, 30, 13, 5), wait_seconds=3)["status"] != "busy"


def test_demo_requests_mark_the_client_active_without_storing_its_address():
    c = TestClient(app)
    db = get_db()
    _fresh_demo_state(db)
    h = {"Authorization": "Bearer " + c.post("/api/auth/demo").json()["access_token"]}
    assert c.get(f"/api/feed/posts?venue_id={D}", headers=h).status_code == 200
    clients = (db.load_from_key("demo:activity") or {}).get("clients") or {}
    assert demo_reset._tag("testclient") in clients
    stamps = repr([db.load_from_key(k) for k in ("demo:activity", "demo:last_session")])
    assert "testclient" not in stamps


def test_throttle_fails_closed_when_the_store_cant_be_read(monkeypatch):
    db = get_db()
    seed_demo_environment(db)
    def boom(key):
        raise RuntimeError("kv down")
    monkeypatch.setattr(db, "load_from_key", boom)
    assert reset_demo_venue(db, now=datetime(2026, 9, 30, 12, 0, 0)) is None


def test_seed_never_overwrites_another_venues_record_with_a_demo_id():
    from rosteriq.models import Employee
    db = get_db()
    seed_demo_environment(db)
    real = Employee(id="demo-staff-003", venue_id="real-venue-x", name="Real Person", employment_type="casual",
                    award_level="level_2", state="wa", hourly_base_rate="40.00",
                    created_at="2026-07-01T00:00:00", updated_at="2026-07-01T00:00:00")
    reset_demo_venue(db, force=True)                    # demo staff gone...
    db.save_employee(real)                              # ...and the id was taken in the window
    seed_demo_environment(db)
    kept = db.get_employee("demo-staff-003")
    assert kept.venue_id == "real-venue-x" and kept.name == "Real Person"
    assert len([e for e in db._employees.values() if e.venue_id == D]) == 5


def test_api_reserves_demo_ids_for_the_seed():
    c = TestClient(app)
    email = f"dr_boot3_{uuid.uuid4().hex[:6]}@x.com"
    h = _login(c, email)
    vid = f"dr-res-{uuid.uuid4().hex[:6]}"
    c.post("/venues", json={"id": vid, "name": "R", "state": "wa", "max_labour_pct": 30,
                            "tanda_org_id": "", "created_at": "2026-07-01T00:00:00"}, headers=h)
    h = _login(c, email)
    def post(eid):
        return c.post("/employees", json={"id": eid, "venue_id": vid, "name": "X",
                                          "employment_type": "casual", "award_level": "level_2", "state": "wa",
                                          "hourly_base_rate": "31.50", "created_at": "2026-07-01T00:00:00",
                                          "updated_at": "2026-07-01T00:00:00"}, headers=h)
    r = post(f"demo-new-{uuid.uuid4().hex[:6]}")
    assert r.status_code == 422, r.text                          # can't create under a demo- id
    seed_demo_environment(get_db())
    assert post("demo-staff-001").status_code in (404, 422)      # nor take over the seed's record
    # a record this venue already holds under such an id stays editable
    from rosteriq.models import Employee
    legacy = f"demo-legacy-{uuid.uuid4().hex[:6]}"
    get_db().save_employee(Employee(id=legacy, venue_id=vid, name="Legacy", employment_type="casual",
                                    award_level="level_2", state="wa", hourly_base_rate="31.50",
                                    created_at="2026-07-01T00:00:00", updated_at="2026-07-01T00:00:00"))
    assert post(legacy).status_code == 200


def test_security_rows_leave_the_demo_view_and_privacy_rows_are_cleared():
    db = get_db()
    seed_demo_environment(db)
    db._audit_logs.append({"venue_id": D, "action": "access.denied", "user_id": None,
                           "details": {"category": "security", "ip": "203.0.113.50", "note": "visitor text"}})
    if isinstance(getattr(db, "_consents", None), dict):
        db._consents[DEMO_STAFF_USER_ID] = {"analytics": True}
    reset_demo_venue(db, force=True)
    rows = [r for r in db._audit_logs if (r.get("details") or {}).get("note") == "visitor text"]
    assert rows and rows[0]["venue_id"] is None and rows[0]["details"]["demo_venue_id"] == D
    assert not [r for r in db._audit_logs if r.get("venue_id") == D]
    assert DEMO_STAFF_USER_ID not in (getattr(db, "_consents", None) or {})
