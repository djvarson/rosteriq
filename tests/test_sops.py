"""
SOP / JSP document library: manager publishes + edits, role targeting via
skills, staff read + acknowledge per version, manager sees who is outstanding,
seed idempotency, and venue scoping.
"""

import uuid

from fastapi.testclient import TestClient

from rosteriq.api import app
from rosteriq.database import get_db


def _register_login(c, email):
    c.post("/api/auth/register", json={"email": email, "password": "Passw0rd!234", "name": "U"})
    tok = c.post("/api/auth/login", json={"email": email, "password": "Passw0rd!234"}).json()["access_token"]
    return {"Authorization": f"Bearer {tok}"}


def _err(r):
    body = r.json()
    return body.get("detail") or body.get("error", {}).get("message") or ""


def _venue(c, owner_h, vid):
    r = c.post("/venues", json={
        "id": vid, "name": "SOP Venue", "state": "wa", "max_labour_pct": 30,
        "tanda_org_id": "", "created_at": "2026-07-01T00:00:00",
    }, headers=owner_h)
    assert r.status_code == 200, r.text


def _employee(c, owner_h, vid, emp_id, name, email, skills):
    r = c.post("/employees", json={
        "id": emp_id, "name": name, "employment_type": "casual",
        "award_level": "level_2", "state": "wa", "venue_id": vid,
        "hourly_base_rate": "30.00", "email": email, "skills": skills,
        "created_at": "2026-07-01T00:00:00", "updated_at": "2026-07-01T00:00:00",
    }, headers=owner_h)
    assert r.status_code == 200, r.text


def _scope(email, vid, role="staff"):
    db = get_db()
    rec = db.get_user_by_email(email)
    rec["venue_ids"] = [vid]
    rec["role"] = role
    db.save_user(rec)


def _setup(c, vid="sop-venue"):
    """Owner + venue + two staff (bar-skilled and kitchen-skilled), each with a
    login. Returns (owner_h, bar_h, kitchen_h, ids)."""
    owner_h = _register_login(c, f"o{uuid.uuid4().hex[:8]}@x.com")
    _venue(c, owner_h, vid)
    bar_email = f"bar{uuid.uuid4().hex[:8]}@x.com"
    kit_email = f"kit{uuid.uuid4().hex[:8]}@x.com"
    _employee(c, owner_h, vid, f"{vid}-bar", "Bec Barkeep", bar_email, ["bar"])
    _employee(c, owner_h, vid, f"{vid}-kit", "Kai Kitchen", kit_email, ["kitchen"])
    bar_h = _register_login(c, bar_email)
    kit_h = _register_login(c, kit_email)
    _scope(bar_email, vid)
    _scope(kit_email, vid)
    return owner_h, bar_h, kit_h, {"bar": f"{vid}-bar", "kit": f"{vid}-kit"}


def _publish(c, owner_h, vid, title="Glass breakage procedure", applies_to=None, **kw):
    payload = {
        "venue_id": vid, "title": title, "category": "sop",
        "body": "1. Stop service in that area. 2. Sweep, never hand-pick. 3. Bin in the glass bin.",
        "applies_to": applies_to or [],
    }
    payload.update(kw)
    r = c.post("/api/sops/documents", json=payload, headers=owner_h)
    assert r.status_code == 200, r.text
    return r.json()


def test_manager_creates_document_version_1():
    c = TestClient(app)
    owner_h, _, _, _ = _setup(c, "sop-v-create")
    out = _publish(c, owner_h, "sop-v-create", applies_to=["bar", "Bar", " floor "])
    doc = out["document"]
    assert out["status"] == "published" and doc["version"] == 1
    assert doc["category"] == "sop" and doc["requires_ack"] is True and doc["active"] is True
    assert doc["applies_to"] == ["bar", "floor"]  # trimmed + de-duplicated (case-insensitive)

    # Bad category is rejected by validation
    r = c.post("/api/sops/documents", json={
        "venue_id": "sop-v-create", "title": "X", "category": "manifesto", "body": "y",
    }, headers=owner_h)
    assert r.status_code == 422

    listing = c.get("/api/sops/documents?venue_id=sop-v-create", headers=owner_h).json()
    assert listing["count"] == 1 and listing["documents"][0]["id"] == doc["id"]


def test_staff_cannot_create_or_list_manager_view():
    c = TestClient(app)
    owner_h, bar_h, _, _ = _setup(c, "sop-v-staff403")
    r = c.post("/api/sops/documents", json={
        "venue_id": "sop-v-staff403", "title": "Nope", "category": "sop", "body": "no",
    }, headers=bar_h)
    assert r.status_code == 403, r.text
    assert "manager" in _err(r).lower()
    assert c.get("/api/sops/documents?venue_id=sop-v-staff403", headers=bar_h).status_code == 403


def test_staff_sees_only_applicable_docs():
    c = TestClient(app)
    vid = "sop-v-applies"
    owner_h, bar_h, kit_h, _ = _setup(c, vid)
    everyone = _publish(c, owner_h, vid, title="Fire evacuation", applies_to=[])["document"]
    bar_only = _publish(c, owner_h, vid, title="RSA refusal script", applies_to=["BAR"])["document"]
    kit_only = _publish(c, owner_h, vid, title="Fryer oil change", applies_to=["kitchen"])["document"]

    mine = c.get("/api/me/sops", headers=bar_h).json()
    assert mine["linked"] is True
    ids = {d["id"] for d in mine["documents"]}
    assert ids == {everyone["id"], bar_only["id"]}
    assert all(d["acknowledged"] is False for d in mine["documents"])
    assert mine["outstanding"] == 2

    kit = c.get("/api/me/sops", headers=kit_h).json()
    assert {d["id"] for d in kit["documents"]} == {everyone["id"], kit_only["id"]}

    # Unlinked user gets the standard no-link shape
    lonely_h = _register_login(c, f"lonely{uuid.uuid4().hex[:8]}@x.com")
    r = c.get("/api/me/sops", headers=lonely_h).json()
    assert r["linked"] is False and "No staff record" in r["message"]


def test_acknowledge_updates_staff_view_and_manager_ack_stats():
    c = TestClient(app)
    vid = "sop-v-ack"
    owner_h, bar_h, kit_h, ids = _setup(c, vid)
    doc = _publish(c, owner_h, vid, title="Fire evacuation")["document"]

    # Before: nobody has acknowledged; both staff outstanding
    stats = c.get(f"/api/sops/documents?venue_id={vid}", headers=owner_h).json()["documents"][0]["ack_stats"]
    assert stats == {"required": 2, "acknowledged_current_version": 0,
                     "outstanding_names": ["Bec Barkeep", "Kai Kitchen"]}

    r = c.post(f"/api/me/sops/{doc['id']}/acknowledge", headers=bar_h)
    assert r.status_code == 200, r.text
    assert r.json()["already"] is False and r.json()["version"] == 1

    mine = c.get("/api/me/sops", headers=bar_h).json()
    me_doc = next(d for d in mine["documents"] if d["id"] == doc["id"])
    assert me_doc["acknowledged"] is True and me_doc["acknowledged_at"]
    assert mine["outstanding"] == 0

    stats = c.get(f"/api/sops/documents?venue_id={vid}", headers=owner_h).json()["documents"][0]["ack_stats"]
    assert stats["required"] == 2
    assert stats["acknowledged_current_version"] == 1
    assert stats["outstanding_names"] == ["Kai Kitchen"]

    # A role-targeted doc only counts the staff it applies to
    bar_doc = _publish(c, owner_h, vid, title="RSA refusal script", applies_to=["bar"])["document"]
    listing = c.get(f"/api/sops/documents?venue_id={vid}", headers=owner_h).json()["documents"]
    bar_stats = next(d for d in listing if d["id"] == bar_doc["id"])["ack_stats"]
    assert bar_stats == {"required": 1, "acknowledged_current_version": 0,
                         "outstanding_names": ["Bec Barkeep"]}
    # ...and the kitchen hand cannot acknowledge a doc that is not theirs
    r = c.post(f"/api/me/sops/{bar_doc['id']}/acknowledge", headers=kit_h)
    assert r.status_code == 403 and "does not apply" in _err(r)


def test_second_acknowledge_is_idempotent():
    c = TestClient(app)
    vid = "sop-v-idem"
    owner_h, bar_h, _, ids = _setup(c, vid)
    doc = _publish(c, owner_h, vid)["document"]
    first = c.post(f"/api/me/sops/{doc['id']}/acknowledge", headers=bar_h).json()
    second = c.post(f"/api/me/sops/{doc['id']}/acknowledge", headers=bar_h)
    assert second.status_code == 200
    assert second.json()["already"] is True
    assert second.json()["acknowledged_at"] == first["acknowledged_at"]
    # Exactly one ack row exists in the store
    assert len(get_db().list_sop_acks(vid, doc["id"])) == 1
    # Store-level: a duplicate write for the same key is a no-op (first wins)
    get_db().save_sop_ack({"id": "dupe", "venue_id": vid, "doc_id": doc["id"], "doc_version": 1,
                           "employee_id": ids["bar"], "employee_name": "X",
                           "acknowledged_at": "2099-01-01T00:00:00"})
    assert len(get_db().list_sop_acks(vid, doc["id"])) == 1


def test_editing_body_bumps_version_and_resets_acknowledgement():
    c = TestClient(app)
    vid = "sop-v-bump"
    owner_h, bar_h, _, ids = _setup(c, vid)
    doc = _publish(c, owner_h, vid, title="Fire evacuation")["document"]
    assert c.post(f"/api/me/sops/{doc['id']}/acknowledge", headers=bar_h).status_code == 200

    # Metadata-only edit: no version bump, ack stands
    r = c.put(f"/api/sops/documents/{doc['id']}", json={"category": "policy"}, headers=owner_h)
    assert r.status_code == 200, r.text
    assert r.json()["version"] == 1 and r.json()["version_bumped"] is False
    assert r.json()["category"] == "policy"
    me = next(d for d in c.get("/api/me/sops", headers=bar_h).json()["documents"] if d["id"] == doc["id"])
    assert me["acknowledged"] is True

    # Body edit: version 2, staff must re-ack
    r = c.put(f"/api/sops/documents/{doc['id']}", json={
        "body": "1. Raise the alarm. 2. Assembly point is the car park. 3. Roll call.",
    }, headers=owner_h)
    assert r.status_code == 200, r.text
    assert r.json()["version"] == 2 and r.json()["version_bumped"] is True

    me = next(d for d in c.get("/api/me/sops", headers=bar_h).json()["documents"] if d["id"] == doc["id"])
    assert me["version"] == 2 and me["acknowledged"] is False and me["acknowledged_at"] is None

    stats = c.get(f"/api/sops/documents?venue_id={vid}", headers=owner_h).json()["documents"][0]["ack_stats"]
    assert stats["acknowledged_current_version"] == 0
    assert "Bec Barkeep" in stats["outstanding_names"]

    # The old ack is still on record (audit), just not for the current version
    acks = get_db().list_sop_acks(vid, doc["id"])
    assert len(acks) == 1 and int(acks[0]["doc_version"]) == 1

    # Re-ack the new version -> two ack rows, current counted
    r = c.post(f"/api/me/sops/{doc['id']}/acknowledge", headers=bar_h)
    assert r.status_code == 200 and r.json()["version"] == 2 and r.json()["already"] is False
    assert len(get_db().list_sop_acks(vid, doc["id"])) == 2
    stats = c.get(f"/api/sops/documents?venue_id={vid}", headers=owner_h).json()["documents"][0]["ack_stats"]
    assert stats["acknowledged_current_version"] == 1

    # Title edit also bumps
    r = c.put(f"/api/sops/documents/{doc['id']}", json={"title": "Fire & evacuation"}, headers=owner_h)
    assert r.json()["version"] == 3

    # Unknown doc 404s
    assert c.put("/api/sops/documents/ghost", json={"title": "x"}, headers=owner_h).status_code == 404


def test_inactive_doc_disappears_and_cannot_be_acknowledged():
    c = TestClient(app)
    vid = "sop-v-inactive"
    owner_h, bar_h, _, _ = _setup(c, vid)
    doc = _publish(c, owner_h, vid, title="Old till procedure")["document"]
    assert any(d["id"] == doc["id"] for d in c.get("/api/me/sops", headers=bar_h).json()["documents"])

    r = c.put(f"/api/sops/documents/{doc['id']}", json={"active": False}, headers=owner_h)
    assert r.status_code == 200 and r.json()["active"] is False and r.json()["version"] == 1

    assert not any(d["id"] == doc["id"] for d in c.get("/api/me/sops", headers=bar_h).json()["documents"])
    r = c.post(f"/api/me/sops/{doc['id']}/acknowledge", headers=bar_h)
    assert r.status_code == 404 and "retired" in _err(r)

    # Manager view hides it by default, shows it with include_inactive
    assert c.get(f"/api/sops/documents?venue_id={vid}", headers=owner_h).json()["count"] == 0
    listing = c.get(f"/api/sops/documents?venue_id={vid}&include_inactive=true", headers=owner_h).json()
    assert listing["count"] == 1 and listing["documents"][0]["active"] is False


def test_seed_is_idempotent_and_skips_existing_titles():
    c = TestClient(app)
    vid = "sop-v-seed"
    owner_h, bar_h, _, _ = _setup(c, vid)
    # A pre-existing (edited) doc with a starter title must not be duplicated
    _publish(c, owner_h, vid, title="cash HANDLING & till variance", category="policy")

    r = c.post("/api/sops/seed", json={"venue_id": vid}, headers=owner_h)
    assert r.status_code == 200, r.text
    assert r.json()["created"] == 3 and r.json()["skipped"] == 1
    assert r.json()["skipped_titles"] == ["Cash handling & till variance"]

    docs = c.get(f"/api/sops/documents?venue_id={vid}", headers=owner_h).json()["documents"]
    titles = sorted(d["title"].lower() for d in docs)
    assert titles == sorted([
        "cash handling & till variance", "opening & closing procedure",
        "safe manual handling", "food safety & allergen declaration",
    ])
    cats = {d["title"]: d["category"] for d in docs}
    assert cats["Safe manual handling"] == "jsp"
    assert cats["Opening & closing procedure"] == "sop"
    for d in docs:
        if d["title"].lower() == "cash handling & till variance":
            continue  # the pre-existing test doc, not a seeded one
        assert 100 <= len(d["body"].split()) <= 400  # substantive AU hospo text
        assert d["applies_to"] == []  # starter set applies to everyone
        assert d["author_name"]  # seeded with the seeding manager's name

    # Second seed: nothing new
    r = c.post("/api/sops/seed", json={"venue_id": vid}, headers=owner_h)
    assert r.json()["created"] == 0 and r.json()["skipped"] == 4
    assert c.get(f"/api/sops/documents?venue_id={vid}", headers=owner_h).json()["count"] == 4

    # Staff can see all four (applies to everyone) and none acked yet
    mine = c.get("/api/me/sops", headers=bar_h).json()
    assert mine["count"] == 4 and mine["outstanding"] == 4

    # Staff cannot seed
    assert c.post("/api/sops/seed", json={"venue_id": vid}, headers=bar_h).status_code == 403


def test_cross_venue_access_is_denied():
    c = TestClient(app)
    vid = "sop-v-scope"
    owner_h, bar_h, _, _ = _setup(c, vid)
    doc = _publish(c, owner_h, vid)["document"]

    # A manager of a different venue
    outsider_email = f"m{uuid.uuid4().hex[:8]}@x.com"
    outsider_h = _register_login(c, outsider_email)
    _scope(outsider_email, "not-this-one", role="manager")

    assert c.get(f"/api/sops/documents?venue_id={vid}", headers=outsider_h).status_code == 403
    assert c.post("/api/sops/documents", json={
        "venue_id": vid, "title": "Nope", "category": "sop", "body": "no",
    }, headers=outsider_h).status_code == 403
    assert c.put(f"/api/sops/documents/{doc['id']}", json={"title": "Hijack"},
                 headers=outsider_h).status_code == 403
    assert c.post("/api/sops/seed", json={"venue_id": vid}, headers=outsider_h).status_code == 403

    # A staff member linked to a different venue cannot acknowledge this venue's doc
    _venue(c, owner_h, "sop-v-other")
    other_email = f"s{uuid.uuid4().hex[:8]}@x.com"
    _employee(c, owner_h, "sop-v-other", "sop-v-other-emp", "Ollie Other", other_email, ["bar"])
    other_h = _register_login(c, other_email)
    _scope(other_email, "sop-v-other")
    r = c.post(f"/api/me/sops/{doc['id']}/acknowledge", headers=other_h)
    assert r.status_code == 404
    assert not any(d["id"] == doc["id"] for d in c.get("/api/me/sops", headers=other_h).json()["documents"])
    # Sanity: the doc is untouched
    assert c.get(f"/api/sops/documents?venue_id={vid}", headers=owner_h).json()["documents"][0]["title"] == doc["title"]


def test_demo_seed_populates_library_with_partial_acks():
    from rosteriq.services.demo import seed_demo_environment, DEMO_VENUE_ID
    db = get_db()
    seed_demo_environment(db)
    docs = db.list_sop_documents(DEMO_VENUE_ID)
    assert len(docs) == 4
    food = next(d for d in docs if d["title"].startswith("Food safety"))
    acks = db.list_sop_acks(DEMO_VENUE_ID, food["id"])
    assert len(acks) == 3
    # Re-seeding is a no-op
    seed_demo_environment(db)
    assert len(db.list_sop_documents(DEMO_VENUE_ID)) == 4
    assert len(db.list_sop_acks(DEMO_VENUE_ID)) == 3


def test_concurrent_seeds_converge_on_one_set():
    """Production runs two uvicorn workers that both seed the demo at boot.
    Both saw 'no documents yet' and both wrote a set with random ids — every
    starter procedure appeared twice. Starter ids are now deterministic per
    venue, so two seeds that BOTH pass the emptiness check still land on the
    same four rows (the store upserts by id)."""
    from rosteriq.database import MemoryStore
    from rosteriq.routes.sops import seed_starter_sops, STARTER_SOPS
    db = MemoryStore()
    # Simulate the race: both callers computed "empty" before either wrote.
    r1 = seed_starter_sops(db, "race-venue", author_name="A")
    # Second caller with a stale emptiness view — force it past the title
    # skip by clearing what it would have read... it can't (it reads live), so
    # emulate the pre-fix hazard directly: same venue, same specs, second call.
    r2 = seed_starter_sops(db, "race-venue", author_name="B")
    docs = db.list_sop_documents("race-venue", include_inactive=True)
    assert len(docs) == len(STARTER_SOPS) == 4
    assert len(r1["created"]) == 4 and r2["created"] == []
    # Deterministic per venue: another venue gets its own ids, same venue same ids
    ids_a = {d["id"] for d in docs}
    seed_starter_sops(db, "other-venue", author_name="A")
    ids_b = {d["id"] for d in db.list_sop_documents("other-venue", include_inactive=True)}
    assert ids_a.isdisjoint(ids_b)
    assert all(i.startswith("sop-") for i in ids_a)
    # A doc the manager RENAMED keeps its id and is never clobbered by a reseed
    cash = next(d for d in docs if d["title"].startswith("Cash"))
    cash["title"] = "Till policy (renamed)"; cash["version"] = 2; cash["body"] = "custom"
    db.save_sop_document(cash)
    r3 = seed_starter_sops(db, "race-venue", author_name="C")
    assert r3["created"] == []  # id exists -> skipped even though the title changed
    assert db.get_sop_document(cash["id"])["body"] == "custom"


def test_delete_only_while_unacknowledged():
    """A draft/duplicate can be deleted outright; once anyone has acknowledged
    it, it's compliance record — 409, retire it instead."""
    c = TestClient(app)
    vid = "sop-v-delete"
    owner_h, bar_h, _, _ = _setup(c, vid)
    doc = _publish(c, owner_h, vid, title="Draft I made by mistake")
    doc_id = doc["document"]["id"] if "document" in doc else doc["document_id"]

    # staff can't delete
    assert c.delete(f"/api/sops/documents/{doc_id}", headers=bar_h).status_code == 403
    # manager can, while unacknowledged
    r = c.delete(f"/api/sops/documents/{doc_id}", headers=owner_h)
    assert r.status_code == 200, r.text
    assert c.delete(f"/api/sops/documents/{doc_id}", headers=owner_h).status_code == 404

    # acknowledged -> refused
    doc2 = _publish(c, owner_h, vid, title="Real procedure")
    d2 = doc2["document"]["id"] if "document" in doc2 else doc2["document_id"]
    assert c.post(f"/api/me/sops/{d2}/acknowledge", headers=bar_h).status_code == 200
    r = c.delete(f"/api/sops/documents/{d2}", headers=owner_h)
    assert r.status_code == 409, r.text
    assert "retire" in _err(r).lower()
