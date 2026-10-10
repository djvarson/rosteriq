"""
One live roster per venue-week.

A week can hold several rosters (every Generate, the CSV import, publishing).
Staff see only the published roster, or the imported one when nothing is
published — never a draft, never two rosters at once. Manager views take the
published roster, else the newest, and never add a week's rosters together.
"""

import uuid
from datetime import date, datetime, time as dtime, timedelta

from fastapi.testclient import TestClient

from rosteriq.api import app
from rosteriq.database import get_db
from rosteriq.models import Roster, Shift, ShiftStatus
from rosteriq.services.clock import venue_today
from rosteriq.services.roster_publisher import publisher

PW = "Passw0rd!234"


def _login(c, email):
    c.post("/api/auth/register", json={"email": email, "password": PW, "name": "U"})
    tok = c.post("/api/auth/login", json={"email": email, "password": PW}).json()["access_token"]
    return {"Authorization": f"Bearer {tok}"}


def _world(n_staff=1):
    """A non-owner manager, a WA venue, n staff with rates; the first one is
    linked to a staff login."""
    c = TestClient(app)
    _login(c, f"a4_boot_{uuid.uuid4().hex[:6]}@x.com")
    email = f"a4m{uuid.uuid4().hex[:8]}@x.com"
    h = _login(c, email)
    vid = f"a4-{uuid.uuid4().hex[:6]}"
    assert c.post("/venues", json={"id": vid, "name": vid, "state": "wa", "max_labour_pct": 30,
                                   "tanda_org_id": "", "created_at": "2026-07-01T00:00:00"},
                  headers=h).status_code in (200, 201)
    mgr = _login(c, email)
    staff_email = f"a4s{uuid.uuid4().hex[:8]}@x.com"
    emp_ids = []
    for i in range(n_staff):
        body = {"id": f"{vid}-e{i}", "venue_id": vid, "name": f"Casual {i}",
                "employment_type": "casual", "award_level": "level_2", "state": "wa",
                "hourly_base_rate": "30.00", "skills": ["bar"],
                "created_at": "2026-07-01T00:00:00", "updated_at": "2026-07-01T00:00:00"}
        if i == 0:
            body["email"] = staff_email
        assert c.post("/employees", json=body, headers=mgr).status_code == 200, body
        emp_ids.append(body["id"])
    staff_h = _login(c, staff_email)
    db = get_db()
    rec = db.get_user_by_email(staff_email)
    rec["venue_ids"], rec["role"] = [vid], "staff"
    db.save_user(rec)
    return c, mgr, staff_h, vid, emp_ids


def _monday(d):
    return d - timedelta(days=d.weekday())


def _generate(c, mgr, vid, week_start):
    r = c.post("/rosters/generate", json={"venue_id": vid, "week_start": week_start.isoformat()},
               headers=mgr)
    assert r.status_code == 200, r.text
    return r.json()


def _my(c, staff_h):
    body = c.get("/api/me/shifts", headers=staff_h).json()
    assert body["linked"], body
    return body["shifts"]


def _seed(vid, rid, week_start, shifts, created_at=None, state=None):
    db = get_db()
    db.save_roster(Roster(id=rid, venue_id=vid, week_start=week_start,
                          week_end=week_start + timedelta(days=6), shifts=shifts,
                          total_cost=None, created_at=created_at or datetime.utcnow()))
    if state:
        db.update_roster_state(rid, state, "seed", "test")


def _shift(sid, emp, day, start=dtime(17, 0), end=dtime(22, 0), cost=None):
    return Shift(id=sid, employee_id=emp, date=day, start_time=start, end_time=end,
                 break_minutes=0, status=ShiftStatus.scheduled, role="bar", cost=cost)


def _csv(emp_name, days, start="11:00", end="17:00"):
    rows = ["Name,Date,Start,End,Role,Break"]
    rows += [f"{emp_name},{d.strftime('%d/%m/%Y')},{start},{end},Bar,0" for d in days]
    return "\n".join(rows) + "\n"


# --- staff-facing: /my -------------------------------------------------------

def test_regenerating_a_week_never_puts_drafts_or_duplicates_on_my():
    c, mgr, staff_h, vid, (emp,) = _world()
    next_monday = _monday(venue_today(vid, get_db())) + timedelta(days=7)
    r1 = _generate(c, mgr, vid, next_monday)
    r2 = _generate(c, mgr, vid, next_monday)
    assert [s for s in r2["shifts"] if s["employee_id"] == emp], "generator rostered nobody"

    # Two unpublished drafts: nothing on the phone
    assert _my(c, staff_h) == []

    db = get_db()
    assert db.get_roster_state(r1["id"]) == "superseded"
    assert db.get_roster_state(r2["id"]) == "draft"

    r = c.post(f"/api/v1/rosters/{r2['id']}/publish", json={"skip_approval": True}, headers=mgr)
    assert r.status_code == 200 and r.json()["success"], r.text

    mine = _my(c, staff_h)
    horizon_end = venue_today(vid, db) + timedelta(days=14)
    expected = {s["id"] for s in r2["shifts"]
                if s["employee_id"] == emp and s["date"] <= horizon_end.isoformat()}
    assert {s["id"] for s in mine} == expected
    keys = [(s["date"], s["start"], s["end"]) for s in mine]
    assert len(keys) == len(set(keys)), "duplicate shift on /my"


def test_a_draft_alone_is_never_on_my():
    c, mgr, staff_h, vid, (emp,) = _world()
    tomorrow = venue_today(vid, get_db()) + timedelta(days=1)
    _seed(vid, f"{vid}-draft", _monday(tomorrow), [_shift(f"{vid}-d1", emp, tomorrow)])
    assert _my(c, staff_h) == []
    # ...and its shift can't be offered for cover
    r = c.post(f"/api/me/shifts/{vid}-d1/cover", json={"reason": "x"}, headers=staff_h)
    assert r.status_code == 404, r.text


def test_an_imported_week_is_on_my_before_anything_is_published():
    c, mgr, staff_h, vid, (emp,) = _world()
    today = venue_today(vid, get_db())
    day = today + timedelta(days=1)
    r = c.post("/api/setup/import-roster", json={"venue_id": vid, "content": _csv("Casual 0", [day])},
               headers=mgr)
    assert r.status_code == 200 and r.json()["shifts_imported"] == 1, r.text
    mine = _my(c, staff_h)
    assert [(s["date"], s["start"]) for s in mine] == [(day.isoformat(), "11:00")]

    # A generated draft for the same week changes nothing on the phone
    _generate(c, mgr, vid, _monday(day))
    mine = _my(c, staff_h)
    assert [(s["date"], s["start"]) for s in mine] == [(day.isoformat(), "11:00")]


def test_publishing_replaces_the_imported_week_on_my_and_the_kiosk():
    c, mgr, staff_h, vid, (emp,) = _world()
    db = get_db()
    today = venue_today(vid, db)
    monday = _monday(today)
    _seed(vid, f"imported-{vid}-{monday.isoformat()}", monday,
          [_shift(f"{vid}-imp", emp, today, dtime(11, 0), dtime(17, 0))])
    _seed(vid, f"{vid}-gen", monday,
          [_shift(f"{vid}-gen1", emp, today, dtime(18, 0), dtime(23, 0))])

    board = c.get(f"/api/clock/board?venue_id={vid}", headers=mgr).json()
    me = next(s for s in board["staff"] if s["employee_id"] == emp)
    assert me["rostered"]["start"] == "11:00"            # the imported week, not the draft

    publisher.transition_state(f"{vid}-gen", "published", "test", "test")
    board = c.get(f"/api/clock/board?venue_id={vid}", headers=mgr).json()
    me = next(s for s in board["staff"] if s["employee_id"] == emp)
    assert me["rostered"]["start"] == "18:00"
    assert [s["start"] for s in _my(c, staff_h) if s["date"] == today.isoformat()] == ["18:00"]


def test_publishing_supersedes_the_weeks_other_rosters_but_keeps_them():
    c, mgr, staff_h, vid, (emp,) = _world()
    db = get_db()
    tomorrow = venue_today(vid, db) + timedelta(days=1)
    ws = _monday(tomorrow)
    _seed(vid, f"{vid}-old-pub", ws, [_shift(f"{vid}-o1", emp, tomorrow, dtime(9, 0), dtime(12, 0))],
          created_at=datetime(2026, 1, 1), state="published")
    _seed(vid, f"{vid}-new", ws, [_shift(f"{vid}-n1", emp, tomorrow, dtime(13, 0), dtime(16, 0))])
    imported = f"imported-{vid}-{ws.isoformat()}"
    _seed(vid, imported, ws, [_shift(f"{vid}-i1", emp, tomorrow)])

    publisher.transition_state(f"{vid}-new", "published", "test", "test")
    assert db.get_roster_state(f"{vid}-old-pub") == "superseded"
    assert db.get_roster_state(imported) == "draft"      # imported rosters stay as they were
    assert db.get_roster(f"{vid}-old-pub") is not None    # history, not deleted
    assert [s["start"] for s in _my(c, staff_h)] == ["13:00"]


def test_a_cover_on_a_shift_that_left_the_live_roster_cannot_be_claimed():
    c, mgr, staff_h, vid, (a, b) = _world(n_staff=2)
    db = get_db()
    tomorrow = venue_today(vid, db) + timedelta(days=1)
    ws = _monday(tomorrow)
    _seed(vid, f"{vid}-v1", ws, [_shift(f"{vid}-v1s", a, tomorrow)], state="published")
    cover = c.post(f"/api/me/shifts/{vid}-v1s/cover", json={"reason": "sick"}, headers=staff_h)
    assert cover.status_code == 200, cover.text
    # The manager republishes the week; the old shift is gone from the live roster
    _seed(vid, f"{vid}-v2", ws, [_shift(f"{vid}-v2s", a, tomorrow, dtime(10, 0), dtime(14, 0))])
    publisher.transition_state(f"{vid}-v2", "published", "test", "test")

    b_email = f"a4b{uuid.uuid4().hex[:8]}@x.com"
    emp_b = db.get_employee(b)
    emp_b.email = b_email
    db.save_employee(emp_b)
    b_h = _login(c, b_email)
    rec = db.get_user_by_email(b_email)
    rec["venue_ids"], rec["role"] = [vid], "staff"
    db.save_user(rec)
    board = c.get("/api/me/cover", headers=b_h).json()
    assert board["up_for_grabs"] == []
    r = c.post(f"/api/me/cover/{cover.json()['cover_id']}/claim", headers=b_h)
    assert r.status_code == 409, r.text


# --- manager-facing: briefing / snapshot / dashboard ------------------------

def test_briefing_and_snapshot_count_one_roster_after_regenerating_twice():
    c, mgr, staff_h, vid, emps = _world(n_staff=3)
    db = get_db()
    today = venue_today(vid, db)
    monday = _monday(today)
    _generate(c, mgr, vid, monday)
    latest = _generate(c, mgr, vid, monday)

    week_cost = round(sum(float(s["cost"] or 0) for s in latest["shifts"]), 2)
    today_cost = round(sum(float(s["cost"] or 0) for s in latest["shifts"]
                           if s["date"] == today.isoformat()), 2)
    today_count = len([s for s in latest["shifts"] if s["date"] == today.isoformat()])
    assert week_cost > 0

    snap = c.get(f"/api/snapshot?venue_id={vid}&start_date={monday.isoformat()}"
                 f"&end_date={(monday + timedelta(days=6)).isoformat()}", headers=mgr).json()
    assert round(sum(d["labour"] for d in snap["days"]), 2) == week_cost

    brief = c.get(f"/api/briefing?venue_id={vid}", headers=mgr).json()
    assert brief["today"]["staff_rostered"] == today_count
    assert brief["today"]["labour_cost"] == today_cost

    # The dashboard's week view asks for the current roster per week only
    cur = c.get(f"/rosters?venue_id={vid}&current=true", headers=mgr).json()["items"]
    assert [r["id"] for r in cur] == [latest["id"]]
    listed = c.get(f"/rosters?venue_id={vid}", headers=mgr).json()["items"]
    assert [r["id"] for r in listed] == [latest["id"]]      # the superseded draft is history


def test_manager_views_prefer_the_published_roster_over_a_newer_draft():
    c, mgr, staff_h, vid, (emp,) = _world()
    db = get_db()
    today = venue_today(vid, db)
    monday = _monday(today)
    _seed(vid, f"{vid}-pub", monday, [_shift(f"{vid}-p", emp, today, cost=100)],
          created_at=datetime(2026, 1, 1), state="published")
    _seed(vid, f"{vid}-wip", monday, [_shift(f"{vid}-w", emp, today, cost=40)])
    brief = c.get(f"/api/briefing?venue_id={vid}", headers=mgr).json()
    assert brief["today"]["labour_cost"] == 100.0 and brief["today"]["staff_rostered"] == 1
    # get_shifts (AI tools, live labour) reads the same one roster per week
    assert [s.id for s in db.get_shifts(vid, today, today)] == [f"{vid}-p"]


def test_briefing_today_labour_matches_the_snapshot_for_an_imported_roster():
    c, mgr, staff_h, vid, (emp,) = _world()
    today = venue_today(vid, get_db())
    r = c.post("/api/setup/import-roster", json={"venue_id": vid, "content": _csv("Casual 0", [today])},
               headers=mgr)
    assert r.status_code == 200, r.text
    snap = c.get(f"/api/snapshot?venue_id={vid}&start_date={today.isoformat()}"
                 f"&end_date={today.isoformat()}", headers=mgr).json()
    brief = c.get(f"/api/briefing?venue_id={vid}", headers=mgr).json()
    labour = snap["days"][0]["labour"]
    assert labour >= 180.0                                # 6h x $30 at least: imports are costed
    assert brief["today"]["labour_cost"] == labour


def test_importing_a_week_supersedes_its_generated_drafts():
    c, mgr, staff_h, vid, (emp,) = _world()
    db = get_db()
    day = venue_today(vid, db) + timedelta(days=7)
    draft = _generate(c, mgr, vid, _monday(day))
    r = c.post("/api/setup/import-roster", json={"venue_id": vid, "content": _csv("Casual 0", [day])},
               headers=mgr)
    assert r.status_code == 200, r.text
    assert db.get_roster_state(draft["id"]) == "superseded"
    cur = c.get(f"/rosters?venue_id={vid}&current=true", headers=mgr).json()["items"]
    assert [x["id"] for x in cur] == [f"imported-{vid}-{_monday(day).isoformat()}"]


# --- generate + setup checklist ---------------------------------------------

def test_generate_snaps_week_start_to_its_monday():
    c, mgr, staff_h, vid, _ = _world()
    sunday = date(2026, 10, 18)
    out = _generate(c, mgr, vid, sunday)
    assert out["week_start"] == "2026-10-12" and out["week_end"] == "2026-10-18"


def test_setup_roster_step_needs_a_generated_roster_not_just_an_import():
    c, mgr, staff_h, vid, _ = _world()
    day = venue_today(vid, get_db()) + timedelta(days=1)
    c.post("/api/setup/import-roster", json={"venue_id": vid, "content": _csv("Casual 0", [day])},
           headers=mgr)
    steps = {s["key"]: s for s in c.get(f"/api/setup?venue_id={vid}", headers=mgr).json()["steps"]}
    assert steps["import"]["done"] is True
    assert steps["roster"]["done"] is False
    _generate(c, mgr, vid, _monday(day))
    steps = {s["key"]: s for s in c.get(f"/api/setup?venue_id={vid}", headers=mgr).json()["steps"]}
    assert steps["roster"]["done"] is True


def test_a_superseded_roster_cannot_be_published_or_approved():
    import asyncio
    c, mgr, staff_h, vid, (emp,) = _world()
    db = get_db()
    tomorrow = venue_today(vid, db) + timedelta(days=1)
    ws = _monday(tomorrow)
    _seed(vid, f"{vid}-a", ws, [_shift(f"{vid}-a1", emp, tomorrow)], created_at=datetime(2026, 1, 1))
    _seed(vid, f"{vid}-b", ws, [_shift(f"{vid}-b1", emp, tomorrow, dtime(9, 0), dtime(12, 0))],
          state="published")
    publisher.transition_state(f"{vid}-b", "published", "test", "test")
    assert db.get_roster_state(f"{vid}-a") == "superseded"

    r = c.post(f"/api/v1/rosters/{vid}-a/publish", json={"skip_approval": True}, headers=mgr)
    assert r.status_code == 200 and r.json()["success"] is False, r.text
    out = asyncio.run(publisher.on_approval_received(f"{vid}-a", approved=True, reviewer_id="x"))
    assert out.success is False
    assert db.get_roster_state(f"{vid}-a") == "superseded"
    assert db.get_roster_state(f"{vid}-b") == "published"


def test_the_seeded_demo_week_is_published_for_the_staff_phone():
    from rosteriq.database import MemoryStore
    from rosteriq.services.demo import DEMO_VENUE_ID, seed_demo_environment
    from rosteriq.services.current_roster import current_roster
    db = MemoryStore()
    seed_demo_environment(db)
    today = venue_today(DEMO_VENUE_ID, db)
    live = current_roster(db, DEMO_VENUE_ID, today, staff=True)
    assert live is not None and db.get_roster_state(live.id) == "published"
    assert any(s.date == today for s in live.shifts)
