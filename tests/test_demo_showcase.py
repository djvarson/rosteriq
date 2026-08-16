"""
Demo showcase seeding: every pillar gets sample data (menu, stock, sales,
roster, forecast, leave, cover, announcements), seeding twice never
duplicates, coverage is honestly green, and staleness self-refreshes.
"""

from datetime import date, timedelta

from rosteriq.database import MemoryStore
from rosteriq.services.demo import seed_demo_environment, DEMO_VENUE_ID
from rosteriq.roster_optimiser import compute_coverage_gaps
from rosteriq.routes.snapshot import _labour_by_day


def test_showcase_seeds_every_pillar_idempotently():
    db = MemoryStore()
    seed_demo_environment(db)
    seed_demo_environment(db)  # second run must not duplicate anything
    today = date.today()

    anns = db.list_announcements(DEMO_VENUE_ID)
    assert len(anns) == 2
    pinned = [a for a in anns if a.get("pinned")]
    assert len(pinned) == 1 and len(pinned[0]["read_by"]) == 4

    # Leave: exactly one pending, always a FUTURE date (self-refreshes)
    leave = [l for l in db.list_leave_requests(DEMO_VENUE_ID) if l["status"] == "pending"]
    assert len(leave) == 1
    ls = leave[0]["start_date"]
    ls = ls if isinstance(ls, date) else date.fromisoformat(str(ls)[:10])
    assert ls >= today

    # Cover: open, and points at a shift that genuinely exists TODAY
    covers = [c for c in db.list_shift_covers(DEMO_VENUE_ID) if c["status"] == "open"]
    assert covers
    roster = max([r for r in db.list_rosters() if r.venue_id == DEMO_VENUE_ID],
                 key=lambda r: r.week_start)
    shift_ids = {s.id for s in roster.shifts}
    assert all(c["shift_id"] in shift_ids for c in covers)

    # Menu seeded server-side (no manual /api/menu/seed needed)
    assert len(db.list_ingredients(DEMO_VENUE_ID)) >= 3
    assert len(db.list_recipes(DEMO_VENUE_ID)) == 3

    # Sales seeded across recent days -> the snapshot shows real trade
    sales = db.list_dish_sales(DEMO_VENUE_ID, today - timedelta(days=7), today)
    assert sales and today.isoformat() in {str(s["sale_date"])[:10] for s in sales}

    # Today is staffed and coverage is honestly fully covered at EVERY hour
    assert sum(1 for s in roster.shifts if s.date == today) == 6
    fcs = db.get_forecasts(DEMO_VENUE_ID, roster.week_start, roster.week_end)
    venue = db.get_venue(DEMO_VENUE_ID)
    cov = compute_coverage_gaps(roster, fcs, min_staff_by_role=venue.min_staff)
    assert cov["fully_covered"] is True

    # A plausible prime cost (labour + food over net sales), not absurd
    rev = sum(float(s["revenue_inc_gst"]) for s in sales)
    cogs = sum(float(s["cogs"]) for s in sales)
    net = rev / 1.1
    lab = sum(_labour_by_day(db, DEMO_VENUE_ID, today - timedelta(days=7), today).values())
    prime = (lab + cogs) / net * 100
    assert 40 <= prime <= 80, f"demo prime cost {prime:.0f}% looks off"


def test_showcase_seed_is_stable_across_runs():
    db = MemoryStore()
    seed_demo_environment(db)
    today = date.today()

    def counts():
        return (
            len(db.list_announcements(DEMO_VENUE_ID)),
            len(db.list_dish_sales(DEMO_VENUE_ID, today - timedelta(days=7), today)),
            len(db.get_forecasts(DEMO_VENUE_ID, today, today)),
            len(db.list_ingredients(DEMO_VENUE_ID)),
        )

    before = counts()
    seed_demo_environment(db)
    seed_demo_environment(db)
    assert counts() == before  # fully idempotent on the same day


def test_showcase_feed_posts_are_reasserted_every_seed():
    """The two showcase feed posts are upserted by fixed id on EVERY seed, so a
    demo post a prospect removed (or edited) is restored next Try Demo, while
    posts prospects created themselves are left alone. (Guarding on "feed is
    empty" meant one removed post blanked the showcase for good.)"""
    from datetime import datetime
    db = MemoryStore()
    seed_demo_environment(db)
    ids = {p["id"] for p in db.list_feed_posts(DEMO_VENUE_ID)}
    assert {"demo-feed-001", "demo-feed-002"} <= ids

    # A prospect soft-removes the swap ask and edits the pinned post.
    post = db.get_feed_post("demo-feed-001")
    post["removed"] = True
    db.save_feed_post(post)
    pinned = db.get_feed_post("demo-feed-002")
    pinned["body"] = "edited by a prospect"
    pinned["pinned"] = False
    db.save_feed_post(pinned)
    # ...and adds a post of their own.
    db.save_feed_post({
        "id": "prospect-post-1", "venue_id": DEMO_VENUE_ID,
        "author_user_id": "someone", "author_name": "Someone", "author_role": "staff",
        "body": "hello", "pinned": False, "removed": False, "reactions": {},
        "comments": [], "created_at": datetime.utcnow(), "updated_at": datetime.utcnow(),
    })
    assert "demo-feed-001" not in {p["id"] for p in db.list_feed_posts(DEMO_VENUE_ID)}

    seed_demo_environment(db)
    posts = {p["id"]: p for p in db.list_feed_posts(DEMO_VENUE_ID)}
    assert "demo-feed-001" in posts, "removed showcase post was not restored"
    assert posts["demo-feed-001"]["removed"] is False
    assert posts["demo-feed-002"]["pinned"] is True
    assert posts["demo-feed-002"]["body"].startswith("New pass-through window")
    assert "prospect-post-1" in posts, "prospect-created post must be untouched"
    assert len(posts) == 3  # no duplicates on re-assert


def test_showcase_food_safety_sop_and_acks_are_reasserted():
    """Hard-deleting the Food safety starter (e.g. via the SOP delete route)
    used to leave the showcase without it forever because the SOP block only
    seeded when the library was empty. Now every seed re-runs the idempotent
    starter seed and re-asserts the three demo acks."""
    from rosteriq.routes.sops import _starter_doc_id, STARTER_SOPS
    food_title = next(s["title"] for s in STARTER_SOPS
                      if s["title"].lower().startswith("food safety"))
    food_id = _starter_doc_id(DEMO_VENUE_ID, food_title)

    db = MemoryStore()
    seed_demo_environment(db)
    assert db.get_sop_document(food_id)
    assert len(db.list_sop_documents(DEMO_VENUE_ID)) == 4
    assert len(db.list_sop_acks(DEMO_VENUE_ID, food_id)) == 3

    db.delete_sop_document(food_id)
    # Simulate the acks going with it (PG cascades / a fresh store).
    for a in list(db.list_sop_acks(DEMO_VENUE_ID, food_id)):
        db._sop_acks.pop(a["id"], None)
    assert db.get_sop_document(food_id) is None
    assert db.list_sop_acks(DEMO_VENUE_ID, food_id) == []

    seed_demo_environment(db)
    food = db.get_sop_document(food_id)
    assert food and food["title"] == food_title
    assert len(db.list_sop_documents(DEMO_VENUE_ID)) == 4  # no duplicates
    acks = db.list_sop_acks(DEMO_VENUE_ID, food_id)
    assert len(acks) == 3
    assert {a["employee_id"] for a in acks} == {"demo-staff-001", "demo-staff-002", "demo-staff-003"}
    assert {a["id"] for a in acks} == {"demo-sop-ack-001", "demo-sop-ack-002", "demo-sop-ack-003"}

    # And a third seed with everything present is a pure no-op.
    seed_demo_environment(db)
    assert len(db.list_sop_documents(DEMO_VENUE_ID)) == 4
    assert len(db.list_sop_acks(DEMO_VENUE_ID, food_id)) == 3
