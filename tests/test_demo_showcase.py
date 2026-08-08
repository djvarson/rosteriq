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
