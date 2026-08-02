"""
Demo showcase seeding: every new pillar gets sample data, seeding twice never
duplicates, and existing (real) numbers are never clobbered.
"""

from datetime import date, timedelta

from rosteriq.database import MemoryStore
from rosteriq.services.demo import seed_demo_environment, DEMO_VENUE_ID


def test_showcase_seeds_every_pillar_idempotently():
    db = MemoryStore()
    seed_demo_environment(db)
    seed_demo_environment(db)  # second run must not duplicate anything

    anns = db.list_announcements(DEMO_VENUE_ID)
    assert len(anns) == 2
    pinned = [a for a in anns if a.get("pinned")]
    assert len(pinned) == 1 and len(pinned[0]["read_by"]) == 4

    leave = db.list_leave_requests(DEMO_VENUE_ID)
    assert len(leave) == 1 and leave[0]["status"] == "pending"
    assert leave[0]["start_date"] > date.today()  # always future, week-rolling

    covers = db.list_shift_covers(DEMO_VENUE_ID)
    assert len(covers) == 1 and covers[0]["status"] == "open"
    # The cover points at a shift that genuinely exists in this week's roster
    week_start = date.today() - timedelta(days=date.today().weekday())
    assert covers[0]["shift_id"] == f"demo-shift-{week_start.isoformat()}-002"

    # No ingredients in a fresh store -> stock/sales blocks skip WITHOUT error
    assert db.list_ingredients(DEMO_VENUE_ID) == []
    assert db.list_dish_sales(DEMO_VENUE_ID,
                              date.today() - timedelta(days=7), date.today()) == []


def test_showcase_dresses_stock_and_sales_when_menu_exists():
    db = MemoryStore()
    # A costed mini-menu exists BEFORE the seed (mirrors production, where the
    # demo menu was seeded earlier)
    db.save_ingredient({
        "id": "demo-ing-1", "venue_id": DEMO_VENUE_ID, "name": "Chicken",
        "unit": "kg", "purchase_size": 5, "purchase_cost": 50,
        "cost_per_unit": 10, "active": True, "stock_qty": 0, "par_level": 0,
    })
    db.save_ingredient({
        "id": "demo-ing-2", "venue_id": DEMO_VENUE_ID, "name": "Cheese",
        "unit": "kg", "purchase_size": 2, "purchase_cost": 30,
        "cost_per_unit": 15, "active": True, "stock_qty": 0, "par_level": 0,
    })
    db.save_recipe({
        "id": "demo-rcp-1", "venue_id": DEMO_VENUE_ID, "name": "Chicken Melt",
        "sell_price_inc_gst": 22.0, "yield_portions": 1, "active": True,
        "items": [{"ingredient_id": "demo-ing-1", "qty": 250, "unit": "g"}],
    })

    seed_demo_environment(db)
    seed_demo_environment(db)

    # Stock/pars set once: exactly one item below par (the LOW-badge beat)
    ings = {i["name"]: i for i in db.list_ingredients(DEMO_VENUE_ID)}
    low = [n for n, i in ings.items()
           if float(i["stock_qty"]) < float(i["par_level"])]
    assert low == ["Cheese"]  # first alphabetically runs low
    assert float(ings["Chicken"]["stock_qty"]) == 12.5  # 5 * 2.5, healthy

    # Sales rows for the last two days, revenue/cogs from real costing
    sales = db.list_dish_sales(DEMO_VENUE_ID,
                               date.today() - timedelta(days=7), date.today())
    assert len(sales) == 2  # one recipe x two days, seeded once
    s = sales[0]
    assert s["revenue_inc_gst"] == round(22.0 * s["qty"], 2)
    assert s["cogs"] == round(2.5 * s["qty"], 2)  # 250g @ $10/kg

    # A third run with data present changes nothing
    before = len(db.list_dish_sales(DEMO_VENUE_ID,
                                    date.today() - timedelta(days=7), date.today()))
    seed_demo_environment(db)
    after = len(db.list_dish_sales(DEMO_VENUE_ID,
                                   date.today() - timedelta(days=7), date.today()))
    assert before == after
