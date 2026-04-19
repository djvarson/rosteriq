"""Seed realistic demo data for a pilot venue.

This module populates demo Tanda history, shift events, concierge KB, and
accountability data so the dashboard looks real from day one when demoing
to venue owners.

Pure stdlib module — no FastAPI/Pydantic/httpx hard deps. Lazy imports
inside functions so the module loads without external dependencies.

Why this matters: a blank dashboard is demoware. Pre-seeded with 8 weeks
of realistic Australian pub patterns (Fridays/Saturdays strong, weekdays
~$4-6k revenue, ~28-35% labour cost), staff can immediately see forecasting
accuracy, labour variance, and shift patterns instead of waiting for live
data to accumulate.
"""

from __future__ import annotations

import logging
import random
import uuid
from datetime import date, datetime, timedelta, timezone

logger = logging.getLogger("rosteriq.demo_seed")

AU_TZ = timezone(timedelta(hours=10))


def seed_all(venue_id: str = "demo-venue-001", weeks: int = 8) -> dict:
    """Seed all demo data for a venue.

    Returns a summary dict with keys:
    - seeded: list of what was seeded
    - errors: list of any errors encountered

    Each seeder is wrapped in try/except — best-effort, never raises.
    """
    summary = {"seeded": [], "errors": []}

    # Seed Tanda history
    try:
        _seed_tanda_history(venue_id, weeks)
        summary["seeded"].append(f"tanda_history (8 weeks for {venue_id})")
    except Exception as e:
        summary["errors"].append(f"tanda_history: {e}")
        logger.exception("demo_seed: tanda_history failed")

    # Seed shift events
    try:
        _seed_shift_events(venue_id, weeks)
        summary["seeded"].append(f"shift_events (8 weeks for {venue_id})")
    except Exception as e:
        summary["errors"].append(f"shift_events: {e}")
        logger.exception("demo_seed: shift_events failed")

    # Seed concierge KB
    try:
        _seed_concierge_kb(venue_id)
        summary["seeded"].append(f"concierge_kb ({venue_id})")
    except Exception as e:
        summary["errors"].append(f"concierge_kb: {e}")
        logger.exception("demo_seed: concierge_kb failed")

    # Seed accountability data
    try:
        _seed_accountability(venue_id)
        summary["seeded"].append(f"accountability ({venue_id})")
    except Exception as e:
        summary["errors"].append(f"accountability: {e}")
        logger.exception("demo_seed: accountability failed")

    logger.info("Demo seed complete for %s: %s", venue_id, summary)
    return summary


def seed_if_empty(venue_id: str = "demo-venue-001") -> bool:
    """Check if the store already has data for the venue; seed only if empty.

    Returns True if seeding happened, False if store already had data.
    """
    from rosteriq import tanda_history

    store = tanda_history.get_history_store()
    # Check if venue already has history
    existing = store.venues()
    if venue_id in existing:
        logger.info("Demo seed skipped for %s (already has data)", venue_id)
        return False

    logger.info("Demo seed starting for %s (no existing data)", venue_id)
    result = seed_all(venue_id, weeks=8)
    if result["errors"]:
        logger.warning("Demo seed had errors for %s: %s", venue_id, result["errors"])
    return True


# =============================================================================
# Tanda History Seeding
# =============================================================================


def _seed_tanda_history(venue_id: str, weeks: int) -> None:
    """Seed 8 weeks of daily + hourly Tanda history for a venue."""
    from rosteriq import tanda_history

    store = tanda_history.get_history_store()
    rng = random.Random(42)  # seeded for reproducibility

    # Calculate date range: 8 weeks back from today
    today = date.today()
    end_date = today - timedelta(days=1)  # yesterday
    start_date = end_date - timedelta(weeks=weeks - 1)

    cur_date = start_date
    while cur_date <= end_date:
        day_of_week = cur_date.weekday()  # 0=Monday, 6=Sunday

        # Realistic Australian pub patterns
        if day_of_week < 4:  # Mon-Thu
            revenue = rng.uniform(4000, 6000)
            rostered_hours = rng.uniform(80, 120)
            employee_count = rng.randint(8, 12)
        elif day_of_week == 4:  # Friday
            revenue = rng.uniform(8000, 12000)
            rostered_hours = rng.uniform(150, 200)
            employee_count = rng.randint(13, 16)
        elif day_of_week == 5:  # Saturday
            revenue = rng.uniform(10000, 15000)
            rostered_hours = rng.uniform(180, 220)
            employee_count = rng.randint(14, 18)
        else:  # Sunday
            revenue = rng.uniform(6000, 9000)
            rostered_hours = rng.uniform(100, 150)
            employee_count = rng.randint(10, 13)

        # Worked hours slightly different from rostered (±5%)
        variance_pct = rng.uniform(-0.05, 0.05)
        worked_hours = rostered_hours * (1 + variance_pct)

        # Labour cost 28-35% of revenue
        labour_pct = rng.uniform(0.28, 0.35)
        worked_cost = revenue * labour_pct
        rostered_cost = worked_cost * (1 + rng.uniform(-0.02, 0.02))  # similar to worked

        # Create daily aggregate
        daily = tanda_history.DailyActuals(
            venue_id=venue_id,
            day=cur_date,
            rostered_hours=round(rostered_hours, 2),
            worked_hours=round(worked_hours, 2),
            rostered_cost=round(rostered_cost, 2),
            worked_cost=round(worked_cost, 2),
            forecast_revenue=round(revenue, 2),
            actual_revenue=round(revenue, 2),
            shift_count=employee_count,
            employee_count=employee_count,
        )
        store.upsert_daily(daily)

        # Hourly patterns: peak at lunch (12-14) and dinner (18-21)
        # 5-8 heads at peak, taper off outside peak
        for hour in range(11, 23):  # 11am-10pm
            if 12 <= hour < 14:  # lunch peak
                heads = rng.randint(5, 8)
                hour_revenue = revenue * rng.uniform(0.12, 0.18)
            elif 18 <= hour < 21:  # dinner peak
                heads = rng.randint(6, 8)
                hour_revenue = revenue * rng.uniform(0.15, 0.22)
            elif 14 <= hour < 17:  # afternoon dip
                heads = rng.randint(2, 4)
                hour_revenue = revenue * rng.uniform(0.05, 0.12)
            elif 17 <= hour < 18:  # ramp up to dinner
                heads = rng.randint(3, 5)
                hour_revenue = revenue * rng.uniform(0.08, 0.15)
            else:  # evening tail-off
                heads = rng.randint(1, 3)
                hour_revenue = revenue * rng.uniform(0.02, 0.08)

            hourly = tanda_history.HourlyActuals(
                venue_id=venue_id,
                day=cur_date,
                hour=hour,
                rostered_heads=heads,
                worked_heads=max(1, heads + rng.randint(-1, 1)),
                forecast_revenue=round(hour_revenue, 2),
            )
            store.upsert_hourly(hourly)

        cur_date += timedelta(days=1)

    store.mark_ingested(venue_id)
    logger.info("Seeded tanda_history for %s: %d days", venue_id, weeks * 7)


# =============================================================================
# Shift Events Seeding
# =============================================================================


def _seed_shift_events(venue_id: str, weeks: int) -> None:
    """Seed shift events (clock-in, break, clock-out patterns) for each day."""
    from rosteriq import shift_events

    store = shift_events.get_shift_event_store()
    rng = random.Random(42)

    today = date.today()
    end_date = today - timedelta(days=1)
    start_date = end_date - timedelta(weeks=weeks - 1)

    # Common event patterns
    event_scenarios = [
        (shift_events.EventCategory.WALK_IN_SURGE, "Unexpected walk-in surge during lunch", [12, 13]),
        (shift_events.EventCategory.PUB_GROUP, "Pub group arrived without reservation", [19, 20, 21]),
        (shift_events.EventCategory.STAFF_SHORTAGE, "Staff member called in sick", [11, 12]),
    ]

    cur_date = start_date
    while cur_date <= end_date:
        day_of_week = cur_date.weekday()

        # More events on Fri/Sat, fewer on Mon-Wed
        if day_of_week < 2:  # Mon-Tue
            num_events = rng.randint(1, 2)
        elif day_of_week < 4:  # Wed-Thu
            num_events = rng.randint(2, 3)
        elif day_of_week == 4:  # Friday
            num_events = rng.randint(3, 5)
        elif day_of_week == 5:  # Saturday
            num_events = rng.randint(4, 6)
        else:  # Sunday
            num_events = rng.randint(2, 3)

        for _ in range(num_events):
            category, description, hours = rng.choice(event_scenarios)
            hour = rng.choice(hours)

            event = shift_events.ShiftEvent(
                event_id=str(uuid.uuid4()),
                venue_id=venue_id,
                category=category,
                description=description,
                timestamp=datetime(cur_date.year, cur_date.month, cur_date.day, hour, rng.randint(0, 59), tzinfo=AU_TZ),
                headcount_at_time=rng.randint(5, 15),
                logged_by=rng.choice(["staff_alice", "staff_bob", "staff_charlie"]),
                shift_date=cur_date,
                day_of_week=day_of_week,
                hour_of_day=hour,
                weather_condition=rng.choice(["sunny", "rainy", "cloudy", None]),
                active_event_ids=[],
                tags=["demo"],
            )
            store.record(event)

        cur_date += timedelta(days=1)

    logger.info("Seeded shift_events for %s: %d days", venue_id, weeks * 7)


# =============================================================================
# Concierge KB Seeding
# =============================================================================


def _seed_concierge_kb(venue_id: str) -> None:
    """Seed concierge KB with realistic venue FAQs."""
    from rosteriq import concierge

    kb_store = concierge.get_kb()

    # Create venue KB
    venue_kb = concierge.VenueKB(
        venue_id=venue_id,
        venue_name="The Brisbane Hotel",
        faqs=[
            concierge.FAQEntry(
                question="What time are you open today?",
                answer="We're open from 11:00 AM to 11:00 PM today.",
                keywords=["open", "opening", "hours", "close", "closing", "what time"],
                tags=["hours"],
            ),
            concierge.FAQEntry(
                question="Do you take walk-ins?",
                answer="Yes — we hold a portion of tables for walk-ins. Bookings still get priority.",
                keywords=["walk in", "walk-in", "without booking", "no booking"],
                tags=["bookings"],
            ),
            concierge.FAQEntry(
                question="Do you have a kids menu?",
                answer="Yes, we have a kids menu for under-12s with smaller mains and a free dessert.",
                keywords=["kids", "children", "child menu", "kid friendly"],
                tags=["menu"],
            ),
            concierge.FAQEntry(
                question="Are you dog friendly?",
                answer="Outdoor areas are dog-friendly; we ask that pets stay on leash.",
                keywords=["dog", "pet", "puppy"],
                tags=["amenities"],
            ),
            concierge.FAQEntry(
                question="Where can I park?",
                answer="On-site parking is free until 11pm; overflow is on the street out front.",
                keywords=["park", "parking", "carpark", "car park"],
                tags=["amenities"],
            ),
            concierge.FAQEntry(
                question="Do you have gluten-free options?",
                answer="Several mains and pizzas are gluten-free; ask the kitchen if anything's unclear.",
                keywords=["gluten", "gf", "celiac", "coeliac"],
                tags=["dietary"],
            ),
            concierge.FAQEntry(
                question="Can we book a function?",
                answer="Yes — function bookings of 15+ go through our events team. Please ask staff to take your details and they'll be in touch.",
                keywords=["function", "private room", "events", "group booking", "party"],
                tags=["functions", "escalate"],
            ),
        ],
        live_context={
            "open_time": "11:00 AM",
            "close_time": "11:00 PM",
            "today_specials": "Parmi Wednesday — $18 schnitzel & fries",
        },
    )
    kb_store.register(venue_kb)
    logger.info("Seeded concierge_kb for %s: %s", venue_id, venue_kb.venue_name)


# =============================================================================
# Accountability Seeding
# =============================================================================


def _seed_accountability(venue_id: str) -> None:
    """Seed accountability data (manager decisions + variance outcomes)."""
    from rosteriq import accountability_engine

    # Check if already seeded
    store = accountability_engine._DECISIONS_STORE
    if store:
        logger.info("Accountability store already populated, skipping seed")
        return

    # Delegate to the existing seed function in accountability_engine
    try:
        accountability_engine._seed_demo_data()
        logger.info("Seeded accountability via _seed_demo_data()")
    except Exception as e:
        logger.warning("accountability _seed_demo_data() failed: %s", e)
