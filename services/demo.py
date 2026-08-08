"""
Demo environment: a sandboxed, public demo so prospective venues can try the
product — including the AI agent — without creating an account.

"Try Demo" mints a short-lived token for a scoped demo user (DEMO_USER_ID) that
can only touch the demo venue (DEMO_VENUE_ID). Because it's a real (if limited)
session, every authenticated feature works through the normal auth path — no
per-endpoint demo bypasses, no security holes. The demo venue is seeded with
sample staff so the AI has real data to reason about.
"""

from datetime import datetime
from datetime import date, time, timedelta
from decimal import Decimal

from rosteriq.models import (
    VenueConfig, Employee, EmploymentType, AwardLevel, State,
    Roster, Shift, ShiftStatus, DemandForecast,
)

DEMO_VENUE_ID = "demo-venue-001"
DEMO_USER_ID = "demo-user"
DEMO_USER_EMAIL = "demo@rosteriq.app"

# (name, role/skill, hourly rate) — mirrors the dashboard's client-side demo set.
_DEMO_STAFF = [
    ("Emma Thompson", "floor", "32.50"),
    ("James Wilson", "bar", "33.00"),
    ("Sarah Chen", "kitchen", "34.00"),
    ("Marcus Johnson", "floor", "31.50"),
    ("Lisa Brown", "bar", "32.00"),
    ("David Miller", "kitchen", "35.00"),
]


def seed_demo_environment(db) -> None:
    """Idempotently seed the demo user, venue, and staff.

    Each entity is ensured independently (rather than skip-all-if-the-user-
    exists) so a previously partial seed self-heals on the next call. Never
    raises on a single entity — a half-seeded demo beats a 500 on the public
    'Try Demo' path.
    """
    now = datetime.utcnow()

    # Scoped, non-owner demo user limited to the demo venue.
    try:
        existing = db.get_user_by_id(DEMO_USER_ID)
        if not existing:
            db.save_user({
                "id": DEMO_USER_ID,
                "email": DEMO_USER_EMAIL,
                "name": "Demo User",
                "password_hash": "",      # login-by-password disabled for demo
                "role": "staff",
                "is_active": True,
                "venue_ids": [DEMO_VENUE_ID],
                "created_at": now,
            })
        elif (
            existing.get("venue_ids") != [DEMO_VENUE_ID]
            or existing.get("role") != "staff"
            or not existing.get("is_active")
        ):
            # Self-heal a demo user created before venue_ids persisted (it
            # otherwise 403s on its own venue) or otherwise drifted.
            existing["venue_ids"] = [DEMO_VENUE_ID]
            existing["role"] = "staff"
            existing["is_active"] = True
            db.save_user(existing)
    except Exception:
        pass

    if not db.get_venue(DEMO_VENUE_ID):
        db.save_venue(VenueConfig(
            id=DEMO_VENUE_ID,
            name="The Brass Monkey",
            tanda_org_id="demo-org-001",
            state=State.wa,
            timezone="Australia/Perth",
            min_staff={"floor": 2, "bar": 1, "kitchen": 2},
            max_labour_pct=30.0,
            pos_system="demo",
            created_at=now,
        ))

    # Seed staff only if the demo venue has none yet (self-heals a prior
    # partial seed without duplicating).
    try:
        already = db.get_employees(DEMO_VENUE_ID)
    except Exception:
        already = []
    if not already:
        employees = [
            Employee(
                id=f"demo-staff-{i:03d}",
                venue_id=DEMO_VENUE_ID,
                name=name,
                employment_type=EmploymentType.casual,
                award_level=AwardLevel.level_2,
                state=State.wa,
                hourly_base_rate=Decimal(rate),
                skills=[role],
                created_at=now,
                updated_at=now,
            )
            for i, (name, role, rate) in enumerate(_DEMO_STAFF, start=1)
        ]
        db.save_employees(employees)

    # Starter menu (ingredients + recipes) so Menu & Sales, inventory and the
    # snapshot are never empty — previously only seeded if someone manually
    # POSTed /api/menu/seed, so a fresh deploy demoed blank.
    try:
        from rosteriq.routes.menu_costing import seed_starter_menu
        seed_starter_menu(db, DEMO_VENUE_ID)
    except Exception:
        pass

    # Day-rolling roster: ensure THIS week's roster has shifts dated TODAY, so
    # the on-shift board, coverage, and briefing always show a staffed today —
    # not just on the day the week was first seeded (the drift that made
    # coverage show phantom shortfalls later in the week).
    try:
        today = date.today()
        week_start = today - timedelta(days=today.weekday())
        week_end = week_start + timedelta(days=6)
        wk = week_start.isoformat()
        # (staff index, role, start, end) — break is 30 min.
        _SHIFTS = [
            (1, "floor", time(11, 0), time(19, 0)),
            (2, "bar", time(15, 0), time(23, 0)),
            (3, "kitchen", time(10, 0), time(18, 0)),
            (4, "floor", time(15, 0), time(23, 0)),
            (5, "bar", time(11, 0), time(19, 0)),
            (6, "kitchen", time(15, 0), time(23, 0)),
        ]
        existing = db.get_roster(f"demo-roster-{wk}")
        have_today = bool(existing) and any(s.date == today for s in existing.shifts)
        if not have_today:
            new_shifts = []
            for i, role, st, en in _SHIFTS:
                paid_hours = (en.hour - st.hour) - 0.5
                new_shifts.append(Shift(
                    id=f"demo-shift-{today.isoformat()}-{i:03d}",
                    employee_id=f"demo-staff-{i:03d}",
                    date=today, start_time=st, end_time=en,
                    break_minutes=30, status=ShiftStatus.scheduled, role=role,
                    cost=Decimal(str(round(paid_hours * 32.5, 2))),
                ))
            all_shifts = (list(existing.shifts) if existing else []) + new_shifts
            db.save_roster(Roster(
                id=f"demo-roster-{wk}",
                venue_id=DEMO_VENUE_ID,
                week_start=week_start,
                week_end=week_end,
                shifts=all_shifts,
                total_cost=Decimal(str(round(sum(float(s.cost or 0) for s in all_shifts), 2))),
                created_at=datetime.utcnow(),
            ))
    except Exception:
        pass

    _seed_demo_showcase(db, now)


def _seed_demo_showcase(db, now) -> None:
    """Dress the newer Venue OS pillars so no demo page opens on an empty
    state: announcements with read receipts, a pending leave request, an open
    shift-cover, stock/par levels, and recent dish sales. Every block is
    guarded (seed only when that pillar is empty) and best-effort — a partial
    showcase beats a broken Try Demo."""
    today = date.today()
    week_start = today - timedelta(days=today.weekday())
    wk = week_start.isoformat()

    # Announcements + read receipts (Announcements page / News tab beat)
    try:
        if not db.list_announcements(DEMO_VENUE_ID):
            db.save_announcement({
                "id": "demo-ann-001", "venue_id": DEMO_VENUE_ID,
                "title": "Welcome to The Brass Monkey on RosterIQ",
                "body": "Rosters, hours, leave and shift swaps all live in /my "
                        "on your phone. Ask a manager if your email isn't linked yet.",
                "author_id": DEMO_USER_ID, "author_name": "Management",
                "pinned": True, "sms_result": None,
                "read_by": ["demo-staff-001", "demo-staff-002",
                            "demo-staff-003", "demo-staff-005"],
                "created_at": now,
            })
            db.save_announcement({
                "id": "demo-ann-002", "venue_id": DEMO_VENUE_ID,
                "title": "New winter menu starts Monday",
                "body": "Tasting for all kitchen staff Sunday 3pm — paid hour.",
                "author_id": DEMO_USER_ID, "author_name": "Management",
                "pinned": False, "sms_result": None,
                "read_by": ["demo-staff-003", "demo-staff-006"],
                "created_at": now,
            })
    except Exception:
        pass

    # One pending leave request (Leave page approve-it-live beat). Refresh the
    # SAME record when its dates fall into the past, so the demo never shows a
    # "pending" leave for a date that's already gone.
    try:
        lv = db.get_leave_request("demo-leave-001")
        lv_start = None
        if lv:
            s = lv.get("start_date")
            lv_start = s if isinstance(s, date) else date.fromisoformat(str(s)[:10])
        if not lv or lv_start < today:
            db.save_leave_request({
                "id": "demo-leave-001", "venue_id": DEMO_VENUE_ID,
                "employee_id": "demo-staff-004",
                "start_date": today + timedelta(days=9),
                "end_date": today + timedelta(days=11),
                "reason": "Sister's wedding in Margaret River",
                "status": "pending", "created_at": now,
            })
    except Exception:
        pass

    # One open shift-cover for TODAY's bar shift (Cover board beat). Keyed to
    # today's shift so it never orphans onto a shift that no longer exists.
    try:
        today_bar_shift = f"demo-shift-{today.isoformat()}-002"
        covers = db.list_shift_covers(DEMO_VENUE_ID) or []
        if not any(c.get("shift_id") == today_bar_shift and c.get("status") == "open"
                   for c in covers):
            db.save_shift_cover({
                "id": f"demo-cover-{today.isoformat()}", "venue_id": DEMO_VENUE_ID,
                "shift_id": today_bar_shift,
                "shift_date": today, "shift_start": "15:00", "shift_end": "23:00",
                "role": "bar", "requested_by": "demo-staff-002",
                "reason": "Uni exam tomorrow morning",
                "claimed_by": None, "status": "open", "created_at": now,
            })
    except Exception:
        pass

    # Stock + par levels on existing ingredients, ONE deliberately below par
    # (Inventory low-badge + order-draft beat). Never clobbers real numbers.
    try:
        ingredients = db.list_ingredients(DEMO_VENUE_ID) or []
        untouched = [i for i in ingredients
                     if not float(i.get("stock_qty") or 0)
                     and not float(i.get("par_level") or 0)]
        if ingredients and len(untouched) == len(ingredients):
            for idx, ing in enumerate(sorted(ingredients, key=lambda i: i["name"])):
                pack = float(ing.get("purchase_size") or 1) or 1
                ing["par_level"] = round(pack * 2, 3)
                # First item runs low so the LOW badge and order draft demo work
                ing["stock_qty"] = round(pack * (0.6 if idx == 0 else 2.5), 3)
                db.save_ingredient(ing)
    except Exception:
        pass

    # Recent dish sales so Menu & Sales + the Business snapshot always show
    # live trade. Seeded PER DAY for the last three days (incl. today, where
    # the demo roster's labour sits) and guarded per-day, so as the calendar
    # advances yesterday self-refreshes and the numbers never go stale — a
    # fixed-date seed drifts out of the trailing window and leaves labour
    # standing against $0 sales, which is the opposite of the demo you want.
    # Rows only — no stock depletion, so the shelf numbers above stay put.
    try:
        recipes = db.list_recipes(DEMO_VENUE_ID) or []
        if recipes:
            from rosteriq.routes.menu_costing import _cost_recipe
            ings = {i["id"]: i for i in (db.list_ingredients(DEMO_VENUE_ID) or [])}
            qtys = [61, 22, 14]  # flat whites move more than parmys
            for day_offset in (0, 1, 2):
                sale_day = today - timedelta(days=day_offset)
                existing = db.list_dish_sales(DEMO_VENUE_ID, sale_day, sale_day) or []
                if existing:
                    continue  # this day already has trade — leave it
                for n, recipe in enumerate(recipes[:3]):
                    costing = _cost_recipe(recipe, ings)
                    qty = max(4, qtys[n % len(qtys)] - day_offset * 3)
                    db.save_dish_sale({
                        "id": f"demo-sale-{sale_day.isoformat()}-{n}",
                        "venue_id": DEMO_VENUE_ID,
                        "sale_date": sale_day,
                        "recipe_id": recipe["id"],
                        "recipe_name": recipe.get("name"),
                        "qty": qty,
                        "revenue_inc_gst": round(
                            float(costing["sell_price_inc_gst"]) * qty, 2),
                        "cogs": round(float(costing["cost_per_portion"]) * qty, 2),
                        "source": "manual",
                        "created_at": now,
                    })
    except Exception:
        pass

    # Today's demand forecast so the coverage check (and the AI's
    # get_roster_coverage tool) have something to assess against — the demo
    # roster puts 6 staff on today, tuned here to comfortably cover the peak
    # so coverage demos as a reassuring "fully covered". Seeded only if today
    # has no forecast, so it self-refreshes as the calendar moves.
    try:
        existing_fc = db.get_forecasts(DEMO_VENUE_ID, today, today) or []
        if not existing_fc:
            # A sports-pub game-day surge, seeded ONLY for the mid-afternoon
            # window the demo roster actually staffs 5-6 deep (min_staff floor
            # is 5, and the roster runs 3 outside 15:00-18:00) — so coverage
            # is honestly "fully covered" at EVERY forecast hour, not just the
            # single busiest one. Peak 15:00 (85 covers) needs 6, exactly met.
            curve = {15: 85, 16: 78, 17: 70, 18: 55}
            db.add_forecasts([
                DemandForecast(
                    id=f"demo-fc-{today.isoformat()}-{hr}",
                    venue_id=DEMO_VENUE_ID, date=today, hour=hr,
                    predicted_covers=float(cov), confidence=0.85,
                    model_version="demo-seed",
                )
                for hr, cov in curve.items()
            ])
    except Exception:
        pass
