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
    Roster, Shift, ShiftStatus,
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

    # A current-week roster with today's shifts, so the AI can answer
    # labour-cost and roster questions (re-seeds each week as 'today' moves).
    try:
        today = date.today()
        week_start = today - timedelta(days=today.weekday())
        week_end = week_start + timedelta(days=6)
        has_roster = db.get_rosters_by_date_range(DEMO_VENUE_ID, week_start, week_end)
    except Exception:
        has_roster = True  # don't risk duplicate seeding if the lookup fails
    if not has_roster:
        # (staff index, role, start, end) — break is 30 min.
        _SHIFTS = [
            (1, "floor", time(11, 0), time(19, 0)),
            (2, "bar", time(15, 0), time(23, 0)),
            (3, "kitchen", time(10, 0), time(18, 0)),
            (4, "floor", time(15, 0), time(23, 0)),
            (5, "bar", time(11, 0), time(19, 0)),
            (6, "kitchen", time(15, 0), time(23, 0)),
        ]
        # Week-specific ids: each week seeds a FRESH roster (a fixed id used to
        # collide with last week's rows, leaving the demo stuck on an old week;
        # past weeks now simply remain as history the AI can reference).
        wk = week_start.isoformat()
        shifts = []
        for i, role, st, en in _SHIFTS:
            paid_hours = (en.hour - st.hour) - 0.5  # minus the 30-min break
            shifts.append(Shift(
                id=f"demo-shift-{wk}-{i:03d}",
                employee_id=f"demo-staff-{i:03d}",
                date=today,
                start_time=st,
                end_time=en,
                break_minutes=30,
                status=ShiftStatus.scheduled,
                role=role,
                cost=Decimal(str(round(paid_hours * 32.5, 2))),
            ))
        try:
            db.save_roster(Roster(
                id=f"demo-roster-{wk}",
                venue_id=DEMO_VENUE_ID,
                week_start=week_start,
                week_end=week_end,
                shifts=shifts,
                total_cost=Decimal(str(round(sum(float(s.cost) for s in shifts), 2))),
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

    # One pending leave request (Leave page approve-it-live beat)
    try:
        if not db.list_leave_requests(DEMO_VENUE_ID):
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

    # One open shift-cover for today's bar shift (Cover board beat)
    try:
        if not db.list_shift_covers(DEMO_VENUE_ID):
            db.save_shift_cover({
                "id": "demo-cover-001", "venue_id": DEMO_VENUE_ID,
                "shift_id": f"demo-shift-{wk}-002",
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

    # Recent dish sales so Menu & Sales chips show real revenue / food cost.
    # Rows only — no stock depletion, so the shelf numbers above stay put.
    try:
        recent = db.list_dish_sales(DEMO_VENUE_ID, today - timedelta(days=7), today)
        recipes = db.list_recipes(DEMO_VENUE_ID) or []
        if recipes and not recent:
            from rosteriq.routes.menu_costing import _cost_recipe
            ings = {i["id"]: i for i in (db.list_ingredients(DEMO_VENUE_ID) or [])}
            qtys = [14, 22, 61]
            for day_offset in (1, 2):
                sale_day = today - timedelta(days=day_offset)
                for n, recipe in enumerate(recipes[:3]):
                    costing = _cost_recipe(recipe, ings)
                    qty = qtys[n % len(qtys)] - day_offset * 2
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
