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

# Second demo identity for the staff-phone (/my) showcase: a login whose email
# matches a seeded employee (Emma Thompson, demo-staff-001) so the portal
# links and every "my shifts / my leave / swaps" beat has real data behind it.
DEMO_STAFF_USER_ID = "demo-staff-user"
DEMO_STAFF_EMAIL = "demo.staff@rosteriq.app"
DEMO_STAFF_EMPLOYEE_ID = "demo-staff-001"

# (name, role/skill, hourly rate) — mirrors the dashboard's client-side demo set.
_DEMO_STAFF = [
    ("Emma Thompson", "floor", "32.50"),
    ("James Wilson", "bar", "33.00"),
    ("Sarah Chen", "kitchen", "34.00"),
    ("Marcus Johnson", "floor", "31.50"),
    ("Lisa Brown", "bar", "32.00"),
    ("David Miller", "kitchen", "35.00"),
]

# Availability per demo staffer (day -> [{start,end}] in the resolver's
# lowercase-day convention). Deliberately varied so "who can cover Saturday
# night?" has a real answer in the demo — with no availability the AI's
# find_available_staff comes back empty on the exact beat the runbook demos.
_ALL_WEEK = {d: [{"start": "09:00", "end": "23:00"}]
             for d in ("monday", "tuesday", "wednesday", "thursday",
                       "friday", "saturday", "sunday")}
_WEEKENDS_AND_NIGHTS = {
    "thursday": [{"start": "16:00", "end": "23:59"}],
    "friday": [{"start": "16:00", "end": "23:59"}],
    "saturday": [{"start": "11:00", "end": "23:59"}],
    "sunday": [{"start": "11:00", "end": "22:00"}],
}
_WEEKDAYS_ONLY = {d: [{"start": "08:00", "end": "18:00"}]
                  for d in ("monday", "tuesday", "wednesday", "thursday", "friday")}
_DEMO_AVAILABILITY = {
    "demo-staff-001": _ALL_WEEK,               # Emma — the showcase staffer
    "demo-staff-002": _WEEKENDS_AND_NIGHTS,    # James — the Saturday-night answer
    "demo-staff-003": _WEEKDAYS_ONLY,          # Sarah — weekday kitchen
    "demo-staff-004": _ALL_WEEK,
    "demo-staff-005": _WEEKENDS_AND_NIGHTS,    # Lisa — second bar cover
    "demo-staff-006": _WEEKDAYS_ONLY,
}
_DEMO_PHONES = {f"demo-staff-{i:03d}": f"04{i:02d} 555 0{i:02d}{i}" for i in range(1, 7)}


def seed_demo_environment(db) -> None:
    """Idempotently seed the demo user, venue, and staff.

    Each entity is ensured independently (rather than skip-all-if-the-user-
    exists) so a previously partial seed self-heals on the next call. Never
    raises on a single entity — a half-seeded demo beats a 500 on the public
    'Try Demo' path.
    """
    now = datetime.utcnow()

    # Scoped, non-owner demo user limited to the demo venue. Role is
    # "manager" (NOT owner): managers are venue_ids-scoped exactly like staff,
    # so the sandbox holds — but the demo dashboard IS the manager view
    # (roster generation, forecasts, labour, publishing, feed moderation) and
    # those are role-gated as of the 2026-08-30 authz pass, so the demo user
    # must be a manager to exercise them. The staff-side demo has its own
    # identity (DEMO_STAFF_USER / Emma, role "staff") for the /my portal.
    try:
        existing = db.get_user_by_id(DEMO_USER_ID)
        if not existing:
            db.save_user({
                "id": DEMO_USER_ID,
                "email": DEMO_USER_EMAIL,
                "name": "Demo User",
                "password_hash": "",      # login-by-password disabled for demo
                "role": "manager",
                "is_active": True,
                "venue_ids": [DEMO_VENUE_ID],
                "created_at": now,
            })
        elif (
            existing.get("venue_ids") != [DEMO_VENUE_ID]
            or existing.get("role") != "manager"
            or not existing.get("is_active")
        ):
            # Self-heal a demo user created before venue_ids persisted (it
            # otherwise 403s on its own venue) or one seeded as staff before the
            # dashboard's manager actions were role-gated.
            existing["venue_ids"] = [DEMO_VENUE_ID]
            existing["role"] = "manager"
            existing["is_active"] = True
            db.save_user(existing)
    except Exception:
        pass

    # Staff-side demo identity (Emma Thompson). Same self-heal pattern: a row
    # that drifted (wrong venue scope, role, deactivated, or an email that no
    # longer matches the seeded employee) is put back so the /my portal links.
    try:
        existing = db.get_user_by_id(DEMO_STAFF_USER_ID)
        if not existing:
            db.save_user({
                "id": DEMO_STAFF_USER_ID,
                "email": DEMO_STAFF_EMAIL,
                "name": "Emma Thompson",
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
            or existing.get("email") != DEMO_STAFF_EMAIL
        ):
            existing["venue_ids"] = [DEMO_VENUE_ID]
            existing["role"] = "staff"
            existing["is_active"] = True
            existing["email"] = DEMO_STAFF_EMAIL
            db.save_user(existing)
    except Exception:
        pass

    _demo_min_staff = {"floor": 1, "bar": 1, "kitchen": 1}  # a 3-body floor the demo roster meets
    _existing_venue = db.get_venue(DEMO_VENUE_ID)
    if not _existing_venue:
        db.save_venue(VenueConfig(
            id=DEMO_VENUE_ID,
            name="The Brass Monkey",
            tanda_org_id="demo-org-001",
            state=State.wa,
            timezone="Australia/Perth",
            min_staff=_demo_min_staff,
            max_labour_pct=30.0,
            pos_system="demo",
            created_at=now,
        ))
    elif getattr(_existing_venue, "min_staff", None) != _demo_min_staff:
        # Heal an earlier min_staff that the thin demo roster couldn't meet.
        try:
            _existing_venue.min_staff = _demo_min_staff
            db.save_venue(_existing_venue)
        except Exception:
            pass

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
                # Emma carries the staff-demo login email so /my links to her.
                email=DEMO_STAFF_EMAIL if f"demo-staff-{i:03d}" == DEMO_STAFF_EMPLOYEE_ID else None,
                availability=_DEMO_AVAILABILITY.get(f"demo-staff-{i:03d}", {}),
                phone=_DEMO_PHONES.get(f"demo-staff-{i:03d}"),
                created_at=now,
                updated_at=now,
            )
            for i, (name, role, rate) in enumerate(_DEMO_STAFF, start=1)
        ]
        db.save_employees(employees)
    else:
        # Self-heal: a demo-staff-001 seeded before the staff-side identity
        # existed has no email, so the staff demo would land on linked:false.
        try:
            emma = next((e for e in already if e.id == DEMO_STAFF_EMPLOYEE_ID), None)
            if emma is not None and (getattr(emma, "email", None) or "").strip().lower() != DEMO_STAFF_EMAIL:
                emma.email = DEMO_STAFF_EMAIL
                emma.updated_at = now
                db.save_employee(emma)
        except Exception:
            pass
        # Self-heal: rows seeded before availability/phone existed make the
        # "who can cover Saturday night?" demo beat come back empty.
        try:
            for e in already:
                changed = False
                if not getattr(e, "availability", None) and e.id in _DEMO_AVAILABILITY:
                    e.availability = _DEMO_AVAILABILITY[e.id]
                    changed = True
                if not getattr(e, "phone", None) and e.id in _DEMO_PHONES:
                    e.phone = _DEMO_PHONES[e.id]
                    changed = True
                if changed:
                    e.updated_at = now
                    db.save_employee(e)
        except Exception:
            pass

    # Starter menu (ingredients + recipes) so Menu & Sales, inventory and the
    # snapshot are never empty — previously only seeded if someone manually
    # POSTed /api/menu/seed, so a fresh deploy demoed blank.
    try:
        from rosteriq.routes.menu_costing import seed_starter_menu
        seed_starter_menu(db, DEMO_VENUE_ID)
        # Self-heal: ingredients seeded before sections existed all backfilled
        # to "kitchen" — put the coffee stock behind the machine so the demo's
        # section pills have something to show.
        try:
            for ing in db.list_ingredients(DEMO_VENUE_ID) or []:
                if ing.get("name") in ("Coffee beans", "Milk") and                         (ing.get("section") or "kitchen") == "kitchen":
                    ing["section"] = "bar"
                    db.save_ingredient(ing)
        except Exception:
            pass
    except Exception:
        pass

    # Day-rolling roster: ensure shifts exist for TODAY and the prior two
    # days — the SAME days the sales seed covers — so labour and revenue
    # always describe the same trading days and the prime cost reads like a
    # real venue (labour on days without sales made the demo look like a
    # failing business at 85%+ prime cost).
    try:
        from rosteriq.services.clock import venue_today
        today = venue_today(DEMO_VENUE_ID, db)  # the demo venue is Perth; seed 'today' in ITS day
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
        # All three trade days get labour, even when yesterday belongs to LAST
        # week (a Monday clamp used to drop Sat/Sun labour while sales still
        # topped those days up — prime cost read ~29%, a too-good-to-be-true
        # venue). Days are grouped into their own Mon-Sun rosters so the
        # roster validator stays honest.
        want_days = [today - timedelta(days=o) for o in (0, 1, 2)]
        by_week: dict = {}
        for d in want_days:
            ws = d - timedelta(days=d.weekday())
            by_week.setdefault(ws, []).append(d)
        for ws, days in by_week.items():
            rid = f"demo-roster-{ws.isoformat()}"
            existing = db.get_roster(rid)
            have_days = {s.date for s in existing.shifts} if existing else set()
            new_shifts = []
            for d in days:
                if d in have_days:
                    continue
                for i, role, st, en in _SHIFTS:
                    paid_hours = (en.hour - st.hour) - 0.5
                    new_shifts.append(Shift(
                        id=f"demo-shift-{d.isoformat()}-{i:03d}",
                        employee_id=f"demo-staff-{i:03d}",
                        date=d, start_time=st, end_time=en,
                        break_minutes=30, status=ShiftStatus.scheduled, role=role,
                        cost=Decimal(str(round(paid_hours * 32.5, 2))),
                    ))
            if new_shifts:
                all_shifts = (list(existing.shifts) if existing else []) + new_shifts
                db.save_roster(Roster(
                    id=rid,
                    venue_id=DEMO_VENUE_ID,
                    week_start=ws,
                    week_end=ws + timedelta(days=6),
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
    from rosteriq.services.clock import venue_today
    today = venue_today(DEMO_VENUE_ID, db)
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

    # Team feed (two-way: a staff swap ask with a manager reply, plus a pinned
    # manager post with a couple of thumbs-up). Re-asserted on EVERY Try Demo:
    # the two showcase posts are upserted by fixed id with their canonical
    # content, so a prospect who removed or edited one gets it back next
    # session, while posts prospects created themselves are left untouched.
    # (Guarding on "feed empty" let one removed post blank the showcase for
    # good — the pillar was never empty again, so it never re-seeded.)
    try:
        db.save_feed_post({
            "id": "demo-feed-002", "venue_id": DEMO_VENUE_ID,
            "author_user_id": DEMO_USER_ID, "author_name": "Management",
            "author_role": "manager",
            "body": "New pass-through window opens Friday — kitchen walkthrough "
                    "3pm Thursday",
            "pinned": True, "removed": False,
            "reactions": {"\U0001F44D": ["demo-staff-003", "demo-staff-006"]},
            "comments": [],
            "created_at": now - timedelta(hours=5), "updated_at": now,
        })
        db.save_feed_post({
            "id": "demo-feed-001", "venue_id": DEMO_VENUE_ID,
            "author_user_id": "demo-staff-002", "author_name": "James Wilson",
            "author_role": "staff",
            "body": "Anyone able to swap my Sat close? Family thing",
            "pinned": False, "removed": False,
            "reactions": {},
            "comments": [{
                "id": "demo-feed-001-c1", "author_user_id": DEMO_USER_ID,
                "author_name": "Management", "author_role": "manager",
                "body": "Post it as a shift cover in /my and I'll approve "
                        "whoever grabs it — Lisa's usually keen for Saturdays.",
                "created_at": now - timedelta(hours=1),
            }],
            "created_at": now - timedelta(hours=2), "updated_at": now,
        })
    except Exception:
        pass

    # SOP / JSP library: the four starter procedures, with 3 of 6 staff having
    # read Food safety so the manager view shows real outstanding names.
    # Re-asserted every Try Demo: seed_starter_sops is idempotent (deterministic
    # ids, skips titles that exist), so a starter a prospect deleted comes back
    # while a renamed/edited one is left alone; the acks are first-write-wins
    # in the store, so re-saving them is a no-op when they already exist.
    try:
        from rosteriq.routes.sops import seed_starter_sops, _starter_doc_id, STARTER_SOPS
        seed_starter_sops(db, DEMO_VENUE_ID, author_name="Management", now=now)
        food_title = next((s["title"] for s in STARTER_SOPS
                           if str(s.get("title", "")).lower().startswith("food safety")),
                          "Food safety & allergen declaration")
        food = db.get_sop_document(_starter_doc_id(DEMO_VENUE_ID, food_title))
        if not food:
            # A demo library seeded before starter ids became deterministic
            # holds Food safety under a random id (seed_starter_sops skips it
            # by title) — fall back to the title so its acks still re-assert.
            food = next((d for d in (db.list_sop_documents(DEMO_VENUE_ID) or [])
                         if str(d.get("title", "")).lower().startswith("food safety")), None)
        if food:
            version = int(food.get("version") or 1)
            have = {
                a.get("employee_id")
                for a in (db.list_sop_acks(DEMO_VENUE_ID, food["id"]) or [])
                if int(a.get("doc_version") or 0) == version
            }
            for i, (name, _role, _rate) in enumerate(_DEMO_STAFF[:3], start=1):
                emp_id = f"demo-staff-{i:03d}"
                if emp_id in have:
                    continue  # first-write-wins anyway; skip the round-trip
                # Fixed ids for the canonical v1 acks; a later version gets its
                # own ids so PG's PK on id can't collide with the v1 rows.
                ack_id = f"demo-sop-ack-{i:03d}" if version == 1 else f"demo-sop-ack-{i:03d}-v{version}"
                try:
                    db.save_sop_ack({
                        "id": ack_id, "venue_id": DEMO_VENUE_ID,
                        "doc_id": food["id"], "doc_version": version,
                        "employee_id": emp_id, "employee_name": name,
                        "acknowledged_at": now - timedelta(hours=3 + i),
                    })
                except Exception:
                    continue  # one ack failing must not drop the others
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

    # A couple of waste entries so the Inventory waste chip + report demo.
    # Rows only (no stock depletion) so the shelf numbers above stay put.
    try:
        ingredients = db.list_ingredients(DEMO_VENUE_ID) or []
        if ingredients and not db.list_waste_entries(
                DEMO_VENUE_ID, today - timedelta(days=7), today):
            for n, (offset, reason, frac) in enumerate([
                    (1, "spoiled", 0.25), (3, "prep_waste", 0.15)]):
                ing = ingredients[n % len(ingredients)]
                cpu = float(ing.get("cost_per_unit") or 0)
                pack = float(ing.get("purchase_size") or 1) or 1
                qty = round(pack * frac, 3)
                db.save_waste_entry({
                    "id": f"demo-waste-{(today - timedelta(days=offset)).isoformat()}-{n}",
                    "venue_id": DEMO_VENUE_ID,
                    "ingredient_id": ing["id"], "ingredient_name": ing["name"],
                    "waste_date": today - timedelta(days=offset),
                    "qty": qty, "unit": ing.get("unit"),
                    "reason": reason, "value": round(qty * cpu, 2),
                    "note": "", "logged_by": DEMO_USER_ID,
                    "created_at": now,
                })
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
            # Target-based, per-day convergent: bring each of the last three
            # trading days (the SAME days the roster seeds labour for) up to
            # ~$5,300 inc GST, so $1,462.50/day of labour reads as a healthy
            # ~30% labour / ~50% prime cost — not a failing venue. Whatever a
            # day already holds, it tops up toward the target then stops, so
            # re-seeding never inflates and stale small-scale days self-heal.
            mix = {"Chicken Parmigiana": 0.63, "Bowl of Chips": 0.20, "Flat White": 0.17}
            by_name = {r.get("name"): r for r in recipes}
            target = 5300.0
            # Every day in the snapshot's trailing 7-day window that carries
            # demo labour gets sales at target — not just the 3 freshly-seeded
            # days. Long-running prod accumulated a shift-day per seeder run
            # (this week's AND last week's roster), and any labour day left
            # without matching revenue drags the 7-day prime cost back into
            # "failing business" territory.
            trade_days = {today - timedelta(days=o) for o in (0, 1, 2)}
            window_start = today - timedelta(days=6)
            for ws in (week_start, week_start - timedelta(days=7)):
                r = db.get_roster(f"demo-roster-{ws.isoformat()}")
                if r:
                    trade_days |= {s.date for s in r.shifts
                                   if window_start <= s.date <= today}
            for sale_day in sorted(trade_days):
                existing = db.list_dish_sales(DEMO_VENUE_ID, sale_day, sale_day) or []
                have = sum(float(s.get("revenue_inc_gst") or 0) for s in existing)
                deficit = target - have
                if deficit < 300:
                    continue  # this day already trades at scale
                for n, (name, frac) in enumerate(sorted(mix.items())):
                    recipe = by_name.get(name)
                    if not recipe:
                        continue
                    costing = _cost_recipe(recipe, ings)
                    price = float(costing["sell_price_inc_gst"]) or 1
                    qty = max(1, round(deficit * frac / price))
                    db.save_dish_sale({
                        "id": f"demo-sale-{sale_day.isoformat()}-t{n}",
                        "venue_id": DEMO_VENUE_ID,
                        "sale_date": sale_day,
                        "recipe_id": recipe["id"],
                        "recipe_name": recipe.get("name"),
                        "qty": qty,
                        "revenue_inc_gst": round(price * qty, 2),
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
        # Full trading-day curve, tuned so the demo roster covers EVERY hour
        # (per-hour check, not just the busiest): quiet 11-14 and 19-22 need 3
        # (=coverage 3), a mid-afternoon game-day surge 15-18 that the 5-6
        # rostered staff meet, peak 15:00 needing 6. Seeded authoritatively
        # (upsert by venue/date/hour/model_version) so it overwrites any
        # earlier demo curve rather than leaving stale hours behind.
        curve = {11: 30, 12: 30, 13: 30, 14: 30, 15: 85, 16: 78,
                 17: 70, 18: 55, 19: 30, 20: 30, 21: 30, 22: 30}
        target = {(today, hr, float(cov)) for hr, cov in curve.items()}
        have = {(f.date, f.hour, float(f.predicted_covers))
                for f in (db.get_forecasts(DEMO_VENUE_ID, today, today) or [])}
        if not target.issubset(have):  # idempotent: skip once it already matches
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
