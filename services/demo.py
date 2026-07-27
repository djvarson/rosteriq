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
