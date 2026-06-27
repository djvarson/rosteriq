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
from decimal import Decimal

from rosteriq.models import (
    VenueConfig, Employee, EmploymentType, AwardLevel, State,
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
        if not db.get_user_by_id(DEMO_USER_ID):
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
