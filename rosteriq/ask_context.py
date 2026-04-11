"""
ask_context — build a QueryContext for the /api/v1/ask endpoint.

The query_library module is pure stdlib and duck-types its inputs, so we
can hand it any dataclasses that match the attribute shapes it expects.

This file does two things:

  1. Defines lightweight dataclasses (ShiftRow, RosterRow, VendorForecast,
     HeadCountRow) with the fields query_library.py needs.

  2. `build_demo_query_context(venue_id, today)` — synthesises ~12 weeks
     of plausible historical data for the venue so the query library has
     something to answer from during a demo. This mirrors the kind of
     weekly cadence an Australian hospitality venue would have: slow
     Tuesdays, big Fridays and Saturdays, moderate Sundays.

When a real pilot venue is connected, swap `build_demo_query_context`
for a DB-backed builder that reads shifts / rosters / forecasts from
Postgres. The query_library itself won't change — it duck-types over any
object with the right attribute names.

Design notes:

  * Everything is deterministic. Given the same (venue_id, today),
    every call returns byte-identical synthetic data. This is critical
    for a demo — you don't want different numbers every time you refresh.
    We seed a random.Random() with a hash of (venue_id, today.isoformat()).

  * Sales are driven from the same weekly curve as labour cost so the
    wage % answers are consistent with the dashboard's revenue tracker.

  * The synthetic week matches the parameters in RosterIQ's marketing:
    ~$120k weekly revenue, 28-32% wage cost, peaks Friday/Saturday.
"""

from __future__ import annotations

import hashlib
import random
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from typing import Any, List, Optional


# ---------------------------------------------------------------------------
# Lightweight dataclasses — shape matches query_library's duck-typed protocols
# ---------------------------------------------------------------------------

@dataclass
class ShiftRow:
    """One shift. Matches _ShiftLike."""
    employee_id: str
    date: date
    start_time: time
    end_time: time
    hours: float
    cost: Decimal
    is_overtime: bool = False
    role: str = "floor"

    @property
    def net_hours(self) -> float:
        return self.hours


@dataclass
class RosterRow:
    """A weekly roster envelope. Matches _RosterLike."""
    venue_id: str
    week_start: date
    shifts: List[ShiftRow] = field(default_factory=list)


@dataclass
class VendorForecastRow:
    """One vendor forecast bucket. Matches _VendorForecastLike + the
    VendorForecastRow in query_library.py."""
    bucket_start: datetime
    bucket_end: datetime
    metric: str          # "revenue", "covers", "foot_traffic"
    amount: Decimal
    department: Optional[str] = None


@dataclass
class HeadCountRow:
    """One head-count sample. Matches _HeadCountLike."""
    counted_at: datetime
    count: int
    zone: Optional[str] = None


@dataclass
class EmployeeRow:
    """Minimal employee record. Matches _EmployeeLike."""
    id: str
    employment_type: str = "casual"
    name: str = ""


# ---------------------------------------------------------------------------
# Demo data generator — deterministic per (venue_id, today)
# ---------------------------------------------------------------------------

# Weekly baseline: Mon=0 ... Sun=6. Tuned for a mid-size AU pub/bistro.
# Values are "typical daily revenue" in AUD.
_WEEKLY_REVENUE_BASELINE = {
    0: 8_500,    # Mon
    1: 7_200,    # Tue (slowest)
    2: 9_800,    # Wed
    3: 12_400,   # Thu
    4: 22_800,   # Fri
    5: 28_600,   # Sat (peak)
    6: 18_400,   # Sun
}

# Target wage % that the venue is trying to hit (used to back-compute labour)
_TARGET_WAGE_PCT = 0.30

# A handful of fake employees with stable IDs for the demo
_DEMO_EMPLOYEES: List[EmployeeRow] = [
    EmployeeRow(id="EMP001", name="Sarah Chen",      employment_type="fulltime"),
    EmployeeRow(id="EMP002", name="Marcus Johnson",  employment_type="parttime"),
    EmployeeRow(id="EMP003", name="Emma Davis",      employment_type="casual"),
    EmployeeRow(id="EMP004", name="Alex Park",       employment_type="casual"),
    EmployeeRow(id="EMP005", name="Jamie Taylor",    employment_type="fulltime"),
    EmployeeRow(id="EMP006", name="Olivia Nguyen",   employment_type="parttime"),
    EmployeeRow(id="EMP007", name="Liam Roberts",    employment_type="casual"),
    EmployeeRow(id="EMP008", name="Mia Williams",    employment_type="casual"),
]


def _seeded_rng(venue_id: str, today: date) -> random.Random:
    """Deterministic RNG keyed off (venue, today) so the demo data is
    stable across refreshes within the same day but varies if you
    actually come back tomorrow."""
    seed_bytes = f"{venue_id}::{today.isoformat()}".encode("utf-8")
    seed_int = int.from_bytes(hashlib.sha256(seed_bytes).digest()[:8], "big")
    return random.Random(seed_int)


def _generate_day(
    venue_id: str,
    day: date,
    rng: random.Random,
) -> tuple[List[ShiftRow], Decimal]:
    """Generate shifts + revenue for one day. Returns (shifts, revenue)."""
    weekday = day.weekday()
    baseline = _WEEKLY_REVENUE_BASELINE[weekday]
    # ±15% daily variance, deterministic per (venue, day)
    variance = 1.0 + ((rng.random() - 0.5) * 0.30)
    revenue = Decimal(str(round(baseline * variance, 2)))

    # Target labour for the day (not always hit — see jitter below)
    target_labour = float(revenue) * _TARGET_WAGE_PCT

    # Actual labour: ~85% of days hit target ±5%, ~15% blow out to 35-45%
    if rng.random() < 0.15:
        actual_labour = target_labour * rng.uniform(1.15, 1.50)
    else:
        actual_labour = target_labour * rng.uniform(0.90, 1.10)

    # Split that labour across 3-6 shifts for the day
    num_shifts = rng.choice([3, 4, 4, 5, 5, 6])
    shift_pool = rng.sample(_DEMO_EMPLOYEES, num_shifts)

    shifts: List[ShiftRow] = []
    per_shift_cost = actual_labour / num_shifts
    for emp in shift_pool:
        # Shift durations between 4 and 8 hours
        hours = round(rng.uniform(4.0, 8.0), 2)
        start_h = rng.choice([10, 11, 12, 15, 16, 17])
        end_h = min(23, start_h + int(hours))
        is_ot = hours > 7.5 and rng.random() < 0.3
        shifts.append(ShiftRow(
            employee_id=emp.id,
            date=day,
            start_time=time(start_h, 0),
            end_time=time(end_h, 0),
            hours=hours,
            cost=Decimal(str(round(per_shift_cost, 2))),
            is_overtime=is_ot,
            role=rng.choice(["floor", "bar", "kitchen", "floor"]),
        ))

    return shifts, revenue


def build_demo_query_context(
    venue_id: str,
    today: Optional[date] = None,
    weeks_of_history: int = 12,
) -> Any:
    """Build a QueryContext populated with ~12 weeks of synthetic data.

    Returns a QueryContext (imported inside the function to avoid a hard
    dependency at module load time — tests can monkeypatch).

    Args:
        venue_id: The venue to generate data for
        today: The "current date" (defaults to real today)
        weeks_of_history: How far back to go (default 12)
    """
    # Deferred import so this module is importable even if query_library
    # hasn't been loaded yet (e.g. in isolated unit tests).
    from rosteriq.query_library import QueryContext

    if today is None:
        today = date.today()

    rng = _seeded_rng(venue_id, today)

    # Generate a shift roster per week, back N weeks from today.
    # Period is half-open: we include today and go back weeks_of_history*7 days.
    start_day = today - timedelta(days=weeks_of_history * 7)

    rosters: List[RosterRow] = []
    all_shifts: List[ShiftRow] = []
    vendor_forecasts: List[VendorForecastRow] = []

    # Walk one week at a time
    current = start_day
    while current <= today:
        # Align to Monday of the week
        week_start = current - timedelta(days=current.weekday())
        roster = RosterRow(venue_id=venue_id, week_start=week_start, shifts=[])

        for offset in range(7):
            day = week_start + timedelta(days=offset)
            if day > today or day < start_day:
                continue
            shifts, revenue = _generate_day(venue_id, day, rng)
            roster.shifts.extend(shifts)
            all_shifts.extend(shifts)

            # Vendor forecast row for this day (one revenue bucket per day)
            bucket_start = datetime.combine(day, time(0, 0), tzinfo=timezone.utc)
            bucket_end = bucket_start + timedelta(days=1)
            vendor_forecasts.append(VendorForecastRow(
                bucket_start=bucket_start,
                bucket_end=bucket_end,
                metric="revenue",
                amount=revenue,
                department=None,
            ))

        rosters.append(roster)
        # Jump to next Monday
        current = week_start + timedelta(days=7)

    # Head-count samples: one per hour on the most recent 14 days
    head_counts: List[HeadCountRow] = []
    for day_offset in range(14):
        day = today - timedelta(days=day_offset)
        for hour in range(11, 23):  # 11am-10pm
            hod_baseline = _hour_of_day_baseline(hour, day.weekday())
            count = max(0, int(hod_baseline * rng.uniform(0.8, 1.2)))
            head_counts.append(HeadCountRow(
                counted_at=datetime.combine(day, time(hour, 0), tzinfo=timezone.utc),
                count=count,
                zone=None,
            ))

    employees_dict = {emp.id: emp for emp in _DEMO_EMPLOYEES}

    return QueryContext(
        venue_id=venue_id,
        today=today,
        shifts=[],                # all shifts live inside rosters above
        rosters=rosters,
        vendor_forecasts=vendor_forecasts,
        head_counts=head_counts,
        employees=employees_dict,
        timezone_label="Australia/Melbourne",
    )


def _hour_of_day_baseline(hour: int, weekday: int) -> int:
    """Rough head count at a given hour for a given weekday.

    Approximates the lunch bump (12-14), quiet afternoon, and dinner
    peak (18-21), scaled by the day's overall volume.
    """
    # Base curve by hour of day
    if hour in (12, 13):
        base = 85   # lunch
    elif hour == 14:
        base = 55
    elif hour in (15, 16, 17):
        base = 25   # afternoon quiet
    elif hour in (18, 19, 20):
        base = 110  # dinner peak
    elif hour == 21:
        base = 80
    else:
        base = 35

    # Scale by day of week relative to Wednesday
    day_scales = {0: 0.7, 1: 0.6, 2: 0.8, 3: 1.0, 4: 1.5, 5: 1.8, 6: 1.2}
    return int(base * day_scales[weekday])
