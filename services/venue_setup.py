"""
Venue setup: what POST /venues may change on a venue that already exists,
and the areas a venue's own data suggests (Settings → Areas & minimums and
the setup wizard's areas step).

Re-running the setup wizard posts the venue again. On an existing venue:
  * an empty areas / trading_hours / min_staff keeps what is stored, unless
    the caller names the field in ``clear`` (the Areas & minimums card does
    when someone removes every area or every day's hours)
  * the name only changes with ``rename`` (the wizard's Rename button, or an
    edited name in Settings)
  * created_at never changes
Whenever a venue has areas, min_staff is each area's highest weekday minimum,
for readers that only know min_staff (section pills, older reports).
"""

import re
from datetime import date, timedelta
from typing import Iterable, Optional

from rosteriq.models import VenueConfig, normalise_skills

KEPT_WHEN_EMPTY = ("areas", "trading_hours", "min_staff")


def parse_clear(value: Optional[str]) -> set:
    return {p.strip() for p in str(value or "").split(",") if p.strip() in KEPT_WHEN_EMPTY}


def min_staff_from_areas(areas: list) -> dict:
    return {a["name"]: max((a.get("min_by_day") or {}).values(), default=0) for a in areas}


def merge_venue_update(existing: Optional[VenueConfig], incoming: VenueConfig, *,
                       rename: bool = False, clear: Iterable[str] = ()) -> tuple:
    """(venue to save, [fields kept from the stored venue])."""
    clear = set(clear)
    if "areas" in clear:
        # min_staff is derived from the areas; it goes with them.
        clear.add("min_staff")
    updates: dict = {}
    kept: list = []
    if existing is not None:
        updates["created_at"] = existing.created_at
        if not rename and incoming.name != existing.name:
            updates["name"] = existing.name
            kept.append("name")
        for field in KEPT_WHEN_EMPTY:
            if not getattr(incoming, field) and field not in clear and getattr(existing, field):
                updates[field] = getattr(existing, field)
                kept.append(field)
    areas = updates.get("areas", incoming.areas)
    if areas:
        updates["min_staff"] = min_staff_from_areas(areas)
        if "min_staff" in kept:
            kept.remove("min_staff")
    if not updates:
        return incoming, kept
    return VenueConfig.model_validate({**incoming.model_dump(), **updates}), kept


def area_candidates(db, venue_id: str, around: date) -> list:
    """The distinct skills of the venue's active staff and the shift roles of
    its published or imported rosters, most used first."""
    from rosteriq.services.current_roster import live_rosters
    counts: dict = {}

    def _bump(name, key):
        if name == "general" or len(name) > 40 or not re.fullmatch(r"[a-z0-9 _/&+.-]+", name):
            return
        row = counts.setdefault(name, {"name": name, "label": name.title(), "staff": 0, "shifts": 0})
        row[key] += 1

    for emp in db.get_employees(venue_id) or []:
        if getattr(emp, "venue_id", venue_id) != venue_id or not getattr(emp, "active", True):
            continue
        for skill in normalise_skills(getattr(emp, "skills", None)):
            _bump(skill, "staff")
    try:
        rosters = live_rosters(db, venue_id, around - timedelta(weeks=12),
                               around + timedelta(weeks=4), staff=True)
    except Exception:
        rosters = []
    for roster in rosters:
        for shift in getattr(roster, "shifts", None) or []:
            for role in normalise_skills(getattr(shift, "role", None)):
                _bump(role, "shifts")
    return sorted(counts.values(), key=lambda r: (-(r["staff"] + r["shifts"]), r["name"]))
