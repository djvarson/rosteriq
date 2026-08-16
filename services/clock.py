"""
Venue-local clock. The app runs in UTC (Railway); every venue is in Australia
(UTC+8 to +11). Between local midnight and ~08:00 Perth / 10:00 Sydney the
host's date.today() is still YESTERDAY, so a manager opening the briefing at
6am saw yesterday's coverage and staff saw last night's shift as "today".

Use venue_today(venue_id) / venue_now(venue_id) wherever "today" is shown to
a person or decides what belongs to today (briefing, snapshot window, staff
portal, time clock, coverage). Rolling analytics windows can stay on UTC.

Never raises: unknown venue / bad tz name falls back to the host clock.
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Optional

DEFAULT_TZ = "Australia/Perth"

_tz_cache: dict = {}


def venue_timezone(venue_id: Optional[str], db=None) -> str:
    """The venue's IANA timezone name (VenueConfig.timezone), cached."""
    if not venue_id:
        return DEFAULT_TZ
    if venue_id in _tz_cache:
        return _tz_cache[venue_id]
    tz = DEFAULT_TZ
    try:
        if db is None:
            from rosteriq.database import get_db
            db = get_db()
        v = db.get_venue(venue_id)
        cand = getattr(v, "timezone", None) if v is not None else None
        if cand:
            from zoneinfo import ZoneInfo
            ZoneInfo(cand)  # validate
            tz = cand
    except Exception:
        tz = DEFAULT_TZ
    _tz_cache[venue_id] = tz
    return tz


def forget_venue_tz(venue_id: str) -> None:
    """Call after a venue's timezone is edited."""
    _tz_cache.pop(venue_id, None)


def venue_now(venue_id: Optional[str], db=None) -> datetime:
    """Timezone-aware 'now' in the venue's local time."""
    try:
        from zoneinfo import ZoneInfo
        return datetime.now(ZoneInfo(venue_timezone(venue_id, db)))
    except Exception:
        return datetime.now()


def venue_today(venue_id: Optional[str], db=None) -> date:
    """The venue's local calendar date right now."""
    try:
        return venue_now(venue_id, db).date()
    except Exception:
        return date.today()
