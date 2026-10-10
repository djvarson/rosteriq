"""
The hours a venue trades, as every demand reading uses them: the cold-start
forecast, roster coverage and the morning briefing.

Each weekday takes the first of:
    recorded        VenueConfig.trading_hours (Settings → Areas & minimums),
                    hours or {"closed": True}
    roster history  the venue's published or imported rosters: the earliest
                    start and latest end rostered on that weekday
    default         10:00 to midnight

Outside trading hours there is no demand, so a closed day has none. A
forecast's (date, hour) is clock time on that date, so a Friday that closes
at 01:00 trades in Saturday's 00:00 hour, and that hour belongs to Friday's
trading day (and Friday's minimums).
"""

import logging
from datetime import date, datetime, time, timedelta
from types import SimpleNamespace
from typing import Optional

from rosteriq.services.venue_areas import DAY_KEYS, day_key, is_closed, trading_window

logger = logging.getLogger(__name__)

DEFAULT_OPEN = "10:00"
DEFAULT_CLOSE = "00:00"
HISTORY_WEEKS_BACK = 12
HISTORY_WEEKS_AHEAD = 4


def _hhmm(minutes: int) -> str:
    minutes %= 24 * 60
    return f"{minutes // 60:02d}:{minutes % 60:02d}"


def inferred_trading_hours(db, venue_id: str, around: date) -> dict:
    """{weekday: {"open", "close"}} from the published or imported rosters in
    the weeks around `around`; weekdays with no shifts are left out."""
    from rosteriq.services.current_roster import live_rosters
    start = around - timedelta(weeks=HISTORY_WEEKS_BACK)
    end = around + timedelta(weeks=HISTORY_WEEKS_AHEAD)
    try:
        rosters = live_rosters(db, venue_id, start, end, staff=True)
    except Exception as e:
        logger.warning(f"Roster history lookup failed for {venue_id}: {e}")
        return {}
    spans: dict = {}
    for roster in rosters:
        for s in getattr(roster, "shifts", None) or []:
            if getattr(s, "status", None) == "cancelled" or getattr(s, "date", None) is None:
                continue
            minutes = int(round(s.duration_hours * 60))
            if minutes <= 0:
                continue
            opens = s.start_time.hour * 60 + s.start_time.minute
            closes = opens + minutes
            key = day_key(s.date)
            lo, hi = spans.get(key, (opens, closes))
            spans[key] = (min(lo, opens), max(hi, closes))
    out = {}
    for key in DAY_KEYS:
        if key in spans:
            lo, hi = spans[key]
            out[key] = {"open": _hhmm(lo), "close": _hhmm(min(hi, lo + 24 * 60))}
    return out


def effective_trading_hours(venue, inferred: Optional[dict] = None) -> dict:
    """All seven weekdays: recorded hours or closed, else `inferred`, else
    the default, each tagged with its source."""
    recorded = getattr(venue, "trading_hours", None) or {}
    out = {}
    for key in DAY_KEYS:
        rec = recorded.get(key) or {}
        if rec.get("closed") is True:
            out[key] = {"closed": True, "source": "recorded"}
        elif rec.get("open") and rec.get("close"):
            out[key] = {"open": rec["open"], "close": rec["close"], "source": "recorded"}
        elif inferred and (inferred.get(key) or {}).get("closed") is True:
            out[key] = {"closed": True, "source": inferred[key].get("source") or "recorded"}
        elif inferred and key in inferred:
            out[key] = {"open": inferred[key]["open"], "close": inferred[key]["close"],
                        "source": "roster history"}
        else:
            out[key] = {"open": DEFAULT_OPEN, "close": DEFAULT_CLOSE, "source": "default"}
    return out


def venue_trading_hours(db, venue, around: date) -> dict:
    """effective_trading_hours, reading roster history only when the venue
    hasn't recorded every weekday."""
    recorded = getattr(venue, "trading_hours", None) or {}
    inferred = None
    if db is not None and any(not recorded.get(k) for k in DAY_KEYS):
        inferred = inferred_trading_hours(db, venue.id, around)
    return effective_trading_hours(venue, inferred)


def _window(hours_map: dict, day: date):
    return trading_window(SimpleNamespace(trading_hours=hours_map or {}), day)


def _closed(hours_map: dict, day: date) -> bool:
    return is_closed(SimpleNamespace(trading_hours=hours_map or {}), day)


def trading_day_of(hours_map: dict, day: date, hour: int) -> Optional[date]:
    """The trading day the clock hour (day, hour) belongs to: `day`, the day
    before (a close past midnight), or None when it is outside trading hours.
    A day missing from hours_map is taken as trading all day; a closed one
    trades none of its own hours."""
    cell = datetime.combine(day, time(hour))
    cell_end = cell + timedelta(hours=1)
    for d in (day, day - timedelta(days=1)):
        window = _window(hours_map, d)
        if window and window[0] < cell_end and cell < window[1]:
            return d
    return day if _window(hours_map, day) is None and not _closed(hours_map, day) else None


def in_trading_hours(hours_map: dict, day: date, hour: int) -> bool:
    return trading_day_of(hours_map, day, hour) is not None


def demand_hours(hours_map: dict, day: date) -> list:
    """The clock hours of `day` inside trading hours, in order."""
    return [h for h in range(24) if in_trading_hours(hours_map, day, h)]
