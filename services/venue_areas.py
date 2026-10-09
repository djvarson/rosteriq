"""
A venue's areas, their minimum staffing by weekday, and its trading hours —
the one reading every rostering feature uses (generator, coverage, conflict
checks, On-Shift, Day view, settings).

VenueConfig.areas (set in Settings → Areas & minimums):
    [{"name": "front bar", "label": "Front Bar",
      "min_by_day": {"mon": 1, "tue": 1, "wed": 2, ..., "sun": 2},
      "requires_skill": False}]
  * name is the lower-case key staff skills and shift roles are matched on
  * requires_skill: only people with this area in their skills may work it
    (duty manager, supervisor). Otherwise people with the skill — or with no
    skills recorded at all — may.
  * a venue with no areas falls back to min_staff ({area: count}, every day)

VenueConfig.trading_hours:
    {"mon": {"open": "10:00", "close": "00:00"}, ...}
  * close at or before open runs past midnight (Fri "11:00"-"01:00")
  * a weekday that is absent, or no hours at all, means "not recorded": callers
    fall back to their own default rather than assuming the venue is closed
"""

from datetime import date, datetime, time, timedelta
from typing import Optional

DAY_KEYS = ("mon", "tue", "wed", "thu", "fri", "sat", "sun")
_SUPERVISORY = ("duty manager", "manager", "supervisor")


def day_key(day) -> str:
    if isinstance(day, (date, datetime)):
        return DAY_KEYS[day.weekday()]
    return str(day).strip().lower()[:3]


def _norm(name) -> str:
    return " ".join(str(name or "").strip().lower().split())


def venue_areas(venue) -> list:
    """The venue's areas, normalised. Derived from min_staff when none are set."""
    areas = getattr(venue, "areas", None) or []
    out = []
    if areas:
        for a in areas:
            a = a if isinstance(a, dict) else dict(a)
            name = _norm(a.get("name"))
            if not name:
                continue
            by_day = a.get("min_by_day") or {}
            out.append({
                "name": name,
                "label": a.get("label") or name.title(),
                "min_by_day": {d: int(by_day.get(d, 0) or 0) for d in DAY_KEYS},
                "requires_skill": bool(a.get("requires_skill",
                                             any(s in name for s in _SUPERVISORY))),
            })
        return out
    for name, count in (getattr(venue, "min_staff", None) or {}).items():
        name = _norm(name)
        if name:
            out.append({
                "name": name,
                "label": name.title(),
                "min_by_day": {d: int(count or 0) for d in DAY_KEYS},
                "requires_skill": any(s in name for s in _SUPERVISORY),
            })
    return out


def area_names(venue) -> list:
    return [a["name"] for a in venue_areas(venue)]


def min_staff_for(venue, day) -> dict:
    """{area: minimum} for that weekday (areas with 0 are left out)."""
    key = day_key(day)
    return {a["name"]: a["min_by_day"][key] for a in venue_areas(venue) if a["min_by_day"].get(key)}


def find_area(venue, name) -> Optional[dict]:
    name = _norm(name)
    return next((a for a in venue_areas(venue) if a["name"] == name), None)


def employee_skills(employee) -> set:
    return {_norm(s) for s in (getattr(employee, "skills", None) or []) if _norm(s)}


def can_fill(employee, area_name, venue=None) -> bool:
    """May this person work this area? Case-insensitive skill match; an area
    that requires the skill (duty manager) never takes someone without it."""
    area_name = _norm(area_name)
    skills = employee_skills(employee)
    if area_name in skills:
        return True
    area = find_area(venue, area_name) if venue is not None else None
    requires = area["requires_skill"] if area else any(s in area_name for s in _SUPERVISORY)
    return not requires and not skills


def _hhmm(value) -> Optional[time]:
    try:
        hh, mm = str(value).strip()[:5].split(":")
        h, m = int(hh), int(mm)
        if h == 24 and m == 0:
            return time(0, 0)
        return time(h, m)
    except (ValueError, TypeError):
        return None


def trading_window(venue, day: date) -> Optional[tuple]:
    """(open, close) as datetimes for the trading day starting on `day`, or
    None when the venue hasn't recorded hours for that weekday."""
    hours = (getattr(venue, "trading_hours", None) or {}).get(day_key(day))
    if not hours:
        return None
    opens, closes = _hhmm(hours.get("open")), _hhmm(hours.get("close"))
    if opens is None or closes is None:
        return None
    start = datetime.combine(day, opens)
    end = datetime.combine(day, closes)
    if end <= start:
        end += timedelta(days=1)
    return start, end


def trading_hours_of_day(venue, day: date) -> Optional[list]:
    """The clock hours (0-23, in order, possibly wrapping past midnight) the
    venue trades on that day, or None when not recorded."""
    window = trading_window(venue, day)
    if not window:
        return None
    start, end = window
    hours, cur = [], start.replace(minute=0)
    while cur < end and len(hours) < 24:
        hours.append(cur.hour)
        cur += timedelta(hours=1)
    return hours
