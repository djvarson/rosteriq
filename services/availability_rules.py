"""
The one meaning of Employee.availability.

Written by the staff app (routes/staff_portal.py POST /api/me/availability)
and read by the generator, the publisher's conflict check, the AI tools,
call-in and cover suggestions, bidding and no-show backups:

* {} / None                -> no constraints: available any time
* a day that isn't listed  -> available all day
* a day listed with []     -> unavailable that day
* a day listed with ranges -> available only inside one of the ranges
  ([{"start": "HH:MM", "end": "HH:MM"}], compared to the minute)

Readers used to disagree (some treated an unlisted day as unavailable, some
treated every list as available), so the same person could be rostered,
refused at publish, and offered as cover on the same day.
"""

from datetime import date, datetime, time
from typing import Optional, Union

_DAY_NAMES = ("monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday")


def _minutes(v, default: int) -> int:
    if v is None or v == "":
        return default
    if isinstance(v, time):
        return v.hour * 60 + v.minute
    try:
        text = str(v).strip()[:5]
        hh, mm = text.split(":")[:2] if ":" in text else (text, "0")   # "9" = 09:00
        m = int(hh) * 60 + int(mm)
        return 24 * 60 if m == 23 * 60 + 59 else m   # "23:59" means end of day
    except Exception:
        return default


def _day_name(day: Union[str, date, datetime]) -> str:
    if isinstance(day, (date, datetime)):
        return _DAY_NAMES[day.weekday()]
    return str(day).strip().lower()


def day_windows(availability: Optional[dict], day) -> Optional[list]:
    """None = no constraint that day (available all day); [] = unavailable;
    otherwise the list of {"start","end"} windows."""
    if not availability:
        return None
    name = _day_name(day)
    if name not in availability:
        return None
    raw = availability.get(name)
    if raw is None or raw is True:
        return None
    if raw is False:
        return []
    if isinstance(raw, dict):             # legacy single-window shape
        return [raw]
    return list(raw)


def is_available(availability: Optional[dict], day, start=None, end=None) -> bool:
    """Is the person available on `day` for the whole of [start, end)?
    Without start/end: are they available at all that day?"""
    windows = day_windows(availability, day)
    if windows is None:
        return True
    if not windows:
        return False
    if start is None or end is None:
        return True
    s = _minutes(start, 0)
    e = _minutes(end, 24 * 60)
    if e <= s:                            # an overnight shift runs to end of day
        e = 24 * 60
    return any(_minutes(w.get("start"), 0) <= s and e <= _minutes(w.get("end"), 24 * 60)
               for w in windows if isinstance(w, dict))
