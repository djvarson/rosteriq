"""
One live roster per venue-week.

A venue can hold several rosters for the same Mon-Sun week: every Generate
saves a new one, the CSV importer saves ``imported-<venue>-<monday>``, and
publishing marks one of them live. Readers take ONE roster per week from
here — never the sum of every roster saved for that week.

    staff=True   staff-facing views (/my shifts and home, shift cover, the
                 kiosk's "rostered" times): the PUBLISHED roster, else the
                 imported one (it was already published in the old system).
                 Never an unpublished draft.
    staff=False  manager views (briefing, snapshot labour, coverage, the
                 dashboard's week): the PUBLISHED roster, else the newest
                 roster of the week, drafts included.

An archived roster was published, so it ranks just below a published one.
A superseded roster is kept as history and no reader sees it. Generating or
importing a week supersedes that week's unpublished generated drafts;
publishing a roster supersedes every other roster for its week, the imported
one included, so recalling it later never puts the old system's week back on
staff phones.
"""

from datetime import date, datetime, timedelta, timezone
from typing import Iterable, Optional

PUBLISHED = "published"
ARCHIVED = "archived"
SUPERSEDED = "superseded"
IMPORTED_PREFIX = "imported-"

# Not-yet-live states that a newer roster for the same week replaces. A roster
# mid-push ("publishing") is left alone so the push can finish.
_REPLACEABLE = frozenset({"draft", "pending_review", "approved", "rejected", "failed"})


def week_monday(d: date) -> date:
    return d - timedelta(days=d.weekday())


def is_imported(roster) -> bool:
    return str(getattr(roster, "id", "") or "").startswith(IMPORTED_PREFIX)


def roster_state(db, roster_id: str) -> str:
    try:
        return db.get_roster_state(roster_id) or "draft"
    except Exception:
        return "draft"


def _age(roster):
    created = getattr(roster, "created_at", None)
    if isinstance(created, datetime):
        if created.tzinfo is not None:
            created = created.astimezone(timezone.utc).replace(tzinfo=None)
    else:
        created = datetime.min
    return (created, str(getattr(roster, "id", "")))


def pick(db, rosters: list, staff: bool = False):
    """The live roster among ONE week's rosters, or None."""
    states = {r.id: roster_state(db, r.id) for r in rosters}
    live = [r for r in rosters if states[r.id] != SUPERSEDED]
    for wanted in (PUBLISHED, ARCHIVED):
        ranked = [r for r in live if states[r.id] == wanted]
        if ranked:
            return max(ranked, key=_age)
    if staff:
        live = [r for r in live if is_imported(r)]
    return max(live, key=_age) if live else None


def _by_week(rosters, venue_id: str) -> dict:
    weeks: dict = {}
    for r in rosters or []:
        if getattr(r, "venue_id", None) != venue_id:
            continue
        weeks.setdefault(week_monday(r.week_start), []).append(r)
    return weeks


def live_rosters(db, venue_id: str, start: date, end: date, *, staff: bool = False) -> list:
    """One roster per Mon-Sun week overlapping [start, end], oldest week first."""
    weeks = _by_week(db.get_rosters_by_date_range(venue_id, start, end), venue_id)
    out = []
    for monday in sorted(weeks):
        chosen = pick(db, weeks[monday], staff=staff)
        if chosen is not None:
            out.append(chosen)
    return out


def live_shifts(db, venue_id: str, start: date, end: date, *, staff: bool = False) -> list:
    """The shifts dated inside [start, end] from each week's live roster."""
    shifts = []
    for roster in live_rosters(db, venue_id, start, end, staff=staff):
        for s in getattr(roster, "shifts", None) or []:
            sd = getattr(s, "date", None)
            if sd is None or start <= sd <= end:
                shifts.append(s)
    return shifts


def current_roster(db, venue_id: str, any_day: date, *, staff: bool = False):
    """The live roster for the Mon-Sun week containing any_day, or None."""
    monday = week_monday(any_day)
    rosters = db.get_rosters_by_date_range(venue_id, monday, monday + timedelta(days=6))
    return pick(db, _by_week(rosters, venue_id).get(monday, []), staff=staff)


def current_per_week(db, rosters: Iterable) -> list:
    """Manager view of an arbitrary roster list: one per (venue, week)."""
    groups: dict = {}
    for r in rosters:
        groups.setdefault((getattr(r, "venue_id", None), week_monday(r.week_start)), []).append(r)
    chosen = (pick(db, group) for group in groups.values())
    return [r for r in chosen if r is not None]


def supersede_week_drafts(db, venue_id: str, any_day: date, keep_id: str, *,
                          reason: str, actor_id: str = "system",
                          include_published: bool = False) -> list:
    """Mark the week's other rosters superseded (kept as history).

    Unpublished generated drafts always; when include_published (a roster is being
    published over the week) the week's other published roster and its
    imported roster too. A roster whose state can't be read is left alone.
    Returns the ids superseded."""
    monday = week_monday(any_day)
    rosters = db.get_rosters_by_date_range(venue_id, monday, monday + timedelta(days=6))
    done = []
    for r in _by_week(rosters, venue_id).get(monday, []):
        if r.id == keep_id or (is_imported(r) and not include_published):
            continue
        try:
            state = db.get_roster_state(r.id) or "draft"
        except Exception:
            continue
        if state in _REPLACEABLE or (include_published and state == PUBLISHED):
            db.update_roster_state(r.id, SUPERSEDED, reason, actor_id)
            done.append(r.id)
    return done


def supersede_for_import(db, venue_id: str, mondays: Iterable[date]) -> list:
    """After a CSV import: each imported week replaces that week's drafts."""
    done = []
    for monday in mondays:
        rid = f"{IMPORTED_PREFIX}{venue_id}-{week_monday(monday).isoformat()}"
        if db.get_roster(rid) is None:
            continue
        done += supersede_week_drafts(db, venue_id, monday, rid,
                                      reason=f"Replaced by imported roster {rid}")
    return done
