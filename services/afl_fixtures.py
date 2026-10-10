"""
AFL fixtures from the published fixture, never from a weekday pattern.

Source: Squiggle (https://api.squiggle.com.au/?q=games;year=YYYY), the free
community API that mirrors the AFL's own fixture. Squiggle asks every client
to send a descriptive User-Agent with a way to contact the operator, so the
header names RosterIQ, the public origin and SQUIGGLE_CONTACT when it is set.

One season is fetched at most once per UTC day and kept in kv_store, so every
worker and every AI question that day reuses it. Kick-offs are converted from
Squiggle's unixtime into the venue's own timezone.

For a venue the relevant games are:
  * home_game       — a local team at home at a local ground (for WA: Fremantle
                      or West Coast at Perth/Optus Stadium), the crowd comes
                      through the precinct;
  * watch_at_venue  — a local team playing anywhere else, fans watch it at the
                      pub.

When nothing is published for the period the answer says so plainly (season
over, next fixture not out yet). When Squiggle can't be reached and nothing is
cached the answer is "fixture source unavailable" — a game is never invented.
"""

from __future__ import annotations

import logging
import os
import re
import time as _time
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import httpx

logger = logging.getLogger(__name__)

SQUIGGLE_URL = "https://api.squiggle.com.au/"
SOURCE_NAME = "Squiggle (api.squiggle.com.au)"
FETCH_TIMEOUT_S = 10.0
# After a failed fetch, don't hammer Squiggle on every AI question.
RETRY_AFTER_FAILURE_S = 600

# Local clubs and their home grounds, by venue state. Squiggle venue names.
LOCAL_FOOTY: Dict[str, Dict[str, Any]] = {
    "wa": {"teams": ["Fremantle", "West Coast"], "grounds": ["Perth Stadium", "Optus Stadium"]},
    "sa": {"teams": ["Adelaide", "Port Adelaide"], "grounds": ["Adelaide Oval"]},
    "vic": {"teams": ["Carlton", "Collingwood", "Essendon", "Geelong", "Hawthorn",
                      "Melbourne", "North Melbourne", "Richmond", "St Kilda",
                      "Western Bulldogs"],
            "grounds": ["M.C.G.", "Docklands", "Kardinia Park"]},
    "qld": {"teams": ["Brisbane Lions", "Gold Coast"], "grounds": ["Gabba", "Carrara"]},
    "nsw": {"teams": ["Sydney", "Greater Western Sydney"],
            "grounds": ["S.C.G.", "Sydney Showground"]},
}

# Squiggle's name for the Perth ground is the stadium's non-sponsor name.
_GROUND_DISPLAY = {"Perth Stadium": "Optus Stadium"}

ALL_TEAMS = sorted({t for v in LOCAL_FOOTY.values() for t in v["teams"]})

# What people (and the AI) call the clubs, mapped to Squiggle's names.
_NICKNAMES = {
    "crows": "Adelaide", "lions": "Brisbane Lions", "brisbane": "Brisbane Lions",
    "blues": "Carlton", "pies": "Collingwood", "magpies": "Collingwood",
    "bombers": "Essendon", "dons": "Essendon", "dockers": "Fremantle",
    "freo": "Fremantle", "cats": "Geelong", "suns": "Gold Coast",
    "giants": "Greater Western Sydney", "gws": "Greater Western Sydney",
    "hawks": "Hawthorn", "demons": "Melbourne", "dees": "Melbourne",
    "roos": "North Melbourne", "kangaroos": "North Melbourne", "north": "North Melbourne",
    "power": "Port Adelaide", "port": "Port Adelaide", "tigers": "Richmond",
    "saints": "St Kilda", "swans": "Sydney", "eagles": "West Coast",
    "dogs": "Western Bulldogs", "bulldogs": "Western Bulldogs", "doggies": "Western Bulldogs",
}
_PHRASES = {**{t.lower(): t for t in ALL_TEAMS},
            "western sydney": "Greater Western Sydney"}


def resolve_teams(text: str) -> set:
    """The Squiggle club names a free-text team filter refers to: formal names
    ("West Coast Eagles" -> West Coast) and nicknames (Freo, Pies, Roos).
    Longer names win, so "North Melbourne" is never also Melbourne."""
    words = re.sub(r"[^a-z0-9]+", " ", (text or "").lower()).split()
    rest = f" {' '.join(words)} "
    found = set()
    for phrase in sorted(_PHRASES, key=len, reverse=True):
        key = f" {phrase} "
        if key in rest:
            found.add(_PHRASES[phrase])
            rest = rest.replace(key, " ")
    found |= {_NICKNAMES[w] for w in rest.split() if w in _NICKNAMES}
    return found

_KEEP = ("id", "round", "roundname", "hteam", "ateam", "venue", "unixtime",
         "date", "tz", "is_final", "is_grand_final", "complete")

_last_failure: Dict[int, float] = {}


def _user_agent() -> str:
    try:
        from rosteriq import __version__ as version
    except Exception:  # pragma: no cover
        version = "1"
    origin = (os.environ.get("PUBLIC_ORIGIN")
              or "https://rosteriq-production-6aaf.up.railway.app").rstrip("/")
    contact = (os.environ.get("SQUIGGLE_CONTACT") or "").strip()
    parts = [f"+{origin}"] + ([contact] if contact else [])
    return f"RosterIQ/{version} (hospitality rostering; {'; '.join(parts)})"


def _http_client() -> httpx.AsyncClient:
    return httpx.AsyncClient(timeout=FETCH_TIMEOUT_S,
                             headers={"User-Agent": _user_agent()})


def _cache_key(year: int) -> str:
    return f"afl:squiggle:games:{year}"


def _trim(game: dict) -> dict:
    return {k: game.get(k) for k in _KEEP}


async def _fetch_year(year: int) -> List[dict]:
    """One season from Squiggle. Raises on any transport/HTTP/shape problem."""
    from rosteriq.services.events import integration
    started = _time.monotonic()
    try:
        async with _http_client() as client:
            resp = await client.get(f"{SQUIGGLE_URL}?q=games;year={int(year)}")
            resp.raise_for_status()
            body = resp.json()
        games = body.get("games") if isinstance(body, dict) else None
        if not isinstance(games, list):
            raise ValueError("unexpected Squiggle response shape")
        integration("squiggle", "games", outcome="ok",
                    duration_ms=(_time.monotonic() - started) * 1000,
                    year=year, games=len(games))
        return [_trim(g) for g in games if isinstance(g, dict)]
    except Exception as e:
        integration("squiggle", "games", outcome="failed",
                    duration_ms=(_time.monotonic() - started) * 1000,
                    year=year, error=type(e).__name__)
        raise


async def season_games(year: int, db=None) -> Tuple[Optional[List[dict]], Optional[str]]:
    """(games, fetched_on) for a season. Cached per UTC day in kv_store; a
    stale cached copy is used when today's fetch fails. (None, None) when
    nothing could be fetched and nothing is cached."""
    if db is None:
        from rosteriq.database import get_db
        db = get_db()
    today = datetime.now(timezone.utc).date().isoformat()
    cached = None
    try:
        cached = db.load_from_key(_cache_key(year))
    except Exception as e:  # noqa: BLE001 — a cache read must never break the answer
        logger.debug("AFL cache read failed: %s", e)
    if isinstance(cached, dict) and cached.get("fetched_on") == today \
            and isinstance(cached.get("games"), list):
        return cached["games"], cached["fetched_on"]

    failed_at = _last_failure.get(year)
    if failed_at is None or _time.monotonic() - failed_at > RETRY_AFTER_FAILURE_S:
        try:
            games = await _fetch_year(year)
            _last_failure.pop(year, None)
            try:
                db.save_to_key(_cache_key(year), {"fetched_on": today, "games": games})
            except Exception as e:  # noqa: BLE001
                logger.warning("AFL cache write failed: %s", e)
            return games, today
        except Exception as e:  # noqa: BLE001
            _last_failure[year] = _time.monotonic()
            logger.warning("Squiggle fixture fetch failed for %s: %s", year, e)

    if isinstance(cached, dict) and isinstance(cached.get("games"), list):
        return cached["games"], cached.get("fetched_on")
    return None, None


def _kickoff(game: dict, tz) -> Optional[datetime]:
    ts = game.get("unixtime")
    try:
        if ts:
            return datetime.fromtimestamp(int(ts), tz)
    except (TypeError, ValueError, OverflowError, OSError):
        pass
    raw, off = game.get("date"), game.get("tz")
    if raw and off:
        try:
            return datetime.fromisoformat(f"{str(raw).replace(' ', 'T')}{off}").astimezone(tz)
        except ValueError:
            return None
    return None


def _venue_context(venue_id: str, db) -> Tuple[str, Any, Optional[str]]:
    """(timezone name, ZoneInfo, state code) for the venue."""
    from zoneinfo import ZoneInfo
    from rosteriq.services import clock
    tz_name = clock.venue_timezone(venue_id, db)
    try:
        tz = ZoneInfo(tz_name)
    except Exception:
        tz_name, tz = clock.DEFAULT_TZ, ZoneInfo(clock.DEFAULT_TZ)
    state = None
    try:
        venue = db.get_venue(venue_id)
        raw = getattr(venue, "state", None) if venue is not None else None
        state = str(getattr(raw, "value", raw) or "").strip().lower() or None
    except Exception:  # noqa: BLE001
        state = None
    return tz_name, tz, state


def _fmt_day(d: date) -> str:
    return f"{d.strftime('%a')} {d.day} {d.strftime('%b')}"


def _fmt_span(start: date, end: date) -> str:
    if start == end:
        return f"on {_fmt_day(start)}"
    return f"between {_fmt_day(start)} and {_fmt_day(end)}"


async def upcoming_games(venue_id: str, days_ahead: int = 14,
                         team_filter: str = "", db=None,
                         now: Optional[datetime] = None) -> Dict[str, Any]:
    """The venue's relevant AFL games from today through days_ahead days."""
    if db is None:
        from rosteriq.database import get_db
        db = get_db()
    try:
        days_ahead = max(1, min(int(days_ahead or 14), 60))
    except (TypeError, ValueError):
        days_ahead = 14
    tz_name, tz, state = _venue_context(venue_id, db)
    now = (now or datetime.now(timezone.utc)).astimezone(tz)
    start = now.date()
    end = start + timedelta(days=days_ahead - 1)
    local = LOCAL_FOOTY.get(state or "")

    base: Dict[str, Any] = {
        "source": SOURCE_NAME,
        "venue_timezone": tz_name,
        "venue_state": (state or "").upper() or None,
        "period": {"from": start.isoformat(), "to": end.isoformat(),
                   "days": days_ahead},
        "games": [],
        "count": 0,
    }
    if not local:
        base.update(status="no_local_team",
                    note=f"No AFL club is based in {base['venue_state'] or 'this venue’s state'}, "
                         "so there are no local home games to plan around.")
        return base
    base["local_teams"] = list(local["teams"])
    base["home_ground"] = _GROUND_DISPLAY.get(local["grounds"][0], local["grounds"][0])
    filter_text = (team_filter or "").strip()
    wanted = resolve_teams(filter_text)
    if filter_text and not wanted:
        base.update(status="team_not_recognised",
                    note=f"'{filter_text[:40]}' isn't an AFL club RosterIQ recognises, so "
                         "nothing was looked up for it. Ask again with the club's name "
                         f"(the local clubs are {' and '.join(local['teams'])}).")
        return base

    years = sorted({start.year, end.year})
    season: Dict[int, Optional[List[dict]]] = {}
    fetched: Dict[int, Optional[str]] = {}
    for y in years:
        season[y], fetched[y] = await season_games(y, db)
    if season[start.year] is None:
        base.update(status="unavailable",
                    note="AFL fixture source unavailable right now — can't confirm "
                         "any games. Don't assume there is or isn't a game; check "
                         "the AFL fixture directly.")
        return base
    stale = [y for y in years if fetched.get(y) and fetched[y] != datetime.now(timezone.utc).date().isoformat()]
    if stale:
        base["fixture_as_of"] = min(fetched[y] for y in stale)

    teams = set(local["teams"])
    grounds = set(local["grounds"])
    any_in_range = False
    games = []
    for y in years:
        for g in season.get(y) or []:
            ko = _kickoff(g, tz)
            if ko is None or not (start <= ko.date() <= end):
                continue
            any_in_range = True
            home, away = g.get("hteam") or "", g.get("ateam") or ""
            if home not in teams and away not in teams:
                continue
            if wanted and home not in wanted and away not in wanted:
                continue
            ground = g.get("venue") or ""
            if home in teams and ground in grounds:
                signal = "home_game"
                note = (f"Home game at {_GROUND_DISPLAY.get(ground, ground)} — "
                        "crowd through the precinct before and after the game.")
            else:
                signal = "watch_at_venue"
                note = "Local team playing away — fans watch it at the pub."
            games.append({
                "date": ko.date().isoformat(),
                "day": ko.strftime("%A"),
                "kickoff": ko.strftime("%H:%M"),
                "matchup": f"{home} v {away}",
                "home_team": home,
                "away_team": away,
                "ground": _GROUND_DISPLAY.get(ground, ground),
                "round": g.get("roundname") or (f"Round {g['round']}" if g.get("round") is not None else None),
                "is_final": bool(g.get("is_final")),
                "is_grand_final": bool(g.get("is_grand_final")),
                "signal": signal,
                "note": note,
                "_ko": ko,
            })
    games.sort(key=lambda x: x["_ko"])
    for g in games:
        g.pop("_ko", None)
    base["games"] = games
    base["count"] = len(games)

    if games:
        base["status"] = "ok"
        base["note"] = ("Real fixture from Squiggle; kick-off times are venue local time. "
                        "Staffing impact is the manager's call — no multiplier is applied.")
        return base

    # A quiet window inside the season (midweek, the pre-finals bye) is not the
    # off-season: games of this season lie on both sides of it.
    this_season = [k for k in (_kickoff(g, tz) for g in season.get(start.year) or []) if k]
    in_season = (any(k.date() < start for k in this_season)
                 and any(k.date() > end for k in this_season))
    if any_in_range or in_season:
        if wanted and not wanted <= teams:
            who = f"{' or '.join(sorted(wanted))} v {' or '.join(local['teams'])}"
        else:
            who = " or ".join(sorted(wanted) if wanted else local["teams"])
        nxt = None
        for y in years:
            for g in season.get(y) or []:
                ko = _kickoff(g, tz)
                home, away = g.get("hteam") or "", g.get("ateam") or ""
                if ko is None or ko.date() <= end or (home not in teams and away not in teams):
                    continue
                if wanted and home not in wanted and away not in wanted:
                    continue
                if nxt is None or ko < nxt[0]:
                    nxt = (ko, f"{home} v {away}")
        quiet = f"No {who} games" if any_in_range else "No AFL games"
        base["status"] = "ok"
        base["note"] = f"{quiet} {_fmt_span(start, end)}"
        if nxt:
            ko, matchup = nxt
            base["next_local_game"] = {"date": ko.date().isoformat(), "kickoff": ko.strftime("%H:%M"),
                                       "matchup": matchup}
            base["note"] += (f"; next local game is {_fmt_day(ko.date())} "
                             f"({matchup}, {ko.strftime('%H:%M')}).")
        else:
            base["note"] += f"; no more {who} games are in the {start.year} fixture."
        return base

    # Nothing scheduled in the window and none on one side of it: off-season.
    later = []
    for y in sorted({start.year, start.year + 1} | set(years)):
        if y not in season:
            season[y], fetched[y] = await season_games(y, db)
        for g in season.get(y) or []:
            ko = _kickoff(g, tz)
            if ko is not None and ko.date() > end:
                later.append(ko)
    base["status"] = "off_season"
    if later:
        first = min(later)
        base["next_season_starts"] = first.date().isoformat()
        base["note"] = f"AFL off-season — the {first.year} season starts {_fmt_day(first.date())}."
    else:
        played = [_kickoff(g, tz) for g in season.get(start.year) or []]
        next_year = start.year + 1 if any(k and k.date() < start for k in played) else start.year
        if season.get(next_year) is None and next_year != start.year:
            base["note"] = (f"AFL off-season — the {start.year} season is over and the "
                            f"{next_year} fixture couldn't be checked right now.")
        else:
            base["note"] = (f"AFL off-season — no games until the {next_year} "
                            "fixture is published.")
    return base
