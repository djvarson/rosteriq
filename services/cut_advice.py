"""
Cut / hold advice for the duty manager — deterministic, in venue-local time.

The On-Shift "Cut / hold" card (GET /api/ops/cut-advice) and the AI's
get_cut_recommendation tool both call advise(), so they give the same call.

* now is the venue's clock (services/clock.venue_now), never the host's
* on shift = live-roster shifts (services/current_roster) whose span covers
  now, including last night's shift still running past midnight
* each hourly person's cost for this hour = their recorded rate priced for
  this hour by the cost calculator (casual loading, weekend/holiday and
  evening rates, super). Salaried people are not in the hourly figure.
* sales per hour = the takings the manager typed for the last hour (inc GST),
  else today's recorded sales averaged over the hours traded so far (whole
  venue only). Labour % is against net (ex-GST) sales and the venue's
  recorded max_labour_pct.
* never below the area minimums for today (services/venue_areas); an area
  with no minimum recorded keeps its last person
* hold when a function-sized booking starts within HOLD_WINDOW, or a local
  AFL team's game is about to start or is being played
* candidates: casuals on shift only — never a salaried, part-time,
  full-time, manager or supervisor shift — finishing latest first, by name
"""

from __future__ import annotations

import logging
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from typing import Iterable, List, Optional

from rosteriq.models import Shift, ShiftStatus, normalise_area
from rosteriq.services.clock import venue_now
from rosteriq.services.current_roster import live_shifts
from rosteriq.services.sos import area_choices, employment_type, is_salaried, shift_span, viewer_sections
from rosteriq.services.venue_areas import area_names, find_area, is_closed, min_staff_for, trading_window

logger = logging.getLogger(__name__)

HOLD_WINDOW = timedelta(hours=2, minutes=30)
# A game that kicked off this long ago is still on (or just finished) — the
# pub is full of people watching it.
AFL_IN_PLAY = timedelta(hours=3)
# A single booking this size is a function, not a table.
FUNCTION_GUESTS = 20
GST_RATE = Decimal("0.10")
_SUPERVISORY = ("duty manager", "manager", "supervisor")
_NOT_WORKING = ("cancelled", "no_show")


def _money(value) -> Optional[float]:
    return None if value is None else round(float(value), 2)


def _probe_cost(emp, state, start: datetime, end: datetime) -> Decimal:
    """What [start, end) of this person's time costs, priced by the cost
    calculator exactly as a rostered shift would be."""
    from rosteriq.cost_calculator import calculate_shift_cost_breakdown
    probe = Shift(id="cut-advice-probe", employee_id=str(emp.id), date=start.date(),
                  start_time=start.time().replace(second=0, microsecond=0),
                  end_time=end.time().replace(second=0, microsecond=0),
                  break_minutes=0, status=ShiftStatus.scheduled, role="")
    return calculate_shift_cost_breakdown(emp, probe, state).total_cost


def _supervisory(text) -> bool:
    text = normalise_area(text)
    return any(word in text for word in _SUPERVISORY)


def expand_area_names(venue, areas: Iterable[str]) -> list:
    """The venue's own area names for loosely named areas: 'bar' covers
    'front bar' and 'sports bar'. An exact area name stays as it is."""
    names = area_names(venue)
    out: list = []
    for a in areas or []:
        a = normalise_area(a)
        if not a:
            continue
        matches = [a] if a in names else ([n for n in names if a in n] or [a])
        out += [m for m in matches if m not in out]
    return out


def _trading_day(venue, now: datetime) -> date:
    yesterday = now.date() - timedelta(days=1)
    window = trading_window(venue, yesterday)
    if window and window[0] <= now < window[1]:
        return yesterday
    return now.date()


def recorded_settings(venue, areas: List[str], day: date) -> dict:
    """Labour target and minimums the venue recorded, with plain notes for
    anything missing. Nothing is assumed in place of a missing setting."""
    notes = []
    target = getattr(venue, "max_labour_pct", None)
    try:
        target = float(target) if target is not None else None
    except (TypeError, ValueError):
        target = None
    if target is None:
        notes.append("No labour target is recorded for this venue.")
    elif areas:
        notes.append(f"No per-area labour target is recorded; using the venue's "
                     f"overall target of {target:g}%.")
    mins = min_staff_for(venue, day)
    closed = is_closed(venue, day)
    if closed:
        notes.append(f"{day:%A} is recorded as a closed day, so no area minimums apply; "
                     "never cutting the last person on.")
    if areas:
        for a in areas:
            if not mins.get(a) and not closed:
                notes.append(f"No minimum staffing is recorded for {a}; "
                             "never cutting the last person on.")
        skeleton = sum(mins.get(a, 0) for a in areas) or None
    else:
        skeleton = sum(mins.values()) or None
    return {"target_pct": target, "minimums": mins, "skeleton": skeleton, "notes": notes}


def on_shift_now(db, venue_id: str, now: datetime) -> list:
    """[(shift, start, end)] of live-roster shifts whose span covers now."""
    out = []
    for s in live_shifts(db, venue_id, now.date() - timedelta(days=1), now.date()):
        if str(getattr(s.status, "value", s.status)) in _NOT_WORKING:
            continue
        start, end = shift_span(s)
        if start <= now < end:
            out.append((s, start, end))
    return out


def _sales_per_hour(db, venue, venue_id: str, now: datetime, takings, whole_venue: bool,
                    on_shift: list):
    """(net sales per hour, where it came from) or (None, why not)."""
    if takings is not None:
        gross = Decimal(str(takings))
        net = gross / (1 + GST_RATE)
        return net, f"the last hour's takings you entered (${gross:,.0f} inc GST)"
    if not whole_venue:
        return None, "Enter this area's takings for the last hour to get a cut call."
    try:
        rows = db.list_dish_sales(venue_id, now.date(), now.date()) or []
    except Exception as e:  # noqa: BLE001
        logger.warning("Cut advice sales lookup failed for %s: %s", venue_id, e)
        rows = []
    gross = sum(Decimal(str(r.get("revenue_inc_gst") or 0)) for r in rows)
    if gross <= 0:
        return None, "No sales recorded yet today — enter the last hour's takings to get a cut call."
    window = trading_window(venue, now.date())
    opened = window[0] if window and window[0] <= now else None
    if opened is None:
        starts = [start for _, start, _ in on_shift if start.date() == now.date()]
        opened = min(starts) if starts else now - timedelta(hours=1)
    hours = max(Decimal(str((now - opened).total_seconds() / 3600)), Decimal("1"))
    net = gross / (1 + GST_RATE) / hours
    return net, f"today's recorded sales averaged since {opened:%H:%M} (${gross:,.0f} inc GST)"


def _booking_holds(db, venue_id: str, now: datetime) -> tuple:
    """(hold reasons, information) from stored bookings starting soon."""
    holds, info = [], []
    try:
        bookings = db.get_reservations(venue_id, now.date(), now.date()) or []
    except Exception as e:  # noqa: BLE001
        logger.warning("Cut advice bookings lookup failed for %s: %s", venue_id, e)
        return holds, info
    soon_covers = 0
    for b in bookings:
        raw = str(getattr(b, "time", None) or "").strip()[:5]
        try:
            hh, mm = raw.split(":")
            at = datetime.combine(now.date(), time(int(hh), int(mm)))
        except (ValueError, TypeError):
            continue
        if not (now <= at <= now + HOLD_WINDOW):
            continue
        try:
            guests = int(float(getattr(b, "party_size", 0) or 0))
        except (TypeError, ValueError):
            guests = 0
        if guests >= FUNCTION_GUESTS:
            holds.append(f"A booking for {guests} starts at {at:%H:%M}")
        else:
            soon_covers += guests
    if soon_covers:
        info.append(f"{soon_covers} covers booked before {(now + HOLD_WINDOW):%H:%M}")
    return holds, info


async def _afl_games(venue_id: str, db, aware_now: Optional[datetime]) -> dict:
    from rosteriq.services.afl_fixtures import upcoming_games
    return await upcoming_games(venue_id, days_ahead=2, db=db, now=aware_now)


async def _afl_holds(venue_id: str, db, now: datetime, aware_now: Optional[datetime]) -> tuple:
    try:
        fixture = await _afl_games(venue_id, db, aware_now)
    except Exception as e:  # noqa: BLE001
        logger.warning("Cut advice AFL lookup failed for %s: %s", venue_id, e)
        return [], ["The AFL fixture couldn't be checked."]
    if fixture.get("status") == "unavailable":
        return [], ["The AFL fixture couldn't be checked."]
    holds = []
    for g in fixture.get("games") or []:
        try:
            ko = datetime.combine(date.fromisoformat(g["date"]),
                                  time.fromisoformat(g["kickoff"]))
        except (KeyError, TypeError, ValueError):
            continue
        if ko - HOLD_WINDOW <= now <= ko + AFL_IN_PLAY:
            when = "kicked off" if ko <= now else "kicks off"
            holds.append(f"{g.get('matchup')} {when} at {ko:%H:%M} "
                         f"({'home game' if g.get('signal') == 'home_game' else 'on the screens'})")
    return holds, []


async def advise(db, venue_id: str, *, areas: Optional[Iterable[str]] = None,
                 takings=None, covers: Optional[int] = None,
                 now: Optional[datetime] = None) -> dict:
    """The cut / hold call for now. areas narrows it to those areas (a
    department manager's sections, or the one picked on the card)."""
    venue = db.get_venue(venue_id)
    if venue is None:
        raise LookupError("Venue not found")
    now = now or venue_now(venue_id, db)
    aware_now = now if now.tzinfo is not None else None
    now = now.replace(tzinfo=None, second=0, microsecond=0)
    wanted = [normalise_area(a) for a in (areas or []) if normalise_area(a)]
    day = _trading_day(venue, now)
    settings = recorded_settings(venue, wanted, day)
    notes = list(settings["notes"])
    state = getattr(venue, "state", None)

    employees = {str(e.id): e for e in (db.get_employees(venue_id) or [])}
    everyone = on_shift_now(db, venue_id, now)

    def area_of(shift) -> str:
        return normalise_area(shift.role) or "no area"

    scoped = [(s, a, b) for s, a, b in everyone if not wanted or area_of(s) in wanted]
    counts: dict = {}
    for s, _, _ in scoped:
        counts[area_of(s)] = counts.get(area_of(s), 0) + 1

    people = []
    labour = Decimal("0")
    salaried_on = unknown = 0
    for s, start, end in scoped:
        emp = employees.get(str(s.employee_id))
        person = {
            "employee_id": str(s.employee_id),
            "name": getattr(emp, "name", None) or "Unknown staff member",
            "area": area_of(s),
            "start": f"{s.start_time:%H:%M}",
            "end": f"{s.end_time:%H:%M}",
            "employment_type": employment_type(emp) if emp is not None else None,
            "hourly_cost": None,
        }
        if emp is None:
            unknown += 1
        elif is_salaried(emp):
            salaried_on += 1
            person["salaried"] = True
        else:
            cost = _probe_cost(emp, state, now, now + timedelta(hours=1))
            person["hourly_cost"] = _money(cost)
            labour += cost
        person["_shift"], person["_end"], person["_emp"] = s, end, emp
        people.append(person)
    if salaried_on:
        notes.append(f"{salaried_on} salaried {'person' if salaried_on == 1 else 'people'} on "
                     "shift — not in the hourly labour figure.")
    if unknown:
        notes.append(f"{unknown} shift(s) belong to staff not on file — not costed.")

    mins = settings["minimums"]

    def keep_at_least(area: str) -> int:
        return max(int(mins.get(area, 0) or 0), 1)

    pool = []
    for p in people:
        emp = p["_emp"]
        if emp is None or is_salaried(emp) or employment_type(emp) != "casual":
            continue
        area = find_area(venue, p["area"])
        if _supervisory(p["area"]) or (area and area["requires_skill"]) \
                or any(_supervisory(k) for k in (getattr(emp, "skills", None) or [])):
            continue
        if p["_end"] <= now + timedelta(minutes=15):
            continue
        pool.append(p)
    pool.sort(key=lambda p: (-p["_end"].timestamp(), -(p["hourly_cost"] or 0), p["name"].lower()))

    sales, sales_source = _sales_per_hour(db, venue, venue_id, now, takings, not wanted, everyone)
    target = settings["target_pct"]
    labour_pct = float(labour / sales * 100) if sales else None
    labour_said = (f"Labour is {labour_pct:.0f}% of sales this hour" if labour_pct is not None
                   else "No takings in the last hour")

    reasons: List[str] = []
    holds: List[str] = []
    info: List[str] = []
    picked: list = []

    def pick(limit_dollars: Optional[Decimal]) -> list:
        left = dict(counts)
        chosen, running = [], labour
        for p in pool:
            if limit_dollars is not None and running <= limit_dollars:
                break
            if left.get(p["area"], 0) - 1 < keep_at_least(p["area"]):
                continue
            chosen.append(p)
            left[p["area"]] -= 1
            running -= Decimal(str(p["hourly_cost"] or 0))
            if limit_dollars is None and len(chosen) >= 3:
                break
        return chosen

    if not scoped:
        recommendation = "no_one_on"
        where = " in " + ", ".join(wanted) if wanted else ""
        headline = f"Nobody is on shift{where} right now."
    elif sales is None:
        recommendation = "need_sales"
        headline = sales_source
        picked = pick(None)
    elif target is None:
        recommendation = "need_target"
        headline = "Set the venue's labour target in Settings to get a cut call."
        reasons.append(labour_said + ".")
    elif labour_pct is not None and labour_pct <= target:
        recommendation = "hold"
        headline = f"Hold — labour is {labour_pct:.0f}% of sales, within your {target:g}% target."
        reasons.append(labour_said + f" — within your {target:g}% target.")
    else:
        reasons.append(labour_said + f" — over your {target:g}% target.")
        picked = pick(sales * Decimal(str(target)) / 100)
        if not picked:
            recommendation = "hold"
            headline = ("Hold — labour is over target, but there's no casual you can send "
                        "home without dropping below an area minimum.")
        else:
            holds, info = _booking_holds(db, venue_id, now)
            afl_holds, afl_info = await _afl_holds(venue_id, db, now, aware_now)
            holds += afl_holds
            info += afl_info
            if holds:
                recommendation = "hold"
                headline = "Hold — " + holds[0] + "."
                reasons += holds
            else:
                recommendation = "cut"
                headline = f"Cut {len(picked)}: " + ", ".join(
                    f"{p['name']} (till {p['end']})" for p in picked)

    candidates = []
    for p in picked:
        emp = p["_emp"]
        saving = _probe_cost(emp, state, now, p["_end"]) if p["_end"] > now else Decimal("0")
        candidates.append({"employee_id": p["employee_id"], "name": p["name"], "area": p["area"],
                           "start": p["start"], "end": p["end"], "hourly_cost": p["hourly_cost"],
                           "saving_if_sent_now": _money(saving)})
    if recommendation == "cut":
        notes.append("Anyone sent home early is still paid their minimum engagement — "
                     "advisor to confirm.")

    on_shift = [{k: v for k, v in p.items() if not k.startswith("_")} for p in people]
    on_shift.sort(key=lambda p: (p["area"], p["end"], p["name"].lower()))
    area_mins = {a: {"on": counts.get(a, 0), "minimum": int(mins.get(a, 0) or 0)}
                 for a in sorted(set(counts) | {a for a in mins if not wanted or a in wanted})}
    return {
        "venue_id": venue_id,
        "date": now.date().isoformat(),
        "time": now.strftime("%H:%M"),
        "areas": wanted,
        "recommendation": recommendation,
        "headline": headline,
        "reasons": reasons,
        "holds": holds,
        "information": info,
        "candidates": candidates,
        "savings_estimate": _money(sum((Decimal(str(c["saving_if_sent_now"] or 0)) for c in candidates),
                                       Decimal("0"))) if candidates else None,
        "on_shift": on_shift,
        "on_shift_count": len(on_shift),
        "area_minimums": area_mins,
        "labour_per_hour": _money(labour),
        "sales_per_hour_ex_gst": _money(sales),
        "sales_source": sales_source if sales is not None else None,
        "labour_pct": round(labour_pct, 1) if labour_pct is not None else None,
        "labour_target_pct": target,
        "skeleton_minimum": settings["skeleton"],
        "covers": covers,
        "settings_notes": settings["notes"],
        "notes": notes,
        "area_choices": area_choices(venue, viewer_sections(venue_id)),
    }


def record_shown(venue_id: str, inputs: dict, result: dict, source: str,
                 user_id: Optional[str] = None) -> None:
    """Keep the advice a manager was shown, so "on Tuesday it said cut Sam"
    can be checked later. Never raises."""
    from rosteriq.services.events import record_event
    record_event("audit", "ops.cut_advice", venue_id=venue_id, resource_type="cut_advice",
                 details={"source": source, "inputs": inputs, "shown": audit_summary(result)},
                 user_id=user_id)


def audit_summary(result: dict) -> dict:
    return {
        "time": result.get("time"),
        "areas": result.get("areas") or ["all"],
        "recommendation": result.get("recommendation"),
        "headline": result.get("headline"),
        "labour_pct": result.get("labour_pct"),
        "target_pct": result.get("labour_target_pct"),
        "labour_per_hour": result.get("labour_per_hour"),
        "sales_per_hour_ex_gst": result.get("sales_per_hour_ex_gst"),
        "candidates": [c["name"] for c in result.get("candidates", [])],
        "holds": result.get("holds", [])[:5],
        "on_shift": result.get("on_shift_count"),
    }
