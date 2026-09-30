"""
The AI agent reads Employee.availability through the shared rule
(services/availability_rules), not its own interpretation.

Before: find_available_staff only understood bool/str/dict day values, so the
shapes the staff app actually writes fell through to "available" — [] (off
that day) and time ranges alike. Its role filter matched a `role` attribute
Employee doesn't have, so any role filter returned nobody. The "staff need
availability refresh" insight read a missing `availability_updated_at` and so
counted every active employee as stale.
"""

import asyncio
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

from rosteriq.database import get_db
from rosteriq.ai_agent import AgentContext, generate_insights, _availability_age_days
from rosteriq.models import VenueConfig, Employee, EmploymentType, AwardLevel, State

VENUE = "avail-adoption-ai-venue"
MONDAY = date(2026, 10, 5)
TUESDAY = date(2026, 10, 6)
FRIDAY = date(2026, 10, 9)
assert MONDAY.weekday() == 0 and TUESDAY.weekday() == 1 and FRIDAY.weekday() == 4


def _emp(eid, availability=None, skills=("bar",), updated_at=None):
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    return Employee(
        id=eid, venue_id=VENUE, name=f"Staff {eid}",
        employment_type=EmploymentType.casual, award_level=AwardLevel.level_2,
        state=State.vic, hourly_base_rate=Decimal("30.00"),
        skills=list(skills), availability=availability or {},
        created_at=now - timedelta(days=400), updated_at=updated_at or now,
    )


def _seed(*emps):
    db = get_db()
    db.save_venue(VenueConfig(
        id=VENUE, name="Adoption Venue", tanda_org_id=f"org-{VENUE}",
        state=State.vic, max_labour_pct=30.0, created_at=datetime(2026, 1, 1),
    ))
    db.save_employees(list(emps))
    return db


def _find(**params):
    return asyncio.run(AgentContext(VENUE)._tool_find_available_staff(params))


def _status(result, eid):
    for bucket in ("available", "unavailable"):
        for e in result[bucket]:
            if e["id"] == eid:
                return e["status"]
    return None


# --- find_available_staff: availability ------------------------------------

def test_empty_availability_is_available_any_day():
    _seed(_emp("free", {}))
    for d in (MONDAY, TUESDAY, FRIDAY):
        assert _status(_find(date=d.isoformat()), "free") == "available"
    assert _status(_find(date=FRIDAY.isoformat(), start_time="06:00", end_time="23:00"),
                   "free") == "available"


def test_day_listed_empty_is_unavailable_that_day_only():
    _seed(_emp("nomon", {"monday": []}))
    # [] used to fall through to "available"
    assert _status(_find(date=MONDAY.isoformat()), "nomon") == "unavailable"
    assert _status(_find(date=MONDAY.isoformat(), start_time="10:00", end_time="14:00"),
                   "nomon") == "unavailable"
    # an unlisted day is available all day
    assert _status(_find(date=TUESDAY.isoformat()), "nomon") == "available"
    assert _status(_find(date=TUESDAY.isoformat(), start_time="10:00", end_time="14:00"),
                   "nomon") == "available"


def test_unavailable_hidden_when_include_unavailable_false():
    _seed(_emp("nomon", {"monday": []}), _emp("free", {}))
    r = _find(date=MONDAY.isoformat(), include_unavailable=False)
    assert [e["id"] for e in r["available"]] == ["free"]
    assert r["unavailable"] == []


def test_range_must_contain_the_requested_shift():
    _seed(_emp("eve", {"friday": [{"start": "17:00", "end": "23:00"}]}))
    fri = FRIDAY.isoformat()
    # fits inside the window
    assert _status(_find(date=fri, start_time="18:00", end_time="22:00"), "eve") == "available"
    assert _status(_find(date=fri, start_time="17:00", end_time="23:00"), "eve") == "available"
    # doesn't fit: before the window, or runs past it
    assert _status(_find(date=fri, start_time="12:00", end_time="16:00"), "eve") == "unavailable"
    assert _status(_find(date=fri, start_time="16:59", end_time="20:00"), "eve") == "unavailable"
    assert _status(_find(date=fri, start_time="18:00", end_time="23:01"), "eve") == "unavailable"
    # no end time = a 4-hour shift: 18:00-22:00 fits, 20:00-24:00 doesn't
    assert _status(_find(date=fri, start_time="18:00"), "eve") == "available"
    assert _status(_find(date=fri, start_time="20:00"), "eve") == "unavailable"
    # no time at all: they're available at some point that day
    assert _status(_find(date=fri), "eve") == "available"
    # a different (unlisted) day is unconstrained
    assert _status(_find(date=MONDAY.isoformat(), start_time="06:00", end_time="12:00"),
                   "eve") == "available"


def test_range_to_end_of_day_covers_until_close():
    _seed(_emp("late", {"friday": [{"start": "17:30", "end": "23:59"}]}))
    fri = FRIDAY.isoformat()
    assert _status(_find(date=fri, start_time="17:30", end_time="23:59"), "late") == "available"
    assert _status(_find(date=fri, start_time="21:00"), "late") == "available"      # capped at close
    assert _status(_find(date=fri, start_time="17:29"), "late") == "unavailable"


def test_the_demo_saturday_night_question_has_the_night_people():
    """'Who can cover Saturday night?' — the demo's AI beat. Night and all-day
    staff, not the day-only ones."""
    from rosteriq.services.demo import _DEMO_AVAILABILITY
    _seed(*[_emp(eid, avail) for eid, avail in _DEMO_AVAILABILITY.items()])
    sat = date(2026, 10, 10)
    r = _find(date=sat.isoformat(), start_time="18:00")
    assert sorted(e["id"] for e in r["available"]) == [
        "demo-staff-001", "demo-staff-002", "demo-staff-004", "demo-staff-006"]


# --- find_available_staff: role filter ---------------------------------------

def test_role_filter_matches_skills():
    _seed(
        _emp("barfloor", {}, skills=("bar", "floor")),
        _emp("chef", {}, skills=("kitchen",)),
        _emp("noskills", {}, skills=()),
    )
    r = _find(date=MONDAY.isoformat(), role="floor")
    assert [e["id"] for e in r["available"]] == ["barfloor"]
    assert r["available"][0]["role"] == "bar"          # primary skill, as get_employees
    assert r["available"][0]["skills"] == ["bar", "floor"]
    assert r["role_filter"] == "floor"

    r = _find(date=MONDAY.isoformat(), role="Kitchen")
    assert [e["id"] for e in r["available"]] == ["chef"]

    r = _find(date=MONDAY.isoformat())
    assert {e["id"] for e in r["available"]} == {"barfloor", "chef", "noskills"}
    assert r["role_filter"] == "all"

    r = _find(date=MONDAY.isoformat(), role="security")
    assert r["available"] == [] and r["unavailable"] == []


def test_role_filter_applies_before_availability():
    _seed(
        _emp("bar_off", {"monday": []}, skills=("bar",)),
        _emp("chef_off", {"monday": []}, skills=("kitchen",)),
    )
    r = _find(date=MONDAY.isoformat(), role="bar")
    assert [e["id"] for e in r["unavailable"]] == ["bar_off"]
    assert r["available"] == []


# --- staleness ----------------------------------------------------------------

def _refresh_insight(insights):
    hits = [i for i in insights if "need availability refresh" in i["title"]]
    assert not any(i["title"] == "Connect your systems" for i in insights)
    return hits[0] if hits else None


def test_insight_quiet_when_staff_recently_saved_availability():
    _seed(*[_emp(f"fresh{i}", {}) for i in range(5)])
    insights = asyncio.run(generate_insights(VENUE, max_insights=50))
    assert _refresh_insight(insights) is None


def test_insight_counts_only_staff_older_than_threshold():
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    old = now - timedelta(days=30)
    edge = now - timedelta(days=14, hours=1)   # 14 whole days: not over the threshold
    _seed(
        *[_emp(f"stale{i}", {}, updated_at=old) for i in range(4)],
        _emp("fresh", {}),
        _emp("edge", {}, updated_at=edge),
    )
    hit = _refresh_insight(asyncio.run(generate_insights(VENUE, max_insights=50)))
    assert hit is not None
    assert hit["title"].startswith("4 staff")
    assert "Staff fresh" not in hit["description"]
    assert "Staff edge" not in hit["description"]


def test_insight_needs_three_stale_staff():
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    old = now - timedelta(days=30)
    _seed(_emp("s1", {}, updated_at=old), _emp("s2", {}, updated_at=old),
          *[_emp(f"f{i}", {}) for i in range(3)])
    assert _refresh_insight(asyncio.run(generate_insights(VENUE, max_insights=50))) is None


def test_tool_flags_stale_availability_from_updated_at():
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    _seed(_emp("old", {}, updated_at=now - timedelta(days=20)), _emp("new", {}))
    r = _find(date=MONDAY.isoformat())
    by_id = {e["id"]: e for e in r["available"]}
    assert by_id["old"]["availability_days_old"] == 20
    assert by_id["old"].get("stale_availability") is True
    assert by_id["new"]["availability_days_old"] == 0
    assert "stale_availability" not in by_id["new"]
    assert "Staff old" in r["stale_availability_warning"]
    assert "Staff new" not in r["stale_availability_warning"]


def test_age_handles_aware_and_missing_stamps():
    class _E:
        pass
    e = _E()
    assert _availability_age_days(e) is None
    e.updated_at = datetime.now(timezone.utc) - timedelta(days=3)      # Postgres timestamptz
    assert _availability_age_days(e) == 3
    e.updated_at = (datetime.now(timezone.utc) - timedelta(days=5)).isoformat()
    assert _availability_age_days(e) == 5
    e.updated_at = "not a date"
    assert _availability_age_days(e) is None


def test_model_supplied_times_are_read_strictly():
    from rosteriq.ai_agent import _tool_time
    assert _tool_time("17:30") == "17:30" and _tool_time("5pm") == "17:00"
    assert _tool_time("5:00 PM") == "17:00" and _tool_time("12am") == "00:00" and _tool_time("1730") == "17:30"
    assert _tool_time(None) == "" and _tool_time("") == ""
    assert _tool_time("tonight") is None and _tool_time("25:00") is None and _tool_time("13pm") is None
    _seed(_emp("night", {"saturday": [{"start": "17:00", "end": "23:59"}]}),
          _emp("day", {"saturday": [{"start": "05:00", "end": "12:00"}]}))
    sat = date(2026, 10, 10).isoformat()
    r = _find(date=sat, start_time="5:00 PM")
    assert _status(r, "night") == "available" and _status(r, "day") == "unavailable"
    assert "error" in _find(date=sat, start_time="tonight")
