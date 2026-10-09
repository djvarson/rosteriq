"""
The Wembley pilot: areas drive the roster, the break rule, late closes and the
staffing check.

Pins:
* staff skills are normalised (case, spacing, Deputy "A, B" blobs) and every
  area/role comparison ignores case, so Deputy-cased areas produce a roster
* the break table separates the unpaid meal break (deducted) from paid rest
  breaks (never deducted); a standard 30-minute meal break publishes
* the hourly staffing walk is midnight-aware and always terminates (an 11:30pm
  close once froze the server)
* staffing is checked per area across every trading hour, empty hours too,
  as plain-words warnings the publish response carries
* the budget trim protects every hour through to close
* auto-fix extends a late short shift past midnight instead of a 500
"""

import signal
import uuid
from contextlib import contextmanager
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient

from rosteriq import award_rules
from rosteriq.api import app
from rosteriq.database import get_db
from rosteriq.models import (
    AwardLevel, DemandForecast, Employee, EmploymentType, Roster, Shift,
    ShiftStatus, State, VenueConfig,
)
from rosteriq.roster_optimiser import (
    _best_shift_template, _employee_can_fill_role, apply_budget_cap,
    generate_weekly_roster,
)
from rosteriq.services.conflict_detector import (
    ConflictDetector, ConflictSeverity, ConflictType,
)


MON = date(2026, 10, 12)
TUE = MON + timedelta(days=1)
SAT = MON + timedelta(days=5)
PW = "Passw0rd!234"


@contextmanager
def _deadline(seconds):
    """Fail (instead of hanging the suite) if the block runs too long."""
    def _boom(signum, frame):
        raise TimeoutError(f"took longer than {seconds}s")
    previous = signal.signal(signal.SIGALRM, _boom)
    signal.setitimer(signal.ITIMER_REAL, seconds)
    try:
        yield
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous)


def _emp(eid, skills=None, etype=EmploymentType.casual, rate="32.60", venue_id="wem"):
    return Employee(
        id=eid, venue_id=venue_id, name=eid.title(), employment_type=etype,
        award_level=AwardLevel.level_2, state=State.wa,
        hourly_base_rate=Decimal(rate), skills=skills or [],
        created_at=datetime(2026, 10, 1), updated_at=datetime(2026, 10, 1))


def _shift(sid, eid, day, start, end, role="front bar", brk=0):
    return Shift(id=sid, employee_id=eid, date=day, start_time=start,
                 end_time=end, break_minutes=brk, status=ShiftStatus.scheduled,
                 role=role)


def _roster(shifts, rid="wem-r", venue_id="wem", week_start=MON):
    return Roster(id=rid, venue_id=venue_id, week_start=week_start,
                  week_end=week_start + timedelta(days=6), shifts=list(shifts),
                  created_at=datetime(2026, 10, 9))


def _venue(min_staff, vid="wem"):
    return VenueConfig(id=vid, name="The Wembley", tanda_org_id="", state=State.wa,
                       timezone="Australia/Perth", min_staff=min_staff,
                       max_labour_pct=30, created_at=datetime(2026, 10, 1))


def _forecasts(day, hours, covers=40.0, venue_id="wem"):
    return [DemandForecast(id=str(uuid.uuid4()), venue_id=venue_id, date=day, hour=h,
                           predicted_covers=covers, confidence=0.5,
                           model_version="test") for h in hours]


def _staffing(conflicts):
    return sorted(c.message for c in conflicts
                  if c.conflict_type == ConflictType.UNDERSTAFFED_HOUR)


def _owner_client():
    c = TestClient(app)
    email = f"wem_{uuid.uuid4().hex[:8]}@x.com"
    c.post("/api/auth/register", json={"email": email, "password": PW, "name": "Ben"})
    db = get_db()
    rec = db.get_user_by_email(email)
    rec["role"] = "owner"
    db.save_user(rec)
    tok = c.post("/api/auth/login", json={"email": email, "password": PW}).json()["access_token"]
    return c, {"Authorization": f"Bearer {tok}"}


def _api_venue(c, h, min_staff=None):
    vid = f"wem-{uuid.uuid4().hex[:6]}"
    r = c.post("/venues", json={
        "id": vid, "name": "The Wembley", "state": "wa", "max_labour_pct": 30,
        "tanda_org_id": "", "timezone": "Australia/Perth",
        "min_staff": min_staff or {}, "created_at": "2026-10-01T00:00:00"}, headers=h)
    assert r.status_code in (200, 201), r.text
    return vid


# ---------------------------------------------------------------------------
# skills: normalised on the model, compared without case
# ---------------------------------------------------------------------------

def test_employee_skills_are_normalised():
    e = _emp("a", ["Front Bar, Sports Bar", " Kitchen ", "kitchen", "Bottle  Shop"])
    assert e.skills == ["front bar", "sports bar", "kitchen", "bottle shop"]

    e.skills = ["RESTAURANT"]                       # assignment is validated too
    assert e.skills == ["restaurant"]

    single = Employee(
        id="b", name="B", employment_type="casual", award_level="level_2", state="wa",
        hourly_base_rate=Decimal("30"), skills="Bar; Cellar | Bottle shop / Floor",
        created_at=datetime(2026, 10, 1), updated_at=datetime(2026, 10, 1))
    assert single.skills == ["bar", "cellar", "bottle shop", "floor"]

    single.skills = '["Front Bar", "Kitchen"]'      # a JSON text column read raw
    assert single.skills == ["front bar", "kitchen"]
    single.skills = None
    assert single.skills == []


def test_area_role_matching_ignores_case():
    assert _employee_can_fill_role(_emp("a", ["front bar"]), "Front Bar")
    assert not _employee_can_fill_role(_emp("a", ["front bar"]), "Kitchen")
    # data that skipped validation (an older stored record) still matches
    raw = SimpleNamespace(skills=["Front Bar, Sports Bar"])
    assert _employee_can_fill_role(raw, "sports bar")
    assert not _employee_can_fill_role(raw, "kitchen")


def test_deputy_cased_areas_generate_a_roster():
    venue = _venue({"Restaurant": 1, "Front Bar": 1, "Kitchen": 1})
    staff = (
        [_emp(f"rest{i}", ["Restaurant"]) for i in range(3)]
        + [_emp(f"bar{i}", ["Front Bar, Sports Bar"]) for i in range(3)]
        + [_emp(f"chef{i}", ["Kitchen"]) for i in range(3)]
    )
    roster = generate_weekly_roster(MON, _forecasts(MON, range(11, 22), 30.0), staff, venue)
    assert roster.shifts, "Deputy-cased areas produced an empty roster"
    assert {s.role for s in roster.shifts} == {"restaurant", "front bar", "kitchen"}
    by_id = {e.id: e for e in staff}
    for s in roster.shifts:
        assert s.role in by_id[s.employee_id].skills, (s.employee_id, s.role)


def test_imported_area_case_is_not_a_skill_mismatch():
    emp = _emp("bar0", ["Front Bar, Sports Bar"])
    shifts = [_shift("s1", "bar0", MON, time(17), time(21), role="Sports Bar"),
              _shift("s2", "bar0", TUE, time(17), time(21), role="FRONT BAR")]
    conflicts = ConflictDetector().detect_conflicts(_roster(shifts), _venue({}), [emp])
    assert [c for c in conflicts if c.conflict_type == ConflictType.SKILL_MISMATCH] == []

    wrong = [_shift("s3", "bar0", MON, time(17), time(21), role="Kitchen")]
    conflicts = ConflictDetector().detect_conflicts(_roster(wrong), _venue({}), [emp])
    assert [c.conflict_type for c in conflicts].count(ConflictType.SKILL_MISMATCH) == 1


# ---------------------------------------------------------------------------
# breaks: unpaid meal break deducted, paid rest breaks never; warnings only
# ---------------------------------------------------------------------------

def test_break_table_separates_unpaid_meal_from_paid_rest():
    assert award_rules.BREAK_ENTITLEMENTS, "one named break table expected"
    assert award_rules.get_minimum_break_minutes(5.0) == 0
    assert award_rules.get_minimum_break_minutes(5.5) == 30
    assert award_rules.get_minimum_break_minutes(8.0) == 30
    assert award_rules.get_minimum_break_minutes(11.0) == 30
    assert award_rules.get_paid_rest_break_minutes(6.0) == 0
    assert award_rules.get_paid_rest_break_minutes(8.0) == 20
    assert award_rules.get_paid_rest_break_minutes(10.5) == 40


def test_generated_long_shift_deducts_only_the_meal_break():
    start, end, brk = _best_shift_template(9, 17, EmploymentType.casual)
    assert (start, end, brk) == (9, 17, 30)
    s = _shift("g", "a", MON, time(start), time(end), brk=brk)
    assert s.net_hours == 7.5


def test_standard_meal_break_is_not_a_conflict_and_a_short_one_only_warns():
    emp = _emp("a", ["front bar"])
    ok = [_shift("s1", "a", MON, time(10), time(18), brk=30),
          _shift("s2", "a", TUE, time(10), time(21), brk=30)]
    conflicts = ConflictDetector().detect_conflicts(_roster(ok), _venue({}), [emp])
    assert [c for c in conflicts if c.conflict_type == ConflictType.BREAK_VIOLATION] == []

    short = [_shift("s3", "a", MON, time(10), time(18), brk=0)]
    conflicts = ConflictDetector().detect_conflicts(_roster(short), _venue({}), [emp])
    brk = [c for c in conflicts if c.conflict_type == ConflictType.BREAK_VIOLATION]
    assert len(brk) == 1 and brk[0].severity == ConflictSeverity.WARNING


def test_deputy_week_with_30_minute_breaks_publishes():
    c, h = _owner_client()
    vid = _api_venue(c, h)
    db = get_db()
    for i in range(3):
        db.save_employee(_emp(f"{vid}-e{i}", ["front bar"], venue_id=vid))
    shifts = [_shift(f"{vid}-s{i}", f"{vid}-e{i}", MON + timedelta(days=i), time(10), time(18), brk=30)
              for i in range(3)]
    db.save_roster(_roster(shifts, rid=f"{vid}-r", venue_id=vid))
    r = c.post(f"/api/v1/rosters/{vid}-r/publish", json={"skip_approval": True}, headers=h)
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["critical_conflicts"] == 0, body
    assert body["success"] is True, body


# ---------------------------------------------------------------------------
# the hourly staffing walk: terminates, midnight-aware
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("close", [time(23, 30), time(23, 59)])
def test_late_close_with_minimums_returns_quickly(close):
    venue = _venue({"front bar": 1, "restaurant": 1})
    emp = _emp("bar0", ["front bar"])
    roster = _roster([_shift("s1", "bar0", MON, time(17), close)])
    with _deadline(3):
        conflicts = ConflictDetector().detect_conflicts(roster, venue, [emp])
    assert _staffing(conflicts) == ["Restaurant: nobody 17:00-24:00 Mon"]


def test_overnight_closer_counts_on_the_right_date():
    venue = _venue({"front bar": 2})
    emps = [_emp("bar0", ["front bar"]), _emp("bar1", ["front bar"])]
    roster = _roster([_shift("s1", "bar0", MON, time(18), time(23)),
                      _shift("s2", "bar1", MON, time(18), time(1))])
    with _deadline(3):
        conflicts = ConflictDetector().detect_conflicts(roster, venue, emps)
    assert _staffing(conflicts) == ["Front bar: 1 of 2 on 23:00-01:00 Mon"]
    gap = next(c for c in conflicts if c.conflict_type == ConflictType.UNDERSTAFFED_HOUR)
    assert (gap.date, gap.hour, gap.employee_ids) == (MON, 23, ["bar1"])

    # Tuesday's 00:00 trading hour is covered by Monday night's closer
    venue = _venue({"front bar": 1})
    forecasts = _forecasts(MON, range(18, 24)) + _forecasts(TUE, [0])
    conflicts = ConflictDetector().detect_conflicts(roster, venue, emps, forecasts=forecasts)
    assert _staffing(conflicts) == []


# ---------------------------------------------------------------------------
# every trading hour, per area, empty hours included
# ---------------------------------------------------------------------------

def test_every_forecast_hour_is_checked_per_area():
    venue = _venue({"front bar": 1, "restaurant": 1})
    emps = [_emp("bar0", ["front bar"]), _emp("rest0", ["restaurant"])]
    roster = _roster([_shift("s1", "rest0", MON, time(17), time(0), role="Restaurant"),
                      _shift("s2", "bar0", MON, time(17), time(21))])
    forecasts = _forecasts(MON, range(17, 24)) + _forecasts(TUE, range(17, 24))
    conflicts = ConflictDetector().detect_conflicts(roster, venue, emps, forecasts=forecasts)
    assert _staffing(conflicts) == [
        "Front bar: nobody 17:00-24:00 Tue",
        "Front bar: nobody 21:00-24:00 Mon",
        "Restaurant: nobody 17:00-24:00 Tue",
    ]
    staffing = [c for c in conflicts if c.conflict_type == ConflictType.UNDERSTAFFED_HOUR]
    assert {c.severity for c in staffing} == {ConflictSeverity.WARNING}


def test_rostered_span_is_checked_when_there_is_no_forecast():
    venue = _venue({"front bar": 2, "restaurant": 1})
    emps = [_emp("bar0", ["front bar"]), _emp("rest0", ["restaurant"]),
            _emp("rest1", ["restaurant"])]
    roster = _roster([_shift("s1", "rest0", MON, time(17), time(23), role="restaurant"),
                      _shift("s2", "rest1", MON, time(17), time(23), role="restaurant"),
                      _shift("s3", "bar0", MON, time(17), time(21))])
    conflicts = ConflictDetector().detect_conflicts(roster, venue, emps)
    assert _staffing(conflicts) == [
        "Front bar: 1 of 2 on 17:00-21:00 Mon",
        "Front bar: nobody 21:00-23:00 Mon",
    ]


def test_nobody_rostered_is_flagged_without_area_minimums():
    emp = _emp("a", [])
    roster = _roster([_shift("s1", "a", MON, time(17), time(20), role="general")])
    conflicts = ConflictDetector().detect_conflicts(
        roster, _venue({}), [emp], forecasts=_forecasts(MON, range(17, 23)))
    assert _staffing(conflicts) == ["Nobody rostered 20:00-23:00 Mon"]


def test_publish_succeeds_and_carries_staffing_warnings():
    c, h = _owner_client()
    vid = _api_venue(c, h, {"front bar": 2})
    db = get_db()
    for i in range(2):
        db.save_employee(_emp(f"{vid}-e{i}", ["front bar"], venue_id=vid))
    shifts = [_shift(f"{vid}-s{i}", f"{vid}-e{i}", MON, time(17), time(21)) for i in range(2)]
    db.save_roster(_roster(shifts, rid=f"{vid}-r", venue_id=vid))
    db.add_forecasts(_forecasts(MON, range(17, 24), venue_id=vid))

    with _deadline(10):
        r = c.post(f"/api/v1/rosters/{vid}-r/publish", json={"skip_approval": True}, headers=h)
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["success"] is True, body
    assert body["staffing_warnings"] == ["Front bar: nobody 21:00-24:00 Mon"]
    assert "Front bar: nobody 21:00-24:00 Mon" in body["message"]

    found = c.get(f"/api/v1/rosters/{vid}-r/conflicts", headers=h).json()
    assert "Front bar: nobody 21:00-24:00 Mon" in [x["message"] for x in found["conflicts"]]


# ---------------------------------------------------------------------------
# budget trim: every hour to close is protected
# ---------------------------------------------------------------------------

def test_budget_trim_keeps_the_late_closers():
    emps = [_emp(f"e{i}", ["front bar"]) for i in range(4)]
    shifts = [_shift("s0", "e0", SAT, time(17), time(23)),
              _shift("s1", "e1", SAT, time(17), time(23)),
              _shift("s2", "e2", SAT, time(22), time(1)),
              _shift("s3", "e3", SAT, time(22), time(1))]
    r = _roster(shifts)
    rep = apply_budget_cap(r, emps, State.wa, Decimal("200"), {"front bar": 2})
    assert rep["removed"] == [], rep["removed"]
    closers = [s for s in r.shifts if s.end_time < s.start_time]
    assert len(closers) == 2
    assert rep["met"] is False and "minimum coverage" in rep["note"]


def test_budget_trim_protects_mixed_case_imported_areas():
    emps = [_emp(f"e{i}", ["front bar"]) for i in range(3)]
    shifts = [_shift(f"s{i}", f"e{i}", SAT, time(17), time(23), role="Front Bar")
              for i in range(3)]
    r = _roster(shifts)
    rep = apply_budget_cap(r, emps, State.wa, Decimal("1"), {"front bar": 2})
    assert len(rep["removed"]) == 1
    assert len(r.shifts) == 2


# ---------------------------------------------------------------------------
# auto-fix past midnight
# ---------------------------------------------------------------------------

def test_auto_fix_extends_a_late_short_shift_past_midnight():
    c, h = _owner_client()
    vid = _api_venue(c, h)
    db = get_db()
    db.save_employee(_emp(f"{vid}-e0", ["front bar"], venue_id=vid))
    db.save_roster(_roster([_shift(f"{vid}-s0", f"{vid}-e0", MON, time(22), time(23, 45))],
                           rid=f"{vid}-r", venue_id=vid))
    r = c.post(f"/api/v1/rosters/{vid}-r/conflicts/auto-fix", headers=h)
    assert r.status_code == 200, r.text
    assert r.json()["changes_made"] == 1
    fixed = db.get_roster(f"{vid}-r").shifts[0]
    assert fixed.end_time == time(0, 15)
    assert fixed.duration_hours == 2.25
