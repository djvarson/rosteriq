"""
Wembley pilot, review wave F follow-ups: Postgres re-imports and closed days.

* A Deputy re-paste on Postgres deletes exactly the shifts it replaced or
  removed: the rebuilt week carries the stored copy's shift ids, so
  PostgresStore.save_roster's DELETE names them, and a shift another request
  added meanwhile survives. A Tanda re-sync and a backup "overwrite" import
  replace a stored roster the same way, and a Tanda re-sync never takes over
  another venue's roster.
* trading_hours[day] = {"closed": true} records a closed weekday. It saves
  and loads on both stores; it has no trading window, no demand and no
  minimums, so the forecaster, the generator, coverage, the conflict
  detector, the budget cap and cut advice all leave that day alone.
"""

import asyncio
import json
import uuid
from contextlib import contextmanager
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient

from rosteriq.api import app
from rosteriq.database import MemoryStore, PostgresStore, get_db
from rosteriq.ensemble import EnsembleForecaster
from rosteriq.models import DemandForecast, Employee, Roster, Shift, ShiftStatus, VenueConfig
from rosteriq.roster_optimiser import apply_budget_cap, compute_coverage_gaps, generate_weekly_roster
from rosteriq.services import venue_areas
from rosteriq.services.conflict_detector import ConflictType, detect_conflicts
from rosteriq.services.trading_hours import (
    demand_hours, effective_trading_hours, trading_day_of, venue_trading_hours,
)

PW = "Passw0rd!234"
DOM = "wembley-g2.example"
DAYS = ("mon", "tue", "wed", "thu", "fri", "sat", "sun")
MON = date(2026, 10, 12)
TUE, WED, SUN = MON + timedelta(days=1), MON + timedelta(days=2), MON + timedelta(days=6)


def _login(c, email):
    c.post("/api/auth/register", json={"email": email, "password": PW, "name": "U"})
    tok = c.post("/api/auth/login", json={"email": email, "password": PW}).json()["access_token"]
    return {"Authorization": f"Bearer {tok}"}


def _manager(c):
    _login(c, f"g2boot{uuid.uuid4().hex[:6]}@{DOM}")
    email = f"g2m{uuid.uuid4().hex[:8]}@{DOM}"
    h = _login(c, email)
    rec = get_db().get_user_by_email(email)
    rec["role"], rec["venue_ids"] = "staff", []
    get_db().save_user(rec)
    return h


def _venue_body(vid, name="Apothecary", **kw):
    body = {"id": vid, "name": name, "state": "wa", "max_labour_pct": 30, "tanda_org_id": "",
            "timezone": "Australia/Perth", "created_at": "2026-10-01T00:00:00"}
    body.update(kw)
    return body


# ---------------------------------------------------------------------------
# A roster table that behaves like Postgres: rows persist between requests,
# only the SQL PostgresStore.save_roster emits is applied, and every read is
# a fresh object built the way PostgresStore builds it.
# ---------------------------------------------------------------------------

class _Rec:
    def __init__(self):
        self.log = []

    @contextmanager
    def _cursor(self):
        log = self.log

        class _Cur:
            def execute(self, sql, params=None):
                log.append((sql, list(params or [])))
        yield _Cur()


class _PGRosters:
    def __init__(self):
        self.rosters, self.shifts, self.deletes = {}, {}, []

    def save_roster(self, roster):
        rec = _Rec()
        PostgresStore.save_roster(rec, roster)
        (sql, params), = rec.log
        assert "BEGIN" not in sql.upper()
        self.rosters[roster.id] = {"id": roster.id, "venue_id": roster.venue_id,
                                   "week_start": roster.week_start, "week_end": roster.week_end,
                                   "total_cost": roster.total_cost, "created_at": roster.created_at}
        for s in roster.shifts:
            self.shifts[s.id] = (roster.id, s.model_copy(deep=True))
        if "DELETE FROM shifts" in sql:
            rid, ids = params[-2], params[-1]
            self.deletes.append(sorted(ids))
            for sid in ids:
                if self.shifts.get(sid, (None,))[0] == rid:
                    del self.shifts[sid]

    def add_shift(self, rid, shift):
        """What another request's save leaves in the table."""
        self.shifts[shift.id] = (rid, shift)

    def get_roster(self, rid):
        row = self.rosters.get(rid)
        if row is None:
            return None
        shifts = sorted((s.model_copy(deep=True) for r, s in self.shifts.values() if r == rid),
                        key=lambda s: (s.date, s.start_time))
        return PostgresStore._row_to_roster(None, row, shifts)

    def list_rosters(self):
        return [self.get_roster(rid) for rid in self.rosters]

    def held(self, rid):
        return sorted((s.employee_id, s.date, f"{s.start_time:%H:%M}-{s.end_time:%H:%M}")
                      for r, s in self.shifts.values() if r == rid)


def _shift(sid, emp, day, start, end, role="bar"):
    return Shift(id=sid, employee_id=emp, date=day, start_time=time(*start), end_time=time(*end),
                 status=ShiftStatus.scheduled, role=role)


def _roster(rid, vid, monday, shifts):
    return Roster(id=rid, venue_id=vid, week_start=monday, week_end=monday + timedelta(days=6),
                  shifts=shifts, created_at=datetime(2026, 10, 1))


# ---------------------------------------------------------------------------
# Deputy re-paste on Postgres (findings 36, 38)
# ---------------------------------------------------------------------------

def test_pg_repaste_deletes_the_replaced_and_removed_shifts_and_keeps_one_added_meanwhile(monkeypatch):
    import rosteriq.routes.roster_import as roster_import
    c = TestClient(app)
    h = _manager(c)
    vid = f"g2imp-{uuid.uuid4().hex[:8]}"
    assert c.post("/venues", json=_venue_body(vid, name="The Wembley Hotel"), headers=h).status_code == 200
    r = c.post("/api/setup/import-staff", json={"venue_id": vid, "content":
               f"Name,Email,Pay Rate,Employment Type\nAlice Ng,alice@{DOM},30,Casual\n"
               f"Bob Tran,bob@{DOM},30,Casual\n"}, headers=h)
    assert r.status_code == 200, r.text
    db = get_db()
    ids = {e.name: e.id for e in db.get_employees(vid)}
    alice, bob = ids["Alice Ng"], ids["Bob Tran"]

    pg = _PGRosters()
    monkeypatch.setattr(db, "save_roster", pg.save_roster)
    monkeypatch.setattr(db, "get_roster", pg.get_roster)
    monkeypatch.setattr(db, "list_rosters", pg.list_rosters)
    rid = f"imported-{vid}-{MON.isoformat()}"

    r = c.post("/api/setup/import-roster", json={"venue_id": vid, "content":
               "Name,Date,Start,End\nAlice Ng,14/10/2026,11:00,17:00\nBob Tran,15/10/2026,10:00,16:00\n"
               "Bob Tran,16/10/2026,10:00,16:00\n"}, headers=h)
    assert r.status_code == 200, r.text
    first = {(s.employee_id, s.date): s.id for s in pg.get_roster(rid).shifts}
    alice_old, bob_removed = first[(alice, date(2026, 10, 14))], first[(bob, date(2026, 10, 15))]

    # A shift another request adds to the week between this import's read
    # and its save.
    real_reconcile = roster_import._reconcile

    def _reconcile_after_a_concurrent_add(*args, **kw):
        pg.add_shift(rid, _shift("added-meanwhile", alice, date(2026, 10, 18), (9, 0), (13, 0)))
        return real_reconcile(*args, **kw)
    monkeypatch.setattr(roster_import, "_reconcile", _reconcile_after_a_concurrent_add)

    r = c.post("/api/setup/import-roster", json={"venue_id": vid, "content":
               "Name,Date,Start,End\nAlice Ng,14/10/2026,12:00,18:00\nAlice Ng,15/10/2026,09:00,13:00\n"
               "Bob Tran,16/10/2026,10:00,16:00\n"}, headers=h)
    assert r.status_code == 200, r.text
    out = r.json()
    assert out["shifts_replaced"] == 1 and out["shifts_removed"] == 1

    assert pg.deletes == [sorted([alice_old, bob_removed])]
    assert "added-meanwhile" in pg.shifts
    assert pg.held(rid) == sorted([
        (alice, date(2026, 10, 14), "12:00-18:00"),
        (alice, date(2026, 10, 15), "09:00-13:00"),
        (bob, date(2026, 10, 16), "10:00-16:00"),
        (alice, date(2026, 10, 18), "09:00-13:00"),
    ])

    # The same paste again deletes nothing; a meal-break change rewrites the
    # row in place, under the same id.
    monkeypatch.setattr(roster_import, "_reconcile", real_reconcile)
    paste = ("Name,Date,Start,End,Break\nAlice Ng,14/10/2026,12:00,18:00,{}\n"
             "Alice Ng,15/10/2026,09:00,13:00,0\nBob Tran,16/10/2026,10:00,16:00,0\n")
    for brk, replaced in ((0, 0), (30, 1)):
        r = c.post("/api/setup/import-roster", json={"venue_id": vid, "content": paste.format(brk)},
                   headers=h)
        assert r.status_code == 200 and r.json()["shifts_replaced"] == replaced, r.text
    assert len(pg.deletes) == 1 and len(pg.held(rid)) == 4


class _FakeTanda:
    def __init__(self, rosters):
        self._rosters = rosters

    def __call__(self, credentials):
        return self

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return False

    async def get_roster(self, week_start):
        return self._rosters.get(week_start)


def test_pg_tanda_resync_deletes_shifts_tanda_dropped_and_never_takes_another_venues_roster(monkeypatch):
    import rosteriq.services.onboarding as onboarding
    pg = _PGRosters()
    today = date.today()
    this_week = today - timedelta(days=today.weekday())
    next_week = this_week + timedelta(days=7)
    pg.save_roster(_roster("tanda-77", "wem", this_week,
                           [_shift("t1", "e1", this_week, (10, 0), (16, 0)),
                            _shift("t2", "e1", this_week + timedelta(days=1), (10, 0), (16, 0))]))
    pg.save_roster(_roster("tanda-88", "someone-else", next_week,
                           [_shift("x1", "z1", next_week, (10, 0), (16, 0))]))
    pg.deletes.clear()
    from_tanda = {
        this_week: _roster("tanda-77", "org", this_week, [_shift("t1", "e1", this_week, (11, 0), (17, 0))]),
        next_week: _roster("tanda-88", "org", next_week, [_shift("y1", "e1", next_week, (10, 0), (16, 0))]),
    }
    monkeypatch.setattr(onboarding, "TandaAdapter", _FakeTanda(from_tanda))
    svc = onboarding.OnboardingService(db=pg)
    state = onboarding.OnboardingState(venue_id="wem", current_step=onboarding.OnboardingStep.IMPORT_ROSTERS,
                                       started_at=datetime(2026, 10, 1))
    asyncio.run(svc._step_import_rosters("wem", state, credentials=object()))

    assert pg.deletes == [["t2"]]
    assert pg.held("tanda-77") == [("e1", this_week, "11:00-17:00")]
    assert pg.rosters["tanda-88"]["venue_id"] == "someone-else"
    assert pg.held("tanda-88") == [("z1", next_week, "10:00-16:00")]


def test_pg_backup_overwrite_import_replaces_the_stored_rosters_shifts(tmp_path):
    from rosteriq.services.backup import BackupService
    pg = _PGRosters()
    pg.save_roster(_roster("r-bk", "wem", MON, [_shift("k1", "e1", MON, (10, 0), (16, 0)),
                                                 _shift("k2", "e1", TUE, (10, 0), (16, 0))]))
    pg.deletes.clear()
    exported = _roster("r-bk", "wem", MON, [_shift("k1", "e1", MON, (12, 0), (18, 0))]).model_dump(mode="json")
    svc = BackupService(backup_dir=str(tmp_path), db=pg)
    result = svc.import_venue_data({"rosters": [exported]}, conflict_strategy="overwrite")
    assert result.success and result.entities_imported["rosters"] == 1
    assert pg.deletes == [["k2"]]
    assert pg.held("r-bk") == [("e1", MON, "12:00-18:00")]


# ---------------------------------------------------------------------------
# A closed weekday (finding 8)
# ---------------------------------------------------------------------------

OPEN = {"open": "16:00", "close": "00:00"}
APOTHECARY_HOURS = {"mon": {"closed": True}, "tue": {"closed": True},
                    **{d: dict(OPEN) for d in ("wed", "thu", "fri", "sat", "sun")}}
BAR = [{"name": "bar", "min_by_day": {d: 2 for d in DAYS}}]


def _venue(**kw):
    base = dict(id="apo", name="Apothecary", tanda_org_id="", state="wa", timezone="Australia/Perth",
                max_labour_pct=30, created_at=datetime(2026, 10, 1), areas=BAR,
                trading_hours=APOTHECARY_HOURS)
    base.update(kw)
    return VenueConfig(**base)


def _emp(vid, eid, **kw):
    fields = dict(employment_type="casual", award_level="level_2", state="wa",
                  hourly_base_rate=Decimal("31.50"), skills=["bar"],
                  created_at=datetime(2026, 10, 1), updated_at=datetime(2026, 10, 1))
    fields.update(kw)
    return Employee(id=eid, venue_id=vid, name=eid, **fields)


def _fc(vid, day, hour, covers=60.0, version="pos_v1"):
    return DemandForecast(id=f"{vid}-{day}-{hour}-{version}", venue_id=vid, date=day, hour=hour,
                          predicted_covers=covers, confidence=0.8, model_version=version)


def test_closed_day_is_accepted_alongside_hours_with_the_same_day_keys():
    v = _venue(trading_hours={"Monday": {"closed": True}, "tue": {"closed": "true"},
                              "wed": {"open": "16:00", "close": "00:00"},
                              "thu": {"closed": False}, "xyz": {"closed": True},
                              "fri": {"closed": True, "open": "11:00", "close": "01:00"}})
    assert v.trading_hours == {"mon": {"closed": True}, "tue": {"closed": True},
                               "wed": {"open": "16:00", "close": "00:00"}, "fri": {"closed": True}}
    with pytest.raises(ValueError):
        _venue(trading_hours={"wed": {"open": "", "close": ""}})
    with pytest.raises(ValueError):
        _venue(trading_hours={"wed": {"closed": False, "open": "25:00", "close": "02:00"}})


def test_closed_day_round_trips_on_both_stores():
    v = _venue(id=f"apo-{uuid.uuid4().hex[:6]}")
    mem = MemoryStore()
    mem.save_venue(v)
    assert mem.get_venue(v.id).trading_hours["mon"] == {"closed": True}

    rec = _Rec()
    PostgresStore.save_venue(rec, v)
    (sql, params), = rec.log
    assert "trading_hours" in sql
    stored = next(p for p in params if isinstance(p, str) and '"closed"' in p)
    row = {"id": v.id, "name": v.name, "tanda_org_id": None, "state": "wa", "timezone": v.timezone,
           "min_staff": {}, "areas": json.loads(json.dumps(v.areas)),
           "trading_hours": json.loads(stored),          # psycopg2 hands JSONB back parsed
           "max_labour_pct": 30.0, "pos_system": None, "created_at": v.created_at}
    loaded = PostgresStore._row_to_venue(None, row)
    assert loaded.trading_hours == APOTHECARY_HOURS


def test_closed_day_saves_through_post_venues_and_reads_back():
    c = TestClient(app)
    h = _manager(c)
    vid = f"g2apo-{uuid.uuid4().hex[:6]}"
    r = c.post("/venues", json=_venue_body(vid, areas=BAR, trading_hours=APOTHECARY_HOURS), headers=h)
    assert r.status_code == 200, r.text
    assert c.get(f"/venues/{vid}", headers=h).json()["trading_hours"] == APOTHECARY_HOURS
    setup = c.get(f"/api/venue-setup?venue_id={vid}", headers=h).json()
    assert setup["effective_hours"]["mon"] == {"closed": True, "source": "recorded"}
    assert setup["effective_hours"]["wed"] == {"open": "16:00", "close": "00:00", "source": "recorded"}
    # Re-running the wizard with no hours keeps the closed days.
    r = c.post("/venues", json=_venue_body(vid, areas=BAR, trading_hours={}), headers=h)
    assert r.status_code == 200, r.text
    assert c.get(f"/venues/{vid}", headers=h).json()["trading_hours"] == APOTHECARY_HOURS


def test_closed_day_has_no_window_hours_demand_or_minimums():
    v = _venue()
    assert venue_areas.is_closed(v, MON) and not venue_areas.is_closed(v, WED)
    assert venue_areas.trading_window(v, MON) is None
    assert venue_areas.trading_hours_of_day(v, MON) == []
    assert venue_areas.trading_hours_of_day(_venue(trading_hours={}), MON) is None   # not recorded
    assert venue_areas.min_staff_for(v, MON) == {} and venue_areas.min_staff_for(v, WED) == {"bar": 2}
    eff = effective_trading_hours(v)
    assert eff["mon"] == {"closed": True, "source": "recorded"}
    assert demand_hours(eff, MON) == [] and demand_hours(eff, WED) == list(range(16, 24))
    assert trading_day_of(eff, MON, 12) is None
    # Sunday's close past midnight still trades into a closed Monday morning.
    late = effective_trading_hours(_venue(trading_hours={**APOTHECARY_HOURS,
                                                         "sun": {"open": "16:00", "close": "01:00"}}))
    assert trading_day_of(late, MON + timedelta(days=7), 0) == SUN
    assert demand_hours(late, MON + timedelta(days=7)) == [0]
    # A closed day is recorded: roster history never stands in for it.
    assert venue_trading_hours(MemoryStore(), v, MON)["tue"] == {"closed": True, "source": "recorded"}


def test_forecaster_emits_no_demand_on_a_closed_day():
    week = EnsembleForecaster(_venue()).predict_week(MON)
    days = {f.date for f in week}
    assert MON not in days and TUE not in days
    assert {f.hour for f in week if f.date == WED} == set(range(16, 24))


def test_generator_staffs_nothing_on_a_closed_day_even_with_demand_there():
    v = _venue(id="apo-gen")
    emps = [_emp("apo-gen", f"bar{i}") for i in range(8)]
    fcs = ([_fc("apo-gen", d, hr) for d in (MON, TUE) for hr in range(10, 24)]
           + [_fc("apo-gen", d, hr, version="cold_start_v1") for d in (MON, TUE) for hr in range(10, 24)]
           + [_fc("apo-gen", MON + timedelta(days=i), hr, covers=40.0) for i in range(2, 7) for hr in range(16, 24)])
    roster = generate_weekly_roster(MON, fcs, emps, v)
    by_day = {s.date for s in roster.shifts}
    assert MON not in by_day and TUE not in by_day
    assert WED in by_day and SUN in by_day


def test_generate_route_puts_no_shifts_on_a_closed_monday():
    c = TestClient(app)
    h = _manager(c)
    vid = f"g2gen-{uuid.uuid4().hex[:6]}"
    r = c.post("/venues", json=_venue_body(vid, areas=BAR, trading_hours=APOTHECARY_HOURS), headers=h)
    assert r.status_code == 200, r.text
    db = get_db()
    for i in range(8):
        db.save_employee(_emp(vid, f"{vid}-bar{i}"))
    r = c.post("/rosters/generate", json={"venue_id": vid, "week_start": MON.isoformat()}, headers=h)
    assert r.status_code == 200, r.text
    days = {date.fromisoformat(s["date"]) for s in r.json()["shifts"]}
    assert MON not in days and TUE not in days, sorted(days)
    assert {WED, SUN} <= days
    assert not [f for f in db.get_forecasts(vid, MON, TUE) if f.predicted_covers > 0]

    # Coverage on the generated week: no shortfall on the closed days, even
    # with demand stored there from before they were recorded closed.
    db.add_forecasts([_fc(vid, MON, hr) for hr in range(16, 22)])
    cov = c.get(f"/rosters/{r.json()['id']}/coverage", headers=h).json()
    assert not [d for d in cov["days"] if d["date"] in (MON.isoformat(), TUE.isoformat())], cov["days"]


def test_no_below_minimum_warnings_for_a_closed_day():
    v = _venue()
    fcs = [_fc("apo", MON, hr) for hr in range(10, 24)] + [_fc("apo", WED, hr, covers=10.0) for hr in range(16, 24)]
    roster = _roster("r-closed", "apo", MON, [_shift("m1", "bar0", MON, (10, 0), (14, 0)),
                                               _shift("w1", "bar0", WED, (16, 0), (0, 0))])
    cov = compute_coverage_gaps(roster, fcs, venue_config=v, trading_hours=venue_trading_hours(None, v, MON))
    assert [d["date"] for d in cov["days"]] == [WED.isoformat()]
    cov = compute_coverage_gaps(roster, fcs, venue_config=v)       # the recorded hours as they are
    assert [d["date"] for d in cov["days"]] == [WED.isoformat()]

    emps = [_emp("apo", "bar0")]
    short = [cf.message for cf in detect_conflicts(roster, v, emps, fcs)
             if cf.conflict_type == ConflictType.UNDERSTAFFED_HOUR]
    assert short and not [m for m in short if "Mon" in m], short
    no_areas = _venue(areas=[], min_staff={})
    short = [cf.message for cf in detect_conflicts(_roster("r2", "apo", MON, []), no_areas, emps, fcs)
             if cf.conflict_type == ConflictType.UNDERSTAFFED_HOUR]
    assert short and not [m for m in short if "Mon" in m], short

    # The budget cap may cut a closed day's shift; it never holds it as a minimum.
    roster = _roster("r-bud", "apo", MON, [_shift("m1", "bar0", MON, (10, 0), (14, 0))])
    report = apply_budget_cap(roster, emps, v.state, Decimal("1"), min_staff_by_role={"bar": 2}, venue=v)
    assert report["removed"] and roster.shifts == []


def test_cut_advice_on_a_closed_day_says_so_instead_of_missing_minimums():
    from rosteriq.services.cut_advice import recorded_settings
    v = _venue()
    closed = recorded_settings(v, ["bar"], MON)
    assert closed["minimums"] == {} and closed["skeleton"] is None
    assert not [n for n in closed["notes"] if "No minimum staffing" in n], closed["notes"]
    assert any("closed" in n for n in closed["notes"]), closed["notes"]
    assert recorded_settings(v, ["bar"], WED)["minimums"] == {"bar": 2}


def test_sos_call_in_on_a_closed_day_says_the_venue_is_closed():
    from rosteriq.services.sos import default_window
    start, end, note = default_window(_venue(), datetime.combine(TUE, time(12, 0)))
    assert note and "closed" in note and "No trading hours are recorded" not in note
    _, _, note = default_window(_venue(trading_hours={}), datetime.combine(TUE, time(12, 0)))
    assert "No trading hours are recorded" in note


def test_closed_day_reads_safely_from_a_bare_hours_map():
    hours = {"mon": {"closed": True}}
    ns = SimpleNamespace(trading_hours=hours)
    assert venue_areas.trading_window(ns, MON) is None and venue_areas.is_closed(ns, MON)
    assert effective_trading_hours(SimpleNamespace(trading_hours={}), inferred=effective_trading_hours(ns))["mon"] \
        == {"closed": True, "source": "recorded"}
