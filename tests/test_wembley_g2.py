"""
Wembley pilot, review wave F follow-ups: Postgres re-imports and closed days.

* A Deputy re-paste on Postgres deletes exactly the shifts it replaced or
  removed: the rebuilt week carries the stored copy's shift ids, so
  PostgresStore.save_roster's DELETE names them, and a shift another request
  added meanwhile survives. A Tanda re-sync and a backup "overwrite" import
  replace a stored roster the same way, and a Tanda re-sync never takes over
  another venue's roster.
* Each week of an import is saved under the roster lock. When another import
  (a double-clicked button) or an edit saved the week after it was read, the
  week is redone against what is stored now, so every pasted row is held
  once. The Import button is disabled while a request is in flight.
* trading_hours[day] = {"closed": true} records a closed weekday. It saves
  and loads on both stores; it has no trading window, no demand and no
  minimums, so the forecaster, the generator, the optimiser, the
  auto-scheduler, coverage, the conflict detector, the budget cap, cut advice
  and the dashboard's Day view and On-Shift heads all leave that day alone.
"""

import asyncio
import json
import os
import re
import shutil
import subprocess
import threading
import uuid
from contextlib import contextmanager
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from types import SimpleNamespace

import httpx
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


def _wembley_on_pg(c, h, monkeypatch):
    vid = f"g2imp-{uuid.uuid4().hex[:8]}"
    assert c.post("/venues", json=_venue_body(vid, name="The Wembley Hotel"), headers=h).status_code == 200
    r = c.post("/api/setup/import-staff", json={"venue_id": vid, "content":
               f"Name,Email,Pay Rate,Employment Type\nAlice Ng,alice@{DOM},30,Casual\n"
               f"Bob Tran,bob@{DOM},30,Casual\n"}, headers=h)
    assert r.status_code == 200, r.text
    db = get_db()
    pg = _PGRosters()
    monkeypatch.setattr(db, "save_roster", pg.save_roster)
    monkeypatch.setattr(db, "get_roster", pg.get_roster)
    monkeypatch.setattr(db, "list_rosters", pg.list_rosters)
    ids = {e.name: e.id for e in db.get_employees(vid)}
    return vid, pg, ids["Alice Ng"], ids["Bob Tran"]


def _both_read_before_either_saves(monkeypatch):
    import rosteriq.routes.roster_import as roster_import
    real = roster_import._reconcile
    both_read = threading.Barrier(2, timeout=10)

    def _reconcile(*a, **kw):
        both_read.wait()
        return real(*a, **kw)
    monkeypatch.setattr(roster_import, "_reconcile", _reconcile)


def _post_together(h, *bodies):
    async def _go():
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://t") as ac:
            return await asyncio.gather(*(ac.post("/api/setup/import-roster", json=b, headers=h)
                                          for b in bodies))
    return asyncio.run(_go())


DOUBLE_PASTE = ("Name,Date,Start,End\nAlice Ng,14/10/2026,11:00,17:00\nBob Tran,15/10/2026,10:00,16:00\n"
                "Bob Tran,16/10/2026,10:00,16:00\n")


def test_pg_double_clicked_import_holds_each_pasted_shift_once(monkeypatch):
    c = TestClient(app)
    h = _manager(c)
    vid, pg, alice, bob = _wembley_on_pg(c, h, monkeypatch)
    rid = f"imported-{vid}-{MON.isoformat()}"
    _both_read_before_either_saves(monkeypatch)

    a, b = _post_together(h, *[{"venue_id": vid, "content": DOUBLE_PASTE}] * 2)
    assert a.status_code == b.status_code == 200, (a.text, b.text)
    bobs = [(bob, date(2026, 10, 15), "10:00-16:00"), (bob, date(2026, 10, 16), "10:00-16:00")]
    assert pg.held(rid) == sorted([(alice, date(2026, 10, 14), "11:00-17:00")] + bobs)
    outs = sorted((r.json()["shifts_imported"], r.json()["duplicates_skipped"]) for r in (a, b))
    assert outs == [(0, 3), (3, 0)]          # the second says what is already there

    # A re-paste with a changed time, double-clicked: the old row goes once
    # and the new one is held once.
    changed = DOUBLE_PASTE.replace("Alice Ng,14/10/2026,11:00,17:00", "Alice Ng,14/10/2026,12:00,18:00")
    a, b = _post_together(h, *[{"venue_id": vid, "content": changed}] * 2)
    assert a.status_code == b.status_code == 200, (a.text, b.text)
    assert pg.held(rid) == sorted([(alice, date(2026, 10, 14), "12:00-18:00")] + bobs)
    assert sorted(r.json()["shifts_replaced"] for r in (a, b)) == [0, 1]


def test_import_redoes_a_week_an_edit_saved_after_it_was_read(monkeypatch):
    """A shift added by hand between the read and the save is taken as stored:
    one on a pasted day for someone in the paste goes, as it would had the
    edit come first; one on another day stays."""
    import rosteriq.routes.roster_import as roster_import
    c = TestClient(app)
    h = _manager(c)
    vid, pg, alice, bob = _wembley_on_pg(c, h, monkeypatch)
    rid = f"imported-{vid}-{MON.isoformat()}"
    assert c.post("/api/setup/import-roster", json={"venue_id": vid, "content": DOUBLE_PASTE},
                  headers=h).status_code == 200
    real = roster_import._reconcile

    def _edited_meanwhile(*a, **kw):
        out = real(*a, **kw)
        stored = pg.get_roster(rid)
        stored.shifts = stored.shifts + [_shift("hand-14", bob, date(2026, 10, 14), (9, 0), (12, 0)),
                                         _shift("hand-18", bob, date(2026, 10, 18), (9, 0), (12, 0))]
        pg.save_roster(stored)
        return out
    monkeypatch.setattr(roster_import, "_reconcile", _edited_meanwhile)
    r = c.post("/api/setup/import-roster", json={"venue_id": vid, "content": DOUBLE_PASTE.replace(
        "Bob Tran,16/10/2026,10:00,16:00\n", "Bob Tran,16/10/2026,11:00,16:00\n")}, headers=h)
    assert r.status_code == 200, r.text
    assert pg.held(rid) == sorted([
        (alice, date(2026, 10, 14), "11:00-17:00"), (bob, date(2026, 10, 15), "10:00-16:00"),
        (bob, date(2026, 10, 16), "11:00-16:00"), (bob, date(2026, 10, 18), "09:00-12:00")])
    out = r.json()
    assert out["shifts_imported"] == 1 and out["shifts_replaced"] == 1 and out["duplicates_skipped"] == 2
    assert [(x["date"], x["start"]) for x in out["removed"]] == [("2026-10-14", "09:00")]


def test_import_that_cant_get_the_week_saves_nothing_and_says_try_again(monkeypatch):
    from rosteriq.services import roster_edit as edit
    c = TestClient(app)
    h = _manager(c)
    vid, pg, alice, bob = _wembley_on_pg(c, h, monkeypatch)
    taken = []

    @contextmanager
    def _busy(db, roster_id, wait_seconds=5.0):
        taken.append(roster_id)
        raise edit.RosterBusy()
        yield
    monkeypatch.setattr(edit, "roster_lock", _busy)
    r = c.post("/api/setup/import-roster", json={"venue_id": vid, "content": DOUBLE_PASTE}, headers=h)
    assert r.status_code == 409, r.text
    assert taken == [f"imported-{vid}-{MON.isoformat()}"]
    assert pg.rosters == {} and pg.shifts == {}

    # Two weeks, the second busy: the first is saved and the second's rows
    # are reported skipped.
    second = MON + timedelta(days=7)

    @contextmanager
    def _second_busy(db, roster_id, wait_seconds=5.0):
        if roster_id.endswith(second.isoformat()):
            raise edit.RosterBusy()
        yield
    monkeypatch.setattr(edit, "roster_lock", _second_busy)
    r = c.post("/api/setup/import-roster", json={"venue_id": vid, "content":
               DOUBLE_PASTE + "Alice Ng,21/10/2026,11:00,17:00\n"}, headers=h)
    assert r.status_code == 200, r.text
    out = r.json()
    assert out["weeks"] == 1 and out["shifts_imported"] == 3
    assert [x["line"] for x in out["rows"] if x["status"] == "skipped"] == [5]
    assert "paste again" in out["skipped"][0]["reason"]
    assert set(pg.rosters) == {f"imported-{vid}-{MON.isoformat()}"}


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


def _next_monday(vid):
    from rosteriq.services.clock import venue_today
    today = venue_today(vid, get_db())
    return today + timedelta(days=7 - today.weekday())


@pytest.mark.parametrize("solver", ["greedy", "hybrid"])
def test_optimise_staffs_nothing_on_a_closed_day_whatever_forecast_rows_it_has(solver):
    c = TestClient(app)
    h = _manager(c)
    vid = f"g2opt-{uuid.uuid4().hex[:6]}"
    # Sunday trades past midnight into the closed Monday's 00:00 hour.
    hours = {**APOTHECARY_HOURS, "sun": {"open": "16:00", "close": "01:00"}}
    r = c.post("/venues", json=_venue_body(vid, areas=BAR, trading_hours=hours), headers=h)
    assert r.status_code == 200, r.text
    db = get_db()
    for i in range(8):
        db.save_employee(_emp(vid, f"{vid}-bar{i}"))
    mon = _next_monday(vid)
    tue = mon + timedelta(days=1)
    db.add_forecasts(
        [_fc(vid, mon, hr) for hr in range(24)]                       # stored before Monday was closed
        + [_fc(vid, tue, hr, covers=0.0, version="cold_start_v1") for hr in range(10, 24)]   # zeroed
        + [_fc(vid, mon + timedelta(days=d), hr, covers=40.0) for d in range(2, 7) for hr in range(16, 24)])
    r = c.post("/api/v1/rosters/optimise", json={"venue_id": vid, "week_start": mon.isoformat(),
                                                 "solver_preference": solver, "timeout_seconds": 10},
               headers=h)
    assert r.status_code == 200, r.text
    assert r.json()["status"] == "completed", r.json()
    days = {s.date for s in db.get_roster(r.json()["roster_id"]).shifts}
    assert mon not in days and tue not in days, sorted(days)
    assert mon + timedelta(days=2) in days


def test_optimise_a_week_with_demand_only_on_closed_days_is_a_clear_4xx():
    c = TestClient(app)
    h = _manager(c)
    vid = f"g2optc-{uuid.uuid4().hex[:6]}"
    r = c.post("/venues", json=_venue_body(vid, areas=BAR, trading_hours=APOTHECARY_HOURS), headers=h)
    assert r.status_code == 200, r.text
    db = get_db()
    for i in range(3):
        db.save_employee(_emp(vid, f"{vid}-bar{i}"))
    mon = _next_monday(vid)
    db.add_forecasts([_fc(vid, mon, hr) for hr in range(10, 24)])
    r = c.post("/api/v1/rosters/optimise", json={"venue_id": vid, "week_start": mon.isoformat(),
                                                 "solver_preference": "greedy"}, headers=h)
    assert r.status_code == 422, r.text
    assert "closed" in r.json()["error"]["message"]


def test_auto_schedule_staffs_nothing_on_a_closed_day():
    from rosteriq.services.auto_scheduler import AutoScheduler
    db = get_db()
    vid = f"g2auto-{uuid.uuid4().hex[:6]}"
    db.save_venue(_venue(id=vid))
    for i in range(6):
        db.save_employee(_emp(vid, f"{vid}-bar{i}"))
    # Monday has demand stored; Tuesday has none (the auto-scheduler's
    # fallback staffs a day with no forecast 06:00-23:00).
    db.add_forecasts([_fc(vid, MON, hr) for hr in range(10, 24)]
                     + [_fc(vid, MON + timedelta(days=d), hr, covers=40.0) for d in range(2, 7)
                        for hr in range(16, 23)])
    result = AutoScheduler(db).generate_week(vid, MON)
    days = {s.date for s in result.roster.shifts}
    assert MON not in days and TUE not in days, sorted(days)
    assert WED in days
    assert not [g for g in result.coverage_gaps if g.date in (MON, TUE)]
    grid = AutoScheduler(db).preview_week(vid, MON)["demand_grid"]
    assert MON.isoformat() not in grid and TUE.isoformat() not in grid and WED.isoformat() in grid


# ---------------------------------------------------------------------------
# Dashboard: Day view, On-Shift heads and the Import button
# ---------------------------------------------------------------------------

DASHBOARD = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "static", "dashboard.html")
NODE = shutil.which("node")
needs_node = pytest.mark.skipif(NODE is None, reason="node is required to run the dashboard JS")

JS_HARNESS = r"""
const _els = {};
function el(id) {
  if (!_els[id]) _els[id] = { id, innerHTML: '', textContent: '', value: '', disabled: false,
    style: {}, classList: { toggle() {} }, setAttribute() {} };
  return _els[id];
}
global.document = { getElementById: el };
global.esc = function (s) { return String(s == null ? '' : s).replace(/[&<>]/g, function (c) {
  return { '&': '&amp;', '<': '&lt;', '>': '&gt;' }[c]; }); };
global.localIso = function (d) { return d.toISOString().slice(0, 10); };
global.toasts = [];
global.showToast = function (m, k) { toasts.push([m, k]); };
global.state = { venues: [], currentVenue: 'apo', rosters: [], employees: [], isDemo: false };
global.setInterval = function () {};
"""


def _between(src, start, end):
    i = src.index(start)
    return src[i:src.index(end, i)]


def _run_js(scenario):
    with open(DASHBOARD, encoding="utf-8") as fh:
        src = re.findall(r"<script[^>]*>([\s\S]*?)</script>", fh.read())[1]
    ops = _between(src, "var OPS_WEEK_KEYS", "// ── Staff Contact Export")
    imp = _between(src, "async function runRosterImport() {", "\nfunction openStaffImport(")
    prog = JS_HARNESS + ops + "\n" + imp + "\n(async () => {\n" + scenario + \
        "\n})().catch(e => { console.log(JSON.stringify({error: String(e && e.stack || e)})); });"
    out = subprocess.run([NODE, "-e", prog], capture_output=True, text=True, timeout=30)
    assert out.returncode == 0, out.stderr
    data = json.loads(out.stdout.strip().splitlines()[-1])
    assert "error" not in data, data.get("error")
    return data


CLOSED_MONDAY_JS = """
const mins = { mon: 2, tue: 2, wed: 2, thu: 2, fri: 2, sat: 2, sun: 2 };
const venue = { id: 'apo', name: 'Apothecary', timezone: 'Australia/Perth',
  areas: [{ name: 'bar', label: 'Bar', min_by_day: mins }],
  trading_hours: { mon: { closed: true }, tue: { closed: true }, wed: { open: '16:00', close: '00:00' } } };
// 2026-10-12 is a Monday: one person in for a delivery.
const shifts = [{ id: 's1', employee_id: 'e1', date: '2026-10-12', start_time: '10:00', end_time: '14:00',
                  role: 'bar', status: 'scheduled' },
                { id: 's2', employee_id: 'e2', date: '2026-10-14', start_time: '16:00', end_time: '20:00',
                  role: 'bar', status: 'scheduled' }];
"""


@needs_node
def test_day_view_shows_no_minimums_on_a_closed_day():
    data = _run_js(CLOSED_MONDAY_JS + """
      const mon = dayTimelineData(shifts, venue, '2026-10-12');
      const wed = dayTimelineData(shifts, venue, '2026-10-14');
      console.log(JSON.stringify({
        mon: { below: mon.below, mins: mon.rows.map(r => r.min), closed: !!mon.closed },
        wed: { below: wed.below, mins: wed.rows.map(r => r.min), closed: !!wed.closed },
        empty: renderDayTimeline(dayTimelineData([], venue, '2026-10-13')),
        unrecorded: renderDayTimeline(dayTimelineData([], venue, '2026-10-15')),
      }));
    """)
    assert data["mon"] == {"below": 0, "mins": [0], "closed": True}
    assert data["wed"]["mins"] == [2] and data["wed"]["below"] > 0 and not data["wed"]["closed"]
    assert "closed" in data["empty"] and "no trading hours are recorded" not in data["empty"]
    assert "no trading hours are recorded" in data["unrecorded"]


@needs_node
def test_on_shift_heads_show_no_minimum_on_a_closed_day():
    data = _run_js(CLOSED_MONDAY_JS + """
      state.venues = [venue];
      state.rosters = [{ shifts: shifts }];
      state.employees = [{ id: 'e1', name: 'Meca' }, { id: 'e2', name: 'Willow' }];
      opsVenueNow = function () { return { iso: '2026-10-12', minute: 11 * 60 }; };
      renderOnShiftNow();
      const mon = { chips: el('headcount-areas').innerHTML, list: el('on-shift-list').innerHTML };
      opsVenueNow = function () { return { iso: '2026-10-14', minute: 17 * 60 }; };
      renderOnShiftNow();
      console.log(JSON.stringify({ mon, wed: el('headcount-areas').innerHTML }));
    """)
    assert "<strong>1</strong> Bar" in data["mon"]["chips"] and " below" not in data["mon"]["chips"]
    assert "(min" not in data["mon"]["list"]
    assert "<strong>1</strong> / 2 Bar" in data["wed"] and " below" in data["wed"]


@needs_node
def test_import_button_sends_one_request_while_one_is_in_flight():
    data = _run_js("""
      let calls = 0, release;
      global.fetchAPI = function () {
        calls++;
        return new Promise(function (ok) { release = function () { ok({ shifts_imported: 3, weeks: 1 }); }; });
      };
      global.renderRosterImportReport = function () { return ''; };
      global.refreshRealRoster = function () {};
      global.loadVenueData = function () {};
      el('roster-import-text').value = 'Name,Date,Start,End';
      const first = runRosterImport();
      const during = el('roster-import-btn').disabled;
      await runRosterImport();
      const second = calls;
      release();
      await first;
      console.log(JSON.stringify({ during, second, after: el('roster-import-btn').disabled }));
    """)
    assert data == {"during": True, "second": 1, "after": False}
