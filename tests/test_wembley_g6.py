"""The Wembley's time and pay, after review.

A forgotten clock-out a manager closes settles like one closed on the kiosk
and waits for approval; one person never holds two open punches, and a
second tap never doubles a break; a recorded break that is short of the meal
break or well over the rostered one needs a look; a clock-in corrected
across midnight moves the shift's day; payroll prices the minutes the
Timesheets page shows and refuses a day-long punch; a salaried leaver, unpaid
leave or a salary at both venues is said out loud; and a rostered day on paid
leave is never also priced as a shift.

Time-dependent tests freeze the time clock's 'now' at a Perth wall time.
"""

import json
import re
import uuid
from contextlib import contextmanager
from datetime import date, datetime, time as dtime, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest
from fastapi.testclient import TestClient

from rosteriq.api import app
from rosteriq.award_rules import get_minimum_break_minutes
from rosteriq.database import MemoryStore, OpenTimesheetExists, PostgresStore, get_db
from rosteriq.models import Roster, Shift, ShiftStatus
from rosteriq.routes import timeclock as timeclock_routes
from rosteriq.tests.kiosk_support import kiosk_headers
from rosteriq.tests.test_wembley_a6 import _main, _run, needs_node

PW = "Passw0rd!234"
PERTH = ZoneInfo("Australia/Perth")
DAY = date(2026, 10, 14)            # a Wednesday
NEXT = DAY + timedelta(days=1)
WEEK = DAY - timedelta(days=DAY.weekday())


def _login(c, email):
    c.post("/api/auth/register", json={"email": email, "password": PW, "name": "U"})
    r = c.post("/api/auth/login", json={"email": email, "password": PW})
    return {"Authorization": f"Bearer {r.json()['access_token']}"}


def _venue(c, h, vid, name):
    r = c.post("/venues", json={"id": vid, "name": name, "state": "wa", "max_labour_pct": 30,
                                "tanda_org_id": "", "timezone": "Australia/Perth",
                                "created_at": "2026-07-01T00:00:00"}, headers=h)
    assert r.status_code in (200, 201), r.text


def _employee(c, h, vid, emp_id, name, email=None, **extra):
    body = {"id": emp_id, "venue_id": vid, "name": name, "employment_type": "casual",
            "award_level": "level_2", "state": "wa", "hourly_base_rate": "30.00",
            "skills": ["bar"], "created_at": "2026-07-01T00:00:00",
            "updated_at": "2026-07-01T00:00:00"}
    if email:
        body["email"] = email
    body.update(extra)
    r = c.post("/employees", json=body, headers=h)
    assert r.status_code == 200, r.text
    return emp_id


def _world():
    c = TestClient(app)
    _login(c, f"g6boot{uuid.uuid4().hex[:6]}@x.com")          # platform owner, unused
    mgr_email = f"g6m{uuid.uuid4().hex[:8]}@x.com"
    h = _login(c, mgr_email)
    vid = f"wem-{uuid.uuid4().hex[:6]}"
    _venue(c, h, vid, "The Wembley Hotel")
    mgr = _login(c, mgr_email)
    ids = {k: _employee(c, mgr, vid, f"{vid}-{k}", n) for k, n in
           (("riley", "Riley Restaurant"), ("sam", "Sam Sportsbar"), ("jo", "Jo Bar"))}
    pins = {}
    for n, emp_id in enumerate(ids.values()):
        pins[emp_id] = f"{2468 + n * 1111}"
        r = c.post("/api/clock/pin", json={"venue_id": vid, "employee_id": emp_id, "pin": pins[emp_id]},
                   headers=mgr)
        assert r.status_code == 200, r.text
    k = kiosk_headers(c, mgr, vid)
    return SimpleNamespace(c=c, mgr=mgr, vid=vid, k=k, mgr_email=mgr_email, pins=pins, **ids)


def _msg(r):
    body = r.json()
    return (body.get("error") or {}).get("message") or body.get("detail") or ""


def _punch(w, emp, direction="in", **extra):
    body = {"venue_id": w.vid, "employee_id": emp, "pin": w.pins.get(emp, ""), **extra}
    return w.c.post(f"/api/clock/{direction}", json=body, headers=w.k)


@pytest.fixture
def clock(monkeypatch):
    """Set the time clock's 'now' to a Perth wall time."""
    def set_local(local_dt):
        utc = local_dt.replace(tzinfo=PERTH).astimezone(timezone.utc).replace(tzinfo=None)

        class _Frozen(datetime):
            @classmethod
            def utcnow(cls):
                return utc

        monkeypatch.setattr(timeclock_routes, "datetime", _Frozen)
        monkeypatch.setattr(timeclock_routes, "venue_today", lambda vid, db=None: local_dt.date())
        return utc
    return set_local


def _at(day, hh, mm=0, ss=0):
    return datetime.combine(day, dtime(hh, mm, ss))


def _utc(local_dt):
    """A Perth wall time as the naive UTC the API takes."""
    return local_dt.replace(tzinfo=PERTH).astimezone(timezone.utc).replace(tzinfo=None)


def _roster(vid, shifts, week=WEEK):
    """Publish a week's roster holding (shift_id, employee_id, day, start, end, break)."""
    db = get_db()
    rid = f"r-{uuid.uuid4().hex[:6]}"
    db.save_roster(Roster(
        id=rid, venue_id=vid, week_start=week, week_end=week + timedelta(days=6),
        shifts=[Shift(id=sid, employee_id=emp, date=day, start_time=s, end_time=e,
                      break_minutes=brk, status=ShiftStatus.scheduled, role="bar")
                for sid, emp, day, s, e, brk in shifts],
        total_cost=None, created_at=datetime(2026, 10, 1)))
    db.update_roster_state(rid, "published", "test", "test")


def _sheet(vid, ts_id, emp, ci, co, **over):
    ts = {"id": ts_id, "venue_id": vid, "employee_id": emp, "work_date": DAY,
          "clock_in": ci, "clock_out": co, "break_minutes": 30, "break_source": "recorded",
          "status": "closed", "pin_verified": True, "rostered_shift_id": None,
          "variance_minutes": 0, "flags": [], "created_at": ci}
    ts.update(over)
    get_db().save_timesheet(ts)
    return ts


def _rows(w, day=DAY, status=""):
    q = f"/api/clock/timesheets?venue_id={w.vid}&start_date={day}&end_date={day}"
    return w.c.get(q + (f"&status={status}" if status else ""), headers=w.mgr).json()


def _review(w, ts_id, **body):
    return w.c.post(f"/api/clock/timesheets/{ts_id}/review",
                    json={"venue_id": w.vid, **body}, headers=w.mgr)


def _prepare_actuals(w, start, end, vid=None):
    return w.c.post("/api/payroll/prepare-actuals", json={
        "venue_id": vid or w.vid, "period_start": str(start), "period_end": str(end),
        "state": "wa"}, headers=w.mgr)


def _batch(r):
    assert r.status_code == 200, r.text
    return get_db().get_payroll_batch(r.json()["batch_id"])


# ---------------------------------------------------------------------------
# [19] Closing an open punch through the Timesheets Edit
# ---------------------------------------------------------------------------

def test_a_manager_closed_punch_gets_the_meal_break_rule_and_waits_for_approval(clock):
    w = _world()
    _roster(w.vid, [("sh-riley", w.riley, NEXT, dtime(17), dtime(23), 30),
                    ("sh-sam", w.sam, NEXT, dtime(17), dtime(23), 30)])
    clock(_at(NEXT, 17))
    riley = _punch(w, w.riley).json()["timesheet_id"]
    sam = _punch(w, w.sam).json()["timesheet_id"]
    clock(_at(NEXT, 23))
    kiosk_out = _punch(w, w.sam, "out").json()
    assert (kiosk_out["worked_minutes"], kiosk_out["break_source"]) == (330, "auto")

    clock(_at(NEXT, 23, 30))
    r = _review(w, riley, clock_out=_utc(_at(NEXT, 23)).isoformat(), note="forgot to clock out")
    assert r.status_code == 200, r.text
    out = r.json()
    assert out["status"] == "closed"                  # not approved in the same step
    assert (out["worked_minutes"], out["break_minutes"], out["break_source"]) == (330, 30, "auto")
    assert {"manager_closed", "auto_break"} <= set(out["flags"])
    assert out["variance_minutes"] == 0

    row = {t["id"]: t for t in _rows(w, NEXT)["timesheets"]}[riley]
    assert row["clean"] is False and "finish time entered by a manager" in row["review_reasons"]
    bulk = w.c.post("/api/clock/timesheets/approve",
                    json={"venue_id": w.vid, "ids": [riley, sam], "only_clean": True}, headers=w.mgr)
    assert bulk.json()["approved_ids"] == [sam]
    assert _review(w, riley, approve=True).json()["status"] == "approved"


def test_closing_a_punch_mid_break_counts_the_break_so_far_and_approves_only_when_asked(clock):
    w = _world()
    _roster(w.vid, [("sh-jo", w.jo, NEXT, dtime(17), dtime(23), 30)])
    clock(_at(NEXT, 17))
    jo = _punch(w, w.jo).json()["timesheet_id"]
    clock(_at(NEXT, 22, 30))
    assert _punch(w, w.jo, "break/start").status_code == 200
    clock(_at(NEXT, 23, 30))
    listed = _rows(w, status="open")["timesheets"]
    assert listed[0]["on_break"] is True and listed[0]["break_minutes"] == 0

    r = _review(w, jo, approve=True, clock_out=_utc(_at(NEXT, 23)).isoformat(),
                note="went home from her break")
    assert r.status_code == 200, r.text
    out = r.json()
    assert out["status"] == "approved"                # the manager asked for it
    assert (out["worked_minutes"], out["break_minutes"], out["break_source"]) == (330, 30, "recorded")
    assert get_db().get_timesheet(jo)["break_started_at"] is None


def test_a_manager_break_figure_overrides_only_when_it_differs(clock):
    w = _world()
    _roster(w.vid, [("sh-r", w.riley, NEXT, dtime(17), dtime(23), 30),
                    ("sh-s", w.sam, NEXT, dtime(17), dtime(23), 30)])
    clock(_at(NEXT, 17))
    riley = _punch(w, w.riley).json()["timesheet_id"]
    sam = _punch(w, w.sam).json()["timesheet_id"]
    clock(_at(NEXT, 23, 30))
    co = _utc(_at(NEXT, 23)).isoformat()
    same = _review(w, riley, clock_out=co, break_minutes=30, note="forgot").json()
    assert same["break_source"] == "auto" and "auto_break" in same["flags"]
    longer = _review(w, sam, clock_out=co, break_minutes=45, note="took a long dinner").json()
    assert (longer["break_minutes"], longer["break_source"], longer["worked_minutes"]) == (45, "manager", 315)
    assert "auto_break" not in longer["flags"]


def test_a_correction_recomputes_the_review_flags(clock):
    w = _world()
    _roster(w.vid, [("sh-r", w.riley, NEXT, dtime(17), dtime(23), 30)])
    clock(_at(NEXT, 17))
    ts = _punch(w, w.riley).json()["timesheet_id"]
    clock(_at(NEXT, 23))
    assert _punch(w, w.riley, "out").json()["flags"] == ["auto_break"]
    clock(_at(NEXT + timedelta(days=1), 9))

    ci = _utc(_at(NEXT, 17)).isoformat()
    long_day = _review(w, ts, approve=False, clock_in=ci,
                       clock_out=_utc(_at(NEXT + timedelta(days=1), 7, 30)).isoformat(), note="typo")
    assert "long_shift" in long_day.json()["flags"]
    blip = _review(w, ts, approve=False, clock_in=ci,
                   clock_out=_utc(_at(NEXT, 17, 3)).isoformat(), note="typo again")
    assert {"short_punch"} <= set(blip.json()["flags"]) and "long_shift" not in blip.json()["flags"]
    no_break = _review(w, ts, approve=False, clock_in=ci, clock_out=_utc(_at(NEXT, 23)).isoformat(),
                       break_minutes=0, note="worked through")
    flags = set(no_break.json()["flags"])
    assert "no_break" in flags and not {"auto_break", "short_punch", "long_shift"} & flags


@needs_node
def test_the_edit_modal_prefills_the_rostered_break_and_sends_what_it_shows():
    src = _main("dashboard.html")
    fns = "\n".join(src[m.start():src.index("\n}\n", m.start()) + 2] for m in (
        re.search(r"^function tsMinutesLabel\(", src, re.M),
        re.search(r"^function openEditTimesheet\(", src, re.M)))
    start = src.index("document.getElementById('ets-save').addEventListener(")
    listener = src[start:src.index("\n});\n", start) + 5]
    data = _run(r"""
global.state = { currentVenue: 'wem' };
global.posted = [];
global.fetchAPI = async function (url, opts) {
  posted.push({ url: url, body: JSON.parse(opts.body) });
  return { status: 'closed', worked_minutes: 330, break_minutes: 30 };
};
global.openModal = function () {};
global.closeModal = function () {};
global.loadTimesheets = function () {};
var rostered = { start: '17:00', end: '23:00', break_minutes: 30 };
var tsRows = [
  { id: 't-open', employee_name: 'Riley', work_date: '2026-10-15', clock_in: '2026-10-15T09:00:00',
    clock_out: null, status: 'open', break_minutes: 0, on_break: false, rostered: rostered },
  { id: 't-brk', employee_name: 'Jo', work_date: '2026-10-15', clock_in: '2026-10-15T09:00:00',
    clock_out: null, status: 'open', break_minutes: 0, on_break: true, rostered: rostered },
  { id: 't-closed', employee_name: 'Sam', work_date: '2026-10-15', clock_in: '2026-10-15T09:00:00',
    clock_out: '2026-10-15T15:00:00', status: 'closed', break_minutes: 30, rostered: rostered },
];
""" + fns + "\nlet etsIndex = null;\nlet etsShown = null;\n" + listener + r"""
async function main() {
  const out = {};
  openEditTimesheet(0);
  out.pre = String(el('ets-break').value); out.label = el('ets-save').textContent;
  el('ets-out').value = '2026-10-15T23:00'; el('ets-note').value = 'forgot';
  await el('ets-save').fire('click');
  openEditTimesheet(1);
  out.onBreak = String(el('ets-break').value); out.who = el('ets-who').textContent;
  el('ets-out').value = '2026-10-15T23:00'; el('ets-note').value = 'forgot';
  await el('ets-save').fire('click');
  openEditTimesheet(2);
  out.closedLabel = el('ets-save').textContent;
  el('ets-break').value = '45'; el('ets-note').value = 'long dinner';
  await el('ets-save').fire('click');
  out.posted = posted; out.toasts = toasts;
  console.log(JSON.stringify(out));
}
""")
    assert data["pre"] == "30" and data["label"] == "Save finish time"
    first, second, third = (p["body"] for p in data["posted"])
    assert first["approve"] is False and first["break_minutes"] == 30   # the break it showed
    assert first["clock_in"] is None and first["clock_out"]
    assert data["onBreak"] == "0" and "on a break" in data["who"]
    assert second["break_minutes"] is None
    assert data["closedLabel"] == "Save & Approve"
    assert third == {"venue_id": "wem", "approve": True, "clock_in": None, "clock_out": None,
                     "break_minutes": 45, "note": "long dinner"}
    assert "Approve it once checked" in data["toasts"][0]["m"]


@needs_node
@pytest.mark.parametrize("auto, source", [(False, "manager"), (True, "auto")])
def test_closing_a_punch_from_the_edit_modal_saves_the_break_it_shows(clock, auto, source):
    w = _world()
    _roster(w.vid, [("sh-r", w.riley, NEXT, dtime(17), dtime(23), 30)])
    r = w.c.put("/api/kiosk/settings", json={"venue_id": w.vid, "auto_meal_break": auto}, headers=w.mgr)
    assert r.status_code == 200, r.text
    clock(_at(NEXT, 17))
    ts = _punch(w, w.riley).json()["timesheet_id"]
    clock(_at(NEXT, 23, 30))
    row = _rows(w, NEXT, status="open")["timesheets"][0]
    src = _main("dashboard.html")
    fns = "\n".join(src[m.start():src.index("\n}\n", m.start()) + 2] for m in (
        re.search(r"^function tsMinutesLabel\(", src, re.M),
        re.search(r"^function openEditTimesheet\(", src, re.M)))
    start = src.index("document.getElementById('ets-save').addEventListener(")
    listener = src[start:src.index("\n});\n", start) + 5]
    finish = _utc(_at(NEXT, 23)).strftime("%Y-%m-%dT%H:%M")       # node runs in UTC
    data = _run(f"""
global.state = {{ currentVenue: {json.dumps(w.vid)} }};
global.posted = [];
global.fetchAPI = async function (url, opts) {{
  posted.push(JSON.parse(opts.body));
  return {{ status: 'closed', worked_minutes: 0, break_minutes: 0 }};
}};
global.openModal = function () {{}};
global.closeModal = function () {{}};
global.loadTimesheets = function () {{}};
var tsRows = [{json.dumps(row)}];
""" + fns + "\nlet etsIndex = null;\nlet etsShown = null;\n" + listener + f"""
async function main() {{
  openEditTimesheet(0);
  const shown = String(el('ets-break').value);
  el('ets-out').value = {json.dumps(finish)}; el('ets-note').value = 'forgot to clock out';
  await el('ets-save').fire('click');
  console.log(JSON.stringify({{ shown: shown, body: posted[0] }}));
}}
""", env={"TZ": "UTC"})
    assert data["shown"] == "30"
    out = w.c.post(f"/api/clock/timesheets/{ts}/review", json=data["body"], headers=w.mgr).json()
    assert (out["break_minutes"], out["break_source"], out["worked_minutes"]) == (30, source, 330)
    assert "no_break" not in out["flags"] and out["status"] == "closed"


# ---------------------------------------------------------------------------
# [20] One open punch per person; a second tap never doubles anything
# ---------------------------------------------------------------------------

def test_the_memory_store_holds_one_open_punch_per_person_per_venue():
    db = MemoryStore()
    ci = datetime(2026, 10, 15, 9)
    db.save_timesheet({"id": "a", "venue_id": "wem", "employee_id": "amy", "clock_in": ci, "clock_out": None})
    with pytest.raises(OpenTimesheetExists):
        db.save_timesheet({"id": "b", "venue_id": "wem", "employee_id": "amy", "clock_in": ci, "clock_out": None})
    db.save_timesheet({"id": "a", "venue_id": "wem", "employee_id": "amy", "clock_in": ci, "clock_out": None})
    db.save_timesheet({"id": "c", "venue_id": "apo", "employee_id": "amy", "clock_in": ci, "clock_out": None})
    db.save_timesheet({"id": "d", "venue_id": "wem", "employee_id": "amy", "clock_in": ci,
                       "clock_out": ci + timedelta(hours=6)})
    assert [t["id"] for t in db.list_open_timesheets("wem")] == ["a"]


def test_a_racing_second_clock_in_gets_already_clocked_in(clock, monkeypatch):
    w = _world()
    db = get_db()
    clock(_at(NEXT, 17))
    assert _punch(w, w.riley).status_code == 200
    # The other worker's check ran before this one's insert landed
    monkeypatch.setattr(db, "get_open_timesheet", lambda venue_id, employee_id: None)
    again = _punch(w, w.riley)
    assert again.status_code == 409 and "already clocked in" in _msg(again)
    assert len([t for t in db.list_open_timesheets(w.vid) if t["employee_id"] == w.riley]) == 1


def test_a_second_end_break_tap_is_refused_not_added_twice(clock, monkeypatch):
    w = _world()
    db = get_db()
    clock(_at(NEXT, 11))
    _punch(w, w.riley)
    clock(_at(NEXT, 14))
    _punch(w, w.riley, "break/start")
    clock(_at(NEXT, 14, 25))
    stale = dict(db.get_open_timesheet(w.vid, w.riley))      # what the other worker read
    first = _punch(w, w.riley, "break/end")
    assert first.json()["total_break_minutes"] == 25
    monkeypatch.setattr(db, "get_open_timesheet", lambda venue_id, employee_id: dict(stale))
    second = _punch(w, w.riley, "break/end")
    assert second.status_code == 409 and "not on a break" in _msg(second)
    assert db.get_timesheet(stale["id"])["break_minutes"] == 25


@needs_node
def test_the_kiosk_disables_its_buttons_until_the_punch_answers():
    src = _main("timeclock.html")
    fns = "\n".join(src[m.start():src.index("\n}\n", m.start()) + 2] for m in (
        re.search(r"^async function punch\(", src, re.M),
        re.search(r"^function punchMessage\(", src, re.M)))
    data = _run(r"""
var current = { employee_id: 'e1', name: 'Amy' }, pin = '1234', venueId = 'wem', query = '';
function modalError(msg) { el('m-err').textContent = msg || ''; }
function minutesLabel(m) { return String(m); }
global.toast = function () {};
global.loadBoard = function () {};
global.closeModal = function () {};
global.switchedOff = function () {};
var settle = null;
global.api = function () { calls.push(1); return new Promise(function (res, rej) { settle = rej; }); };
""" + fns + r"""
async function main() {
  const a = mkEl(''), b = mkEl('');
  el('m-actions').children = [a, b];
  const p = punch('in');
  const during = [a.disabled, b.disabled];
  settle(Object.assign(new Error('Amy is already clocked in'), { status: 409 }));
  await p;
  console.log(JSON.stringify({ during: during, after: [a.disabled, b.disabled], err: el('m-err').textContent }));
}
""")
    assert data["during"] == [True, True]
    assert data["after"] == [False, False] and "already clocked in" in data["err"]


class _Cur:
    def __init__(self, log, rows=None, fail=None):
        self.log, self.rows, self.fail = log, rows or [], fail

    def execute(self, sql, params=None):
        flat = " ".join(sql.split())
        self.log.append((flat, params))
        if self.fail and self.fail[0] in flat:
            raise self.fail[1]

    def fetchone(self):
        return self.rows[0] if self.rows else None

    def fetchall(self):
        return list(self.rows)

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


class _PG:
    """Just enough PostgresStore to run one method against a recording cursor."""
    _TABLE_DDL = PostgresStore._TABLE_DDL
    _ONE_OPEN_PUNCH_DDL = PostgresStore._ONE_OPEN_PUNCH_DDL
    _ensure_table = PostgresStore._ensure_table

    def __init__(self, rows=None, fail=None):
        self.log, self.rows, self.fail = [], rows, fail

    @contextmanager
    def _cursor(self):
        yield _Cur(self.log, self.rows, self.fail)

    def sql(self):
        return [s for s, _ in self.log if not s.startswith("CREATE TABLE")]


def _ts_row(**over):
    ts = {"id": "ts1", "venue_id": "wem", "employee_id": "amy", "work_date": DAY,
          "clock_in": datetime(2026, 10, 14, 9), "clock_out": None, "break_minutes": 0,
          "status": "open", "flags": []}
    ts.update(over)
    return ts


def test_pg_has_a_one_open_punch_index_that_never_blocks_startup():
    import inspect
    import psycopg2.errors
    ddl = PostgresStore._ONE_OPEN_PUNCH_DDL
    assert ddl.startswith("CREATE UNIQUE INDEX IF NOT EXISTS uq_timesheets_one_open")
    assert ddl.endswith("ON timesheets (venue_id, employee_id) WHERE clock_out IS NULL")
    assert "self._ensure_one_open_punch()" in inspect.getsource(PostgresStore._ensure_core_schema)

    ok = _PG()
    PostgresStore._ensure_one_open_punch(ok)
    assert ok.sql() == [ddl]

    # Prod already holds a duplicate open punch: logged with the duplicates, boot carries on
    dup = psycopg2.errors.UniqueViolation('could not create unique index "uq_timesheets_one_open"')
    failing = _PG(rows=[{"venue_id": "wem", "employee_id": "amy", "open_punches": 2}],
                  fail=("uq_timesheets_one_open", dup))
    PostgresStore._ensure_one_open_punch(failing)
    assert failing.sql()[0] == ddl and "HAVING count(*) > 1" in failing.sql()[1]


def test_pg_turns_the_open_punch_violation_into_already_clocked_in():
    import psycopg2.errors
    dup = psycopg2.errors.UniqueViolation(
        'duplicate key value violates unique constraint "uq_timesheets_one_open"')
    with pytest.raises(OpenTimesheetExists):
        PostgresStore.save_timesheet(_PG(fail=("INSERT INTO timesheets", dup)), _ts_row())
    other = psycopg2.errors.UniqueViolation('duplicate key value violates unique constraint "x"')
    with pytest.raises(psycopg2.errors.UniqueViolation):
        PostgresStore.save_timesheet(_PG(fail=("INSERT INTO timesheets", other)), _ts_row())


def test_pg_end_break_is_one_guarded_update():
    pg = _PG(rows=[{"break_minutes": 55}])
    assert PostgresStore.end_timesheet_break(pg, "ts1", 25) == 55
    stmts = pg.sql()
    assert len(stmts) == 1, stmts
    sql, params = pg.log[-1]
    assert sql.startswith("UPDATE timesheets SET") and "RETURNING break_minutes" in sql
    assert "WHERE id = %s AND break_started_at IS NOT NULL" in sql
    assert "break_started_at = NULL" in sql and sql.count("%s") == len(params)
    assert not any(w in sql.upper().split() for w in ("BEGIN", "COMMIT", "ROLLBACK"))
    assert PostgresStore.end_timesheet_break(_PG(rows=[]), "ts1", 25) is None


def test_overlapping_punches_need_a_look_and_payroll_says_so():
    w = _world()
    ci = _utc(_at(DAY, 17))
    _sheet(w.vid, "ts-a", w.riley, ci, ci + timedelta(hours=6, minutes=2))
    _sheet(w.vid, "ts-b", w.riley, ci + timedelta(minutes=1), ci + timedelta(hours=6, minutes=3))
    _sheet(w.vid, "ts-c", w.sam, ci, ci + timedelta(hours=6))
    rows = {t["id"]: t for t in _rows(w)["timesheets"]}
    for ts_id in ("ts-a", "ts-b"):
        assert "overlaps another punch for this person" in rows[ts_id]["review_reasons"]
    assert "overlaps another punch for this person" not in rows["ts-c"]["review_reasons"]
    bulk = w.c.post("/api/clock/timesheets/approve", headers=w.mgr, json={
        "venue_id": w.vid, "ids": ["ts-a", "ts-b"], "only_clean": True}).json()
    assert bulk["approved"] == 0 and "overlaps another punch" in bulk["skipped"][0]["reason"]
    w.c.post("/api/clock/timesheets/approve", headers=w.mgr,
             json={"venue_id": w.vid, "ids": ["ts-a", "ts-b", "ts-c"]})
    errors = _prepare_actuals(w, DAY, DAY).json()["validation_errors"]
    assert any("Riley Restaurant: approved punches overlap" in e for e in errors), errors
    assert not any("Sam Sportsbar" in e for e in errors)


# ---------------------------------------------------------------------------
# [21] A break is an unpaid meal break; odd ones need a look
# ---------------------------------------------------------------------------

def test_a_recorded_break_well_over_the_rostered_one_needs_a_look(clock):
    w = _world()
    _roster(w.vid, [("sh-g", w.riley, NEXT, dtime(16), dtime(23), 30)])
    for at, action in ((_at(NEXT, 15, 55), "in"), (_at(NEXT, 19), "break/start"),
                       (_at(NEXT, 19, 30), "break/end"), (_at(NEXT, 21), "break/start"),
                       (_at(NEXT, 21, 20), "break/end"), (_at(NEXT, 23, 5), "out")):
        clock(at)
        assert _punch(w, w.riley, action).status_code == 200, action
    row = _rows(w, NEXT)["timesheets"][0]
    assert (row["break_minutes"], row["variance_minutes"]) == (50, -10)
    assert row["clean"] is False
    assert "50 min unpaid break, 30 rostered — paid rest break included?" in row["review_reasons"]


def test_a_recorded_break_under_the_meal_break_minimum_needs_a_look(clock):
    w = _world()
    _roster(w.vid, [("sh-s", w.sam, NEXT, dtime(16), dtime(23), 10)])
    for at, action in ((_at(NEXT, 16), "in"), (_at(NEXT, 19), "break/start"),
                       (_at(NEXT, 19, 10), "break/end"), (_at(NEXT, 23), "out")):
        clock(at)
        assert _punch(w, w.sam, action).status_code == 200, action
    row = _rows(w, NEXT)["timesheets"][0]
    assert row["variance_minutes"] == 0 and row["clean"] is False
    minimum = get_minimum_break_minutes(7)
    assert f"10 min break, under the {minimum} min meal break" in row["review_reasons"]


@needs_node
def test_the_kiosk_calls_its_break_an_unpaid_meal_break():
    from rosteriq.tests.test_wembley_b5 import NO_TIMERS, _kiosk_js
    data = _run(NO_TIMERS + r"""
global.punch = function () {};
global.closeModal = function () {};
var modalTimer = null, MODAL_IDLE_MS = 45000;
""" + _kiosk_js("holdModal", "cardSub", "addAction", "modalError", "localHHMM", "showActions",
                "punchMessage", "minutesLabel") + r"""
function labels() { return el('m-actions').children.map(function (b) { return b.textContent; }); }
async function main() {
  const out = {};
  el('m-actions').children = []; showActions({ state: 'on' }); out.on = labels();
  el('m-actions').children = []; showActions({ state: 'break' }); out.brk = labels();
  out.started = punchMessage('break/start', { employee: 'Gus' });
  console.log(JSON.stringify(out));
}
""")
    assert data["on"] == ["Start meal break (unpaid)", "Clock out"]
    assert data["brk"] == ["End meal break", "Clock out"]
    assert "unpaid meal break" in data["started"]


# ---------------------------------------------------------------------------
# [22] A clock-in corrected across midnight moves the shift's day
# ---------------------------------------------------------------------------

def test_correcting_a_clock_in_across_midnight_moves_the_work_date(clock):
    w = _world()
    sunday = date(2026, 10, 18)
    _roster(w.vid, [("sh-dan", w.riley, sunday, dtime(18), dtime(2), 30)])
    clock(_at(sunday + timedelta(days=1), 0, 20))
    ts_id = _punch(w, w.riley).json()["timesheet_id"]
    clock(_at(sunday + timedelta(days=1), 2))
    _punch(w, w.riley, "out")
    db = get_db()
    assert db.get_timesheet(ts_id)["work_date"] == sunday + timedelta(days=1)

    r = _review(w, ts_id, approve=True, clock_in=_utc(_at(sunday, 18)).isoformat(),
                break_minutes=30, note="clocked in late, started at 6")
    assert r.status_code == 200, r.text
    assert r.json()["work_date"] == "2026-10-18" and r.json()["variance_minutes"] == 0
    stored = db.get_timesheet(ts_id)
    assert stored["work_date"] == sunday and stored["rostered_shift_id"] == "sh-dan"

    batch = _batch(_prepare_actuals(w, WEEK, sunday))
    dan = {e["employee_id"]: e for e in batch["employees"]}[w.riley]
    # Sunday 18:00-24:00 of an 8h span with its 30 min break spread across it
    sunday_hours = (Decimal(6) * Decimal(450) / Decimal(480)).quantize(Decimal("0.01"), ROUND_HALF_UP)
    assert dict((p["penalty_type"], p["hours"]) for p in dan["penalty_entries"])["sunday"] == str(sunday_hours)
    assert _prepare_actuals(w, sunday + timedelta(days=1), sunday + timedelta(days=7)).status_code == 422


def test_pg_save_timesheet_updates_the_work_date_on_conflict():
    pg = _PG()
    PostgresStore.save_timesheet(pg, _ts_row())
    sql, params = pg.log[-1]
    assert "INSERT INTO timesheets" in sql
    update = sql.split("ON CONFLICT (id) DO UPDATE SET", 1)[1]
    assert "work_date=EXCLUDED.work_date" in update
    assert sql.count("%s") == len(params)


# ---------------------------------------------------------------------------
# [23] Payroll uses the Timesheets page's minutes; a day-long punch is refused
# ---------------------------------------------------------------------------

def test_payroll_hours_equal_the_timesheets_page():
    w = _world()
    ci = _utc(_at(DAY, 10, 0, 45))
    _sheet(w.vid, "ts-sec", w.riley, ci, _utc(_at(DAY, 15, 30, 10)), break_minutes=0,
           status="approved")
    page = _rows(w)
    assert page["timesheets"][0]["worked_minutes"] == 329
    riley = _batch(_prepare_actuals(w, DAY, DAY))["employees"][0]
    assert Decimal(riley["ordinary_hours"]) == (Decimal(329) / 60).quantize(Decimal("0.01"))


def test_a_correction_of_a_day_or_more_is_refused_and_never_approved(clock):
    w = _world()
    clock(_at(DAY, 17))
    ts_id = _punch(w, w.riley).json()["timesheet_id"]
    clock(_at(DAY, 22))
    _punch(w, w.riley, "out")
    clock(_at(DAY + timedelta(days=2), 9))
    wrong_day = _utc(_at(DAY + timedelta(days=1), 23, 30)).isoformat()
    r = _review(w, ts_id, approve=True, clock_out=wrong_day, note="finished 11:30")
    assert r.status_code == 422 and "24 hours" in _msg(r)
    assert get_db().get_timesheet(ts_id)["status"] == "closed"

    ci = _utc(_at(DAY, 9))
    _sheet(w.vid, "ts-long", w.sam, ci, ci + timedelta(hours=30))
    out = w.c.post("/api/clock/timesheets/approve", headers=w.mgr,
                   json={"venue_id": w.vid, "ids": ["ts-long"]}).json()
    assert out["approved"] == 0 and "24 hours" in out["skipped"][0]["reason"]


def test_payroll_refuses_a_timesheet_of_a_day_or_more_instead_of_wrapping_it():
    w = _world()
    ci = _utc(_at(DAY, 17))
    _sheet(w.vid, "ts-wrap", w.jo, ci, ci + timedelta(hours=30), status="approved")
    r = _prepare_actuals(w, DAY, DAY)
    assert r.status_code == 409
    assert "24 hours" in _msg(r) and "Jo Bar" in _msg(r) and "30h 00m" in _msg(r)


# ---------------------------------------------------------------------------
# [25] 'Needs a look' totals only the rows it lists
# ---------------------------------------------------------------------------

def test_needs_a_look_totals_only_the_rows_it_lists():
    w = _world()
    ci = _utc(_at(DAY, 10))
    _sheet(w.vid, "ts-look", w.riley, ci, ci + timedelta(hours=7, minutes=20), pin_verified=False)
    _sheet(w.vid, "ts-clean", w.sam, ci, ci + timedelta(hours=7))
    _sheet(w.vid, "ts-done", w.jo, ci, ci + timedelta(hours=8), status="approved")
    look = _rows(w, status="review")
    assert [t["id"] for t in look["timesheets"]] == ["ts-look"]
    assert look["total_worked_minutes"] == look["timesheets"][0]["worked_minutes"] == 410
    assert _rows(w)["total_worked_minutes"] == 410 + 390 + 450


# ---------------------------------------------------------------------------
# [18] Salaried: leavers, unpaid leave, and a salary at both venues
# ---------------------------------------------------------------------------

SAL = {"employment_type": "full_time", "hourly_base_rate": "40.00", "pay_basis": "salaried",
       "weekly_salary": "1500.00"}


def test_a_salaried_leaver_and_unpaid_leave_are_noted_for_the_advisor_to_prorate():
    w = _world()
    db = get_db()
    start, end = date(2026, 9, 28), date(2026, 10, 11)
    mia = _employee(w.c, w.mgr, w.vid, f"{w.vid}-mia", "Mia Manager", **SAL)
    una = _employee(w.c, w.mgr, w.vid, f"{w.vid}-una", "Una Unpaid", **SAL)
    lee = _employee(w.c, w.mgr, w.vid, f"{w.vid}-lee", "Lee Longgone", **SAL)
    gone = db.get_employee(lee).model_copy(deep=True)
    gone.active = False                     # left in July: updated_at stays 1 Jul
    db.save_employee(gone)
    assert w.c.post(f"/employees/{mia}/deactivate", headers=w.mgr).status_code == 200
    db.save_leave_request({"id": f"lv-{uuid.uuid4().hex[:6]}", "venue_id": w.vid, "employee_id": una,
                           "start_date": date(2026, 9, 28), "end_date": date(2026, 10, 4),
                           "leave_type": "unpaid", "status": "approved", "paid_hours": None,
                           "created_at": datetime(2026, 9, 20)})
    ci = _utc(_at(start, 10))
    _sheet(w.vid, "ts-cas", w.riley, ci, ci + timedelta(hours=6), status="approved", work_date=start)

    batch = _batch(_prepare_actuals(w, start, end))
    lines = {e["employee_id"]: e for e in batch["employees"]}
    assert lines[mia]["salary_amount"] == "3000.00"           # her last fortnight is not dropped
    assert lines[una]["salary_amount"] == "3000.00"           # never auto-prorated
    assert lee not in lines
    notes = batch["validation_errors"]
    assert any(n.startswith("Mia Manager (salaried) is no longer active") and "prorate" in n
               for n in notes), notes
    assert any("Una Unpaid (salaried) has approved unpaid leave Mon 28 Sep to Sun 04 Oct "
               "(7 days in this period)" in n for n in notes), notes
    assert not any("Lee" in n for n in notes)
    text = w.c.get(f"/api/payroll/batch/{batch['batch_id']}/csv?venue_id={w.vid}", headers=w.mgr).text
    assert "Note,Mia Manager (salaried) is no longer active" in text

    # The rostered basis says the same
    _roster(w.vid, [("sh-mia", mia, start, dtime(9), dtime(17), 30)], week=start)
    r = w.c.post("/api/payroll/prepare", json={"venue_id": w.vid, "period_start": str(start),
                                               "period_end": str(end), "state": "wa"}, headers=w.mgr)
    assert r.status_code == 200, r.text
    assert any(n.startswith("Mia Manager (salaried) is no longer active")
               for n in r.json()["validation_errors"])


def test_one_person_salaried_at_both_venues_is_flagged_and_strangers_stay_out():
    w = _world()
    apo = f"apo-{uuid.uuid4().hex[:6]}"
    _venue(w.c, w.mgr, apo, "Apothecary")
    mgr = _login(w.c, w.mgr_email)
    gina = f"gina{uuid.uuid4().hex[:6]}@x.com"
    _employee(w.c, mgr, w.vid, f"{w.vid}-gina", "Gina GM", email=gina, **SAL)
    _employee(w.c, mgr, apo, f"{apo}-gina", "Gina GM", email=gina.upper(), **SAL)
    # Another owner's pub with a record under the same email is never read
    other_email = f"g6o{uuid.uuid4().hex[:8]}@x.com"
    other = _login(w.c, other_email)
    elsewhere = f"oth-{uuid.uuid4().hex[:6]}"
    _venue(w.c, other, elsewhere, "Somebody Else's Bar")
    other = _login(w.c, other_email)
    _employee(w.c, other, elsewhere, f"{elsewhere}-gina", "Gina GM", email=gina, **SAL)

    ci = _utc(_at(DAY, 10))
    _sheet(w.vid, "ts-cas", w.riley, ci, ci + timedelta(hours=6), status="approved")
    r = w.c.post("/api/payroll/prepare-actuals", headers=mgr, json={
        "venue_id": w.vid, "period_start": str(DAY), "period_end": str(DAY), "state": "wa"})
    errors = r.json()["validation_errors"]
    flagged = [e for e in errors if e.startswith("Gina GM is also salaried at")]
    assert flagged and "Apothecary" in flagged[0], errors
    assert "Somebody Else" not in " ".join(errors)


@contextmanager
def _deactivated_on(when):
    """Run the deactivate route as if it were ``when`` (naive UTC)."""
    from rosteriq.routes import staff_records as staff_records_routes
    from rosteriq.services import events as events_module

    class _Then(datetime):
        @classmethod
        def utcnow(cls):
            return when

    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(staff_records_routes, "datetime", _Then)
        mp.setattr(events_module, "datetime", _Then)
        yield


def test_a_salaried_leaver_from_months_ago_stays_out_after_their_record_is_saved_again():
    w = _world()
    db = get_db()
    start, end = date(2026, 9, 28), date(2026, 10, 11)
    lee = _employee(w.c, w.mgr, w.vid, f"{w.vid}-lee", "Lee Longgone", **SAL)
    mia = _employee(w.c, w.mgr, w.vid, f"{w.vid}-mia", "Mia Manager", **SAL)
    with _deactivated_on(datetime(2026, 7, 15, 3)):
        assert w.c.post(f"/employees/{lee}/deactivate", headers=w.mgr).status_code == 200
    with _deactivated_on(datetime(2026, 10, 8, 3)):
        assert w.c.post(f"/employees/{mia}/deactivate", headers=w.mgr).status_code == 200
    # Saving Lee's record again (a phone number here; a re-import, an integration
    # re-sync or anonymising on Postgres do the same) stamps updated_at now.
    r = w.c.put(f"/employees/{lee}", json={"phone": "0400 000 000"}, headers=w.mgr)
    assert r.status_code == 200, r.text
    assert db.get_employee(lee).updated_at.date() >= start
    ci = _utc(_at(start, 10))
    _sheet(w.vid, f"ts-{uuid.uuid4().hex[:6]}", w.riley, ci, ci + timedelta(hours=6),
           status="approved", work_date=start)

    batch = _batch(_prepare_actuals(w, start, end))
    lines = {e["employee_id"]: e for e in batch["employees"]}
    assert lee not in lines and lines[mia]["salary_amount"] == "3000.00"
    assert Decimal(batch["total_gross"]) < Decimal("3500")
    notes = batch["validation_errors"]
    assert not any("Lee" in n for n in notes), notes
    assert any(n.startswith("Mia Manager (salaried) is no longer active (marked as left on Thu 08 Oct")
               for n in notes), notes


def test_the_leaving_day_is_the_last_deactivation_unless_reactivated_since(monkeypatch):
    from rosteriq.services import clock as venue_clock
    from rosteriq.services.payroll_export import PayrollExporter
    monkeypatch.setattr(venue_clock, "venue_timezone", lambda venue_id, db=None: "Australia/Perth")
    db = MemoryStore()
    vid = f"wem-{uuid.uuid4().hex[:6]}"
    emp = SimpleNamespace(id="e1", venue_id=vid, active=False, updated_at=datetime(2026, 10, 1, 4))

    def event(action, when, resource_id="e1"):
        db.save_audit_log({"venue_id": vid, "action": action, "resource_type": "employee",
                           "resource_id": resource_id, "details": {"category": "audit"},
                           "created_at": when})

    event("employee.deactivate", datetime(2026, 3, 1, 1))
    # (back by a whole-record save, which logs no reactivation)
    event("employee.deactivate", datetime(2026, 7, 14, 17))   # 15 Jul, 01:00 in Perth
    event("privacy.anonymise", datetime(2026, 9, 20))          # details removed later
    event("employee.deactivate", datetime(2026, 9, 1), resource_id="e2")
    event("privacy.anonymise", datetime(2026, 8, 20, 3), resource_id="e3")   # while still active
    assert PayrollExporter(db).left_on(emp) == date(2026, 7, 15)
    e3 = SimpleNamespace(id="e3", venue_id=vid, active=False, updated_at=datetime(2026, 10, 1, 4))
    assert PayrollExporter(db).left_on(e3) == date(2026, 8, 20)
    # Back, then inactive again by a path that records no event: the record's last save
    event("employee.reactivate", datetime(2026, 9, 25))
    assert PayrollExporter(db).marked_left_on(emp) is None
    assert PayrollExporter(db).left_on(emp) == date(2026, 10, 1)
    assert PayrollExporter(None).left_on(emp) == date(2026, 10, 1)


def test_a_salary_at_the_other_venue_is_flagged_only_when_that_venue_would_pay_it():
    w = _world()
    apo = f"apo-{uuid.uuid4().hex[:6]}"
    _venue(w.c, w.mgr, apo, "Apothecary")
    mgr = _login(w.c, w.mgr_email)
    gina = f"gina{uuid.uuid4().hex[:6]}@x.com"
    _employee(w.c, mgr, w.vid, f"{w.vid}-gina", "Gina GM", email=gina, **SAL)
    old = _employee(w.c, mgr, apo, f"{apo}-gina", "Gina GM", email=gina, **SAL)
    with _deactivated_on(datetime(2026, 7, 15, 3)):          # left Apothecary in July
        assert w.c.post(f"/employees/{old}/deactivate", headers=mgr).status_code == 200
    ci = _utc(_at(DAY, 10))
    _sheet(w.vid, f"ts-{uuid.uuid4().hex[:6]}", w.riley, ci, ci + timedelta(hours=6), status="approved")

    def flagged():
        r = w.c.post("/api/payroll/prepare-actuals", headers=mgr, json={
            "venue_id": w.vid, "period_start": str(DAY), "period_end": str(DAY), "state": "wa"})
        assert r.status_code == 200, r.text
        return [e for e in r.json()["validation_errors"] if e.startswith("Gina GM is also salaried")]

    assert flagged() == []
    # She covered a shift at Apothecary this period after all: its run pays her too
    _sheet(apo, f"ts-{uuid.uuid4().hex[:6]}", old, ci, ci + timedelta(hours=4), status="closed")
    assert ["Apothecary" in e for e in flagged()] == [True]


# ---------------------------------------------------------------------------
# [17] A rostered day on paid leave is a leave line, never also a priced shift
# ---------------------------------------------------------------------------

def test_rostered_prep_never_prices_a_day_already_on_paid_leave():
    w = _world()
    saturday = date(2026, 10, 17)
    _roster(w.vid, [("sh-jo", w.jo, saturday, dtime(16), dtime(23), 30),
                    ("sh-sam", w.sam, saturday, dtime(16), dtime(23), 30)])
    get_db().save_leave_request({
        "id": f"lv-{uuid.uuid4().hex[:6]}", "venue_id": w.vid, "employee_id": w.jo,
        "start_date": saturday, "end_date": saturday, "leave_type": "personal",
        "status": "approved", "paid_hours": 6.5, "created_at": datetime(2026, 10, 19)})
    r = w.c.post("/api/payroll/prepare", json={"venue_id": w.vid, "period_start": str(WEEK),
                                               "period_end": str(saturday), "state": "wa"},
                 headers=w.mgr)
    batch = _batch(r)
    lines = {e["employee_id"]: e for e in batch["employees"]}
    jo = lines[w.jo]
    assert jo["penalty_entries"] == [] and Decimal(jo["total_gross"]) == 0
    assert [(le["leave_type"], le["hours"]) for le in jo["leave_entries"]] == [("personal", "6.50")]
    assert [p["penalty_type"] for p in lines[w.sam]["penalty_entries"]] == ["saturday"]
    assert any(n.startswith("Jo Bar: rostered Sat 17 Oct 16:00-23:00 not priced") and "leave" in n
               for n in r.json()["validation_errors"])
