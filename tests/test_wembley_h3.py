"""The Wembley's time and pay, last round.

A 30-minute meal break the kiosk stores as 29 is clean; a meal break the
system presumed comes off when a correction makes the shift too short to need
one, from the API and from the Edit modal; time worked on a day of approved
paid leave needs a look and is noted for the advisor; leave recorded after a
punch was auto-closed counts the shift as leave and says why; a salaried
person's paid leave is shown as inside their salary; a correction that moves
a shift into a pay run already prepared says to prepare it again; a rate
change dated inside the shifts a run prices is noted; and the rostered run
prices the worked part of a part-worked leave day, reckoned across the whole
leave so a multi-day or straddling leave neither drops nor doubles hours.

Time-dependent tests freeze the time clock's 'now' at a Perth wall time.
"""

import csv
import io
import json
import re
import uuid
from datetime import date, datetime, time as dtime, timedelta, timezone
from decimal import Decimal

import pytest

from rosteriq.award_rules import get_minimum_break_minutes
from rosteriq.database import get_db
from rosteriq.routes import timeclock as timeclock_routes
from rosteriq.services.staff_records import rate_change_entry
from rosteriq.tests.test_wembley_a6 import _main, _run, needs_node
from rosteriq.tests.test_wembley_g6 import (
    NEXT, PERTH, SAL, WEEK, _at, _batch, _employee, _prepare_actuals, _punch, _review,
    _roster, _rows, _sheet, _utc, _world,
)


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


def _approve(w, ids, only_clean=False):
    r = w.c.post("/api/clock/timesheets/approve", headers=w.mgr,
                 json={"venue_id": w.vid, "ids": list(ids), "only_clean": only_clean})
    assert r.status_code == 200, r.text
    return r.json()


def _leave(vid, emp, start, end, leave_type="personal", paid_hours=None):
    get_db().save_leave_request({
        "id": f"lv-{uuid.uuid4().hex[:6]}", "venue_id": vid, "employee_id": emp,
        "start_date": start, "end_date": end, "leave_type": leave_type, "status": "approved",
        "paid_hours": paid_hours, "created_at": datetime(2026, 10, 1)})


def _hours_by_type(line):
    out = {p["penalty_type"]: p["hours"] for p in line["penalty_entries"]}
    if Decimal(line["ordinary_hours"]):
        out["ordinary"] = line["ordinary_hours"]
    return out


# ---------------------------------------------------------------------------
# [2] A 30-minute meal break stored as 29 is clean
# ---------------------------------------------------------------------------

def test_a_thirty_minute_break_stored_as_29_is_clean_and_approved_in_bulk(clock):
    w = _world()
    _roster(w.vid, [("sh-r", w.riley, NEXT, dtime(15), dtime(23), 30),
                    ("sh-s", w.sam, NEXT, dtime(15), dtime(23), 30)])
    ids = {}
    for at, emp, action in ((_at(NEXT, 15), w.riley, "in"), (_at(NEXT, 15), w.sam, "in"),
                            (_at(NEXT, 19), w.sam, "break/start"),
                            (_at(NEXT, 19, 0, 40), w.riley, "break/start"),
                            (_at(NEXT, 19, 28), w.sam, "break/end"),            # 28m00s
                            (_at(NEXT, 19, 30, 39), w.riley, "break/end"),      # 29m59s
                            (_at(NEXT, 23), w.riley, "out"), (_at(NEXT, 23), w.sam, "out")):
        clock(at)
        r = _punch(w, emp, action)
        assert r.status_code == 200, (action, r.text)
        ids.setdefault(emp, r.json().get("timesheet_id"))
    rows = {t["employee_id"]: t for t in _rows(w, NEXT)["timesheets"]}
    riley, sam = rows[w.riley], rows[w.sam]
    assert riley["break_minutes"] == 29                     # stored truncated, pay unchanged
    assert riley["worked_minutes"] == 8 * 60 - 29
    assert riley["clean"] is True and riley["review_reasons"] == []
    minimum = get_minimum_break_minutes(8)
    assert sam["clean"] is False
    assert f"28 min break, under the {minimum} min meal break" in sam["review_reasons"]

    out = _approve(w, [ids[w.riley], ids[w.sam]], only_clean=True)
    assert out["approved_ids"] == [ids[w.riley]]
    assert out["skipped"][0]["reason"] == f"28 min break, under the {minimum} min meal break"


# ---------------------------------------------------------------------------
# [10] A presumed meal break comes off when the shift is cut too short for one
# ---------------------------------------------------------------------------

def test_cutting_an_auto_closed_punch_short_drops_the_meal_break_no_longer_due(clock):
    w = _world()
    _roster(w.vid, [(f"sh-{n}", emp, NEXT, dtime(17), dtime(23), 30)
                    for n, emp in (("r", w.riley), ("s", w.sam), ("j", w.jo))])
    clock(_at(NEXT, 17))
    ids = {emp: _punch(w, emp).json()["timesheet_id"] for emp in (w.riley, w.sam, w.jo)}
    clock(_at(NEXT, 18))
    _punch(w, w.jo, "break/start")
    clock(_at(NEXT, 18, 20))
    _punch(w, w.jo, "break/end")                            # Jo recorded a 20 min break
    clock(_at(NEXT + timedelta(days=1), 8))                 # the board's poll closes all three
    assert w.c.get(f"/api/clock/board?venue_id={w.vid}", headers=w.k).status_code == 200
    db = get_db()
    closed = db.get_timesheet(ids[w.riley])
    assert (closed["break_source"], closed["break_minutes"]) == ("auto", 30)
    assert "auto_closed" in closed["flags"]

    sent_home = _utc(_at(NEXT, 21)).isoformat()
    out = _review(w, ids[w.riley], clock_out=sent_home, note="sent home at 9, forgot to clock out").json()
    assert (out["status"], out["worked_minutes"], out["break_minutes"], out["break_source"]) == \
        ("approved", 240, 0, None)
    assert not {"auto_break", "no_break"} & set(out["flags"])
    assert db.get_timesheet(ids[w.riley])["break_minutes"] == 0

    # Still long enough to need a meal break: the rostered one stays
    later = _review(w, ids[w.sam], clock_out=_utc(_at(NEXT, 22, 30)).isoformat(),
                    note="sent home at 10:30").json()
    assert (later["worked_minutes"], later["break_minutes"], later["break_source"]) == (300, 30, "auto")
    assert "auto_break" in later["flags"]
    # A break she recorded is hers, whatever the finish
    jo = _review(w, ids[w.jo], clock_out=sent_home, note="sent home at 9").json()
    assert (jo["worked_minutes"], jo["break_minutes"], jo["break_source"]) == (220, 20, "recorded")


@pytest.mark.parametrize("auto", [True, False])
def test_closing_an_open_punch_short_with_the_rostered_break_left_as_shown_deducts_none(clock, auto):
    w = _world()
    _roster(w.vid, [(f"sh-{n}", emp, NEXT, dtime(17), dtime(23), 30)
                    for n, emp in (("r", w.riley), ("s", w.sam), ("j", w.jo))])
    r = w.c.put("/api/kiosk/settings", json={"venue_id": w.vid, "auto_meal_break": auto}, headers=w.mgr)
    assert r.status_code == 200, r.text
    clock(_at(NEXT, 17))
    ids = {emp: _punch(w, emp).json()["timesheet_id"] for emp in (w.riley, w.sam, w.jo)}
    clock(_at(NEXT, 23, 30))

    short = _review(w, ids[w.riley], clock_out=_utc(_at(NEXT, 20)).isoformat(), break_minutes=30,
                    break_from_roster=True, note="sent home at 8").json()
    assert (short["worked_minutes"], short["break_minutes"], short["break_source"]) == (180, 0, None)
    assert short["status"] == "closed" and "no_break" not in short["flags"]
    full = _review(w, ids[w.sam], clock_out=_utc(_at(NEXT, 23)).isoformat(), break_minutes=30,
                   break_from_roster=True, note="forgot to clock out").json()
    assert (full["worked_minutes"], full["break_minutes"], full["break_source"]) == \
        (330, 30, "auto" if auto else "manager")
    # A break the manager typed is what they say was taken, however short the shift
    typed = _review(w, ids[w.jo], clock_out=_utc(_at(NEXT, 20)).isoformat(), break_minutes=30,
                    note="took her dinner before going home").json()
    assert (typed["worked_minutes"], typed["break_minutes"], typed["break_source"]) == (150, 30, "manager")


def _modal_js():
    src = _main("dashboard.html")
    fns = "\n".join(src[m.start():src.index("\n}\n", m.start()) + 2] for m in (
        re.search(r"^function tsMinutesLabel\(", src, re.M),
        re.search(r"^function openEditTimesheet\(", src, re.M)))
    start = src.index("document.getElementById('ets-save').addEventListener(")
    listener = src[start:src.index("\n});\n", start) + 5]
    return fns + "\nlet etsIndex = null;\nlet etsShown = null;\n" + listener


@needs_node
def test_the_edit_modal_closing_a_punch_short_saves_no_meal_break(clock):
    w = _world()
    _roster(w.vid, [("sh-r", w.riley, NEXT, dtime(17), dtime(23), 30),
                    ("sh-s", w.sam, NEXT, dtime(17), dtime(23), 30)])
    clock(_at(NEXT, 17))
    ids = {emp: _punch(w, emp).json()["timesheet_id"] for emp in (w.riley, w.sam)}
    clock(_at(NEXT, 23, 30))
    rows = sorted(_rows(w, NEXT, status="open")["timesheets"], key=lambda t: t["employee_id"] != w.riley)
    finish = _utc(_at(NEXT, 20)).strftime("%Y-%m-%dT%H:%M")       # node runs in UTC
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
var tsRows = {json.dumps(rows)};
""" + _modal_js() + f"""
async function main() {{
  const shown = [];
  openEditTimesheet(0);
  shown.push(String(el('ets-break').value));
  el('ets-out').value = {json.dumps(finish)}; el('ets-note').value = 'sent home at 8';
  await el('ets-save').fire('click');
  openEditTimesheet(1);
  shown.push(String(el('ets-break').value));
  el('ets-out').value = {json.dumps(finish)}; el('ets-break').value = '15';
  el('ets-note').value = 'short dinner, then home';
  await el('ets-save').fire('click');
  console.log(JSON.stringify({{ shown: shown, bodies: posted }}));
}}
""", env={"TZ": "UTC"})
    assert data["shown"] == ["30", "30"]
    untouched, typed = data["bodies"]
    assert untouched["break_minutes"] == 30 and untouched["break_from_roster"] is True
    assert typed["break_minutes"] == 15 and "break_from_roster" not in typed
    saved = w.c.post(f"/api/clock/timesheets/{ids[w.riley]}/review", json=untouched, headers=w.mgr).json()
    assert (saved["worked_minutes"], saved["break_minutes"], saved["break_source"]) == (180, 0, None)
    kept = w.c.post(f"/api/clock/timesheets/{ids[w.sam]}/review", json=typed, headers=w.mgr).json()
    assert (kept["worked_minutes"], kept["break_minutes"], kept["break_source"]) == (165, 15, "manager")


# ---------------------------------------------------------------------------
# [11] Work on a day of approved paid leave
# ---------------------------------------------------------------------------

def test_work_on_a_day_of_approved_paid_leave_needs_a_look_and_is_noted_for_the_advisor():
    w = _world()
    thu, sat = date(2026, 10, 15), date(2026, 10, 17)
    _leave(w.vid, w.jo, thu, sat, paid_hours=19.5)
    ci = _utc(_at(sat, 16))
    _sheet(w.vid, "ts-jo", w.jo, ci, ci + timedelta(hours=7), work_date=sat)
    _sheet(w.vid, "ts-sam", w.sam, ci, ci + timedelta(hours=7), work_date=sat)
    rows = {t["id"]: t for t in _rows(w, sat)["timesheets"]}
    assert rows["ts-sam"]["clean"] is True
    assert rows["ts-jo"]["clean"] is False
    assert "on approved personal/carer's leave that day" in rows["ts-jo"]["review_reasons"]
    bulk = _approve(w, ["ts-jo", "ts-sam"], only_clean=True)
    assert bulk["approved_ids"] == ["ts-sam"]
    assert bulk["skipped"][0]["reason"] == "on approved personal/carer's leave that day"

    _approve(w, ["ts-jo"])
    r = _prepare_actuals(w, WEEK, WEEK + timedelta(days=6))
    jo = {e["employee_id"]: e for e in _batch(r)["employees"]}[w.jo]
    assert _hours_by_type(jo) == {"saturday": "6.50"}         # the worked time is still priced
    assert [(le["leave_type"], le["hours"]) for le in jo["leave_entries"]] == [("personal", "19.50")]
    assert ("Jo Bar: worked Sat 17 Oct 6.5h during approved personal/carer's leave Thu 15 Oct "
            "to Sat 17 Oct (19.5h recorded) — check the leave hours don't also cover that day"
            ) in r.json()["validation_errors"]
    assert not any(n.startswith("Sam Sportsbar") for n in r.json()["validation_errors"])


# ---------------------------------------------------------------------------
# [13] Leave recorded after the punch was auto-closed
# ---------------------------------------------------------------------------

def test_leave_after_a_punch_was_auto_closed_counts_the_shift_as_leave_and_says_why(clock, monkeypatch):
    from rosteriq.services import leave_shifts
    w = _world()
    sat, sun, mon = date(2026, 10, 17), date(2026, 10, 18), date(2026, 10, 19)
    _roster(w.vid, [("sh-jo", w.jo, sat, dtime(16), dtime(23), 30),
                    ("sh-sam", w.sam, sat, dtime(16), dtime(23), 30)])
    clock(_at(sat, 16))
    jo = _punch(w, w.jo).json()["timesheet_id"]           # goes home sick at 6, never clocks out
    sam = _punch(w, w.sam).json()["timesheet_id"]         # forgets to clock out at 11
    clock(_at(sun, 7))
    assert w.c.get(f"/api/clock/board?venue_id={w.vid}", headers=w.k).status_code == 200
    db = get_db()
    assert "auto_closed" in db.get_timesheet(jo)["flags"]
    monkeypatch.setattr(leave_shifts, "venue_now", lambda *a, **k: _at(mon, 9).replace(tzinfo=PERTH))
    monkeypatch.setattr(leave_shifts, "_venue_now", lambda *a, **k: _at(mon, 9))

    def impact(emp):
        r = w.c.get(f"/api/leave/impact?venue_id={w.vid}&employee_id={emp}&start_date={sat}"
                    f"&end_date={sat}&leave_type=personal", headers=w.mgr)
        assert r.status_code == 200, r.text
        return r.json()

    imp = impact(w.jo)
    assert imp["paid_hours_default"] == 6.5
    assert ("their punch that day was closed automatically at the rostered end, so it isn't "
            "counted and its 6.5h counts as leave: check the hours") in imp["message"]
    r = w.c.post("/api/leave", json={"venue_id": w.vid, "employee_id": w.jo, "start_date": str(sat),
                                     "end_date": str(sat), "leave_type": "personal"}, headers=w.mgr)
    assert r.status_code == 200 and r.json()["paid_hours"] == 6.5, r.text

    # The finish a manager put right, or a punch they signed off as it was, is time worked
    assert _review(w, jo, approve=False, clock_out=_utc(_at(sat, 18)).isoformat(),
                   note="went home sick at 6").status_code == 200
    imp = impact(w.jo)
    assert imp["paid_hours_default"] == 4.5
    assert "they worked 2h that day, so 4.5h of it counts as leave" in imp["message"]
    assert _review(w, sam).json()["status"] == "approved"
    imp = impact(w.sam)
    assert imp["paid_hours_default"] is None
    assert "they worked 6.5h that day, so 0h of it counts as leave" in imp["message"]


# ---------------------------------------------------------------------------
# [14] A salaried person's paid leave is inside their salary
# ---------------------------------------------------------------------------

def test_a_salaried_managers_paid_leave_is_shown_inside_the_salary_not_to_price():
    w = _world()
    mon = date(2026, 10, 12)
    gina = _employee(w.c, w.mgr, w.vid, f"{w.vid}-gina", "Gina GM", **SAL)
    _leave(w.vid, gina, mon, mon + timedelta(days=4), leave_type="annual", paid_hours=37.5)
    _leave(w.vid, w.jo, mon, mon, leave_type="annual", paid_hours=7.6)
    ci = _utc(_at(mon, 10))
    _sheet(w.vid, "ts-r", w.riley, ci, ci + timedelta(hours=6), status="approved", work_date=mon)

    r = _prepare_actuals(w, mon, mon + timedelta(days=6))
    batch = _batch(r)
    lines = {e["employee_id"]: e for e in batch["employees"]}
    line = lines[gina]
    assert line["salary_amount"] == "1500.00" and line["total_gross"] == "1500.00"
    entry = line["leave_entries"][0]
    assert (entry["leave_type"], entry["hours"], entry["amount"]) == ("annual", "37.50", "0.00")
    assert "already paid within the salary line" in entry["note"]
    assert "advisor to price" not in entry["note"]
    assert "advisor to price" in lines[w.jo]["leave_entries"][0]["note"]     # hourly: unchanged
    assert ("Gina GM (salaried) has approved annual leave Mon 12 Oct to Fri 16 Oct (37.5h) — the "
            "salary line already pays those days; record the hours against their leave balance, "
            "don't price them on top") in r.json()["validation_errors"]

    text = w.c.get(f"/api/payroll/batch/{batch['batch_id']}/csv?venue_id={w.vid}", headers=w.mgr).text
    table = list(csv.reader(io.StringIO(text)))
    header = table[2]
    priced = header.index("Paid leave hours (advisor to price)")
    within = header.index("Leave within salary (not to price)")
    by_name = {row[0]: row for row in table[3:] if row}
    assert by_name["Gina GM"][priced] == "" and by_name["Gina GM"][within].startswith("annual 37.50h")
    assert by_name["Jo Bar"][priced].startswith("annual 7.60h") and by_name["Jo Bar"][within] == ""
    totals = by_name["TOTALS"]
    assert len(totals) == len(header)


# ---------------------------------------------------------------------------
# [15] A correction that moves a shift into a pay run already prepared
# ---------------------------------------------------------------------------

def test_a_midnight_correction_into_a_prepared_pay_run_says_to_prepare_it_again(clock):
    w = _world()
    p1, sun, mon = date(2026, 10, 5), date(2026, 10, 18), date(2026, 10, 19)
    p2_end = mon + timedelta(days=13)
    _roster(w.vid, [("sh-dan", w.riley, sun, dtime(18), dtime(2), 30),
                    ("sh-jo", w.jo, sun, dtime(18), dtime(2), 30),
                    ("sh-amy", w.sam, sun, dtime(10), dtime(16), 30)], week=date(2026, 10, 12))
    for at, emp, action in ((_at(sun, 10), w.sam, "in"), (_at(sun, 16), w.sam, "out"),
                            (_at(mon, 0, 20), w.riley, "in"), (_at(mon, 0, 20), w.jo, "in"),
                            (_at(mon, 2), w.riley, "out"), (_at(mon, 2), w.jo, "out"),
                            (_at(mon, 10), w.sam, "in"), (_at(mon, 16), w.sam, "out")):
        clock(at)
        assert _punch(w, emp, action).status_code == 200, action
    db = get_db()
    late = {t["employee_id"]: t["id"] for t in db.get_timesheets(w.vid, mon, mon)
            if t["employee_id"] in (w.riley, w.jo)}
    # Corrected before the run for that fortnight is prepared: nothing to warn about
    early = _review(w, late[w.jo], clock_in=_utc(_at(sun, 18)).isoformat(), note="started at 6")
    assert early.status_code == 200 and early.json()["work_date"] == "2026-10-18"
    assert early.json().get("warning") is None
    _approve(w, [t["id"] for t in db.get_timesheets(w.vid, p1, sun)])
    first = _prepare_actuals(w, p1, sun)
    assert first.status_code == 200 and first.json()["employee_count"] == 2, first.text

    clock(_at(mon + timedelta(days=1), 10))
    r = _review(w, late[w.riley], clock_in=_utc(_at(sun, 18)).isoformat(),
                note="kiosk missed his 6pm clock-in")
    assert r.status_code == 200, r.text
    assert r.json()["work_date"] == "2026-10-18"
    assert r.json()["warning"] == (
        "This moves Riley Restaurant's shift to Sun 18 Oct, in the Mon 05 Oct to Sun 18 Oct pay "
        "run already prepared without it — prepare that range again so it's included.")
    _approve(w, [t["id"] for t in db.get_timesheets(w.vid, mon, mon)])

    note = ("Riley Restaurant: the approved shift on Sun 18 Oct isn't in the Mon 05 Oct to Sun 18 Oct "
            "pay run, which was prepared before it was approved — prepare that range again")
    nxt = _prepare_actuals(w, mon, p2_end)
    assert nxt.status_code == 200, nxt.text
    errors = nxt.json()["validation_errors"]
    assert note in errors
    assert not any(e.startswith("Jo Bar") for e in errors)      # hers is in that run
    # Prepared again, the earlier run holds it and the next one stops saying so
    again = _prepare_actuals(w, p1, sun)
    assert again.json()["employee_count"] == 3
    assert note not in _prepare_actuals(w, mon, p2_end).json()["validation_errors"]


@needs_node
def test_the_edit_modal_shows_the_warning_a_correction_comes_back_with():
    data = _run(r"""
global.state = { currentVenue: 'wem' };
global.fetchAPI = async function () {
  return { status: 'approved', worked_minutes: 450, break_minutes: 30,
           warning: 'This moves Dan\'s shift to Sun 18 Oct, in the pay run already prepared.' };
};
global.openModal = function () {};
global.closeModal = function () {};
global.loadTimesheets = function () {};
var tsRows = [{ id: 't1', employee_name: 'Dan', work_date: '2026-10-19', clock_in: '2026-10-18T16:20:00',
  clock_out: '2026-10-18T18:00:00', status: 'closed', break_minutes: 30, rostered: null }];
""" + _modal_js() + r"""
async function main() {
  openEditTimesheet(0);
  el('ets-in').value = '2026-10-18T10:00'; el('ets-note').value = 'started at 6';
  await el('ets-save').fire('click');
  console.log(JSON.stringify({ toasts: toasts }));
}
""", env={"TZ": "UTC"})
    kinds = [(t["k"], t["m"]) for t in data["toasts"]]
    assert kinds[0][0] == "success"
    assert ("warning", "This moves Dan's shift to Sun 18 Oct, in the pay run already prepared.") in kinds


# ---------------------------------------------------------------------------
# [16] A rate change dated inside the shifts a run prices
# ---------------------------------------------------------------------------

def test_the_pay_run_notes_shifts_priced_at_a_rate_that_started_after_them():
    w = _world()
    db = get_db()
    kai = _employee(w.c, w.mgr, w.vid, f"{w.vid}-kai", "Kai Junior", hourly_base_rate="16.50")
    mon, fri = date(2026, 9, 28), date(2026, 10, 2)
    for day in (mon, fri):
        ci = _utc(_at(day, 10))
        _sheet(w.vid, f"ts-kai-{day}", kai, ci, ci + timedelta(hours=4), break_minutes=0,
               status="approved", work_date=day)
    emp = db.get_employee(kai).model_copy(deep=True)
    emp.hourly_base_rate = Decimal("19.80")
    emp.rate_history = [rate_change_entry(Decimal("16.50"), Decimal("19.80"), date(2026, 9, 30), "mgr")]
    db.save_employee(emp)

    r = _prepare_actuals(w, mon, mon + timedelta(days=6))
    line = {e["employee_id"]: e for e in _batch(r)["employees"]}[kai]
    assert line["ordinary_rate"] == "19.80"                  # priced at the current rate, as before
    assert ("Kai Junior: rate changed $16.50 → $19.80 from Wed 30 Sep; 1 shift before then "
            "(Mon 28 Sep) is priced here at the current $19.80 — advisor to pay it at the rate "
            "in effect that day") in r.json()["validation_errors"]

    # Saved after the period with no backdate: every shift in it was before the change
    emp.hourly_base_rate = Decimal("21.00")
    emp.rate_history = emp.rate_history + [
        rate_change_entry(Decimal("19.80"), Decimal("21.00"), date(2026, 10, 6), "mgr")]
    db.save_employee(emp)
    errors = _prepare_actuals(w, mon, mon + timedelta(days=6)).json()["validation_errors"]
    assert ("Kai Junior: rate changed $19.80 → $21.00 from Tue 06 Oct; 2 shifts before then "
            "(Mon 28 Sep, Fri 02 Oct) are priced here at the current $21.00 — advisor to pay them "
            "at the rate in effect that day") in errors
    assert any(e.startswith("Kai Junior: rate changed $16.50 → $19.80") for e in errors)
    # Changes from before every shift in the run say nothing
    later = mon + timedelta(days=14)
    ci = _utc(_at(later, 10))
    _sheet(w.vid, "ts-kai-later", kai, ci, ci + timedelta(hours=4), break_minutes=0,
           status="approved", work_date=later)
    r = _prepare_actuals(w, later, later + timedelta(days=6))
    assert r.status_code == 200, r.text
    assert not any("rate changed" in e for e in r.json()["validation_errors"])


# ---------------------------------------------------------------------------
# [17] The rostered run on a part-worked leave day
# ---------------------------------------------------------------------------

def test_rostered_prep_prices_the_worked_part_of_a_part_worked_leave_day():
    w = _world()
    fri, sat = date(2026, 10, 16), date(2026, 10, 17)
    _roster(w.vid, [("sh-jo", w.jo, sat, dtime(16), dtime(23), 30),
                    ("sh-sam-f", w.sam, fri, dtime(16), dtime(23), 30),
                    ("sh-sam-s", w.sam, sat, dtime(16), dtime(23), 30)])
    _leave(w.vid, w.jo, sat, sat, paid_hours=4.5)               # worked 2h, then went home sick
    _leave(w.vid, w.sam, fri, sat, paid_hours=6.5)              # worked Friday, off sick Saturday
    r = w.c.post("/api/payroll/prepare", json={"venue_id": w.vid, "period_start": str(WEEK),
                                               "period_end": str(sat), "state": "wa"}, headers=w.mgr)
    lines = {e["employee_id"]: e for e in _batch(r)["employees"]}
    jo, sam = lines[w.jo], lines[w.sam]
    assert _hours_by_type(jo) == {"saturday": "2.00"}
    assert [(le["leave_type"], le["hours"]) for le in jo["leave_entries"]] == [("personal", "4.50")]
    errors = r.json()["validation_errors"]
    assert ("Jo Bar: approved personal/carer's leave holds 4.5h of the 6.5h rostered on Sat 17 Oct, "
            "so the other 2h is priced from the roster (Sat 17 Oct 16:00-18:00)") in errors
    assert not any(e.startswith("Jo Bar: rostered Sat 17 Oct 16:00-23:00 not priced") for e in errors)

    hours = sum(Decimal(h) for h in _hours_by_type(sam).values())
    assert hours == Decimal("6.50") and "saturday" not in _hours_by_type(sam)
    assert ("Sam Sportsbar: approved personal/carer's leave holds 6.5h of the 13h rostered on "
            "Fri 16 Oct to Sat 17 Oct, so the other 6.5h is priced from the roster "
            "(Fri 16 Oct 16:00-23:00)") in errors
    assert any(e.startswith("Sam Sportsbar: rostered Sat 17 Oct 16:00-23:00 not priced") for e in errors)


def _record_leave(w, monkeypatch, emp, start, end, now):
    """Record approved personal leave through the API at a Perth wall time,
    hours left blank (so they default from the roster and the punches)."""
    from rosteriq.services import leave_shifts
    monkeypatch.setattr(leave_shifts, "venue_now", lambda *a, **k: now.replace(tzinfo=PERTH))
    monkeypatch.setattr(leave_shifts, "_venue_now", lambda *a, **k: now)
    r = w.c.post("/api/leave", json={"venue_id": w.vid, "employee_id": emp, "start_date": str(start),
                                     "end_date": str(end), "leave_type": "personal"}, headers=w.mgr)
    assert r.status_code == 200, r.text
    return r.json()


def _prepare_rostered(w, monday):
    r = w.c.post("/api/payroll/prepare", json={"venue_id": w.vid, "period_start": str(monday),
                                               "period_end": str(monday + timedelta(days=6)),
                                               "state": "wa"}, headers=w.mgr)
    line = next(e for e in _batch(r)["employees"] if e["employee_id"] == w.jo)
    leave = [(le["leave_type"], le["hours"]) for le in line["leave_entries"]]
    notes = [e for e in r.json()["validation_errors"] if e.startswith("Jo Bar:")]
    return _hours_by_type(line), leave, notes


def test_rostered_prep_prices_the_worked_part_when_the_leave_left_later_shifts_open(clock, monkeypatch):
    sat, sun = date(2026, 10, 17), date(2026, 10, 18)
    w = _world()
    _roster(w.vid, [("sh-sat", w.jo, sat, dtime(16), dtime(23), 30),
                    ("sh-sun", w.jo, sun, dtime(16), dtime(23), 30)])
    clock(_at(sat, 16))
    assert _punch(w, w.jo).status_code == 200
    clock(_at(sat, 18))
    assert _punch(w, w.jo, "out").status_code == 200           # went home sick at 6
    leave = _record_leave(w, monkeypatch, w.jo, sat, sun, _at(sat, 19))
    assert leave["paid_hours"] == 11.0                          # 4.5h of Saturday + Sunday's 6.5h
    assert [(o["date"], o["hours"]) for o in leave["opened_shifts"]] == [(str(sun), 6.5)]

    hours, leave_lines, notes = _prepare_rostered(w, WEEK)
    assert hours == {"saturday": "2.00"}
    assert leave_lines == [("personal", "11.00")]
    assert notes == [
        "Jo Bar: approved personal/carer's leave holds 4.5h of the 6.5h rostered on Sat 17 Oct "
        "(its other 6.5h is the shifts it left open), so the other 2h is priced from the roster "
        "(Sat 17 Oct 16:00-18:00)"]


@pytest.mark.parametrize("worked_saturday", [False, True])
def test_rostered_prep_of_leave_across_two_pay_runs_neither_drops_nor_doubles_hours(
        clock, monkeypatch, worked_saturday):
    sat, sun, mon, tue = (date(2026, 10, 17), date(2026, 10, 18), date(2026, 10, 19),
                          date(2026, 10, 20))
    nxt = WEEK + timedelta(days=7)
    w = _world()
    _roster(w.vid, [("sh-sat", w.jo, sat, dtime(16), dtime(23), 30),
                    ("sh-sun", w.jo, sun, dtime(10), dtime(14), 0)])
    _roster(w.vid, [("sh-mon", w.jo, mon, dtime(16), dtime(23), 30)], week=nxt)
    if worked_saturday:
        clock(_at(sat, 16))
        assert _punch(w, w.jo).status_code == 200
        clock(_at(sat, 18))
        assert _punch(w, w.jo, "out").status_code == 200
    leave = _record_leave(w, monkeypatch, w.jo, sat, mon, _at(tue, 9))
    assert leave["paid_hours"] == (15.0 if worked_saturday else 17.0)
    assert leave["opened_shifts"] == []                         # every shift had started

    first, first_leave, first_notes = _prepare_rostered(w, WEEK)
    second, second_leave, second_notes = _prepare_rostered(w, nxt)
    unpriced = "not priced — on approved personal/carer's leave that day (see the leave line)"
    assert second == {} and second_notes == [f"Jo Bar: rostered Mon 19 Oct 16:00-23:00 {unpriced}"]
    if worked_saturday:
        assert first == {"saturday": "2.00"}
        assert first_notes == [
            f"Jo Bar: rostered Sun 18 Oct 10:00-14:00 {unpriced}",
            "Jo Bar: approved personal/carer's leave holds 15h of the 17h rostered on "
            "Sat 17 Oct to Mon 19 Oct, so the other 2h is priced from the roster (Sat 17 Oct 16:00-18:00)"]
    else:
        assert first == {}
        assert first_notes == [f"Jo Bar: rostered Sat 17 Oct 16:00-23:00 {unpriced}",
                               f"Jo Bar: rostered Sun 18 Oct 10:00-14:00 {unpriced}"]
    leave_total = Decimal(first_leave[0][1]) + Decimal(second_leave[0][1])
    assert leave_total == Decimal(str(leave["paid_hours"]))


def test_rostered_prep_says_when_worked_time_before_leave_falls_in_the_next_pay_run():
    sat, sun, mon = date(2026, 10, 17), date(2026, 10, 18), date(2026, 10, 19)
    nxt = WEEK + timedelta(days=7)
    w = _world()
    _roster(w.vid, [("sh-sat", w.jo, sat, dtime(16), dtime(23), 30),
                    ("sh-sun", w.jo, sun, dtime(10), dtime(14), 0)])
    _roster(w.vid, [("sh-mon", w.jo, mon, dtime(16), dtime(23), 30)], week=nxt)
    _leave(w.vid, w.jo, sat, mon, paid_hours=4.5)       # worked the weekend and 2h of Monday

    first, _, first_notes = _prepare_rostered(w, WEEK)
    second, _, second_notes = _prepare_rostered(w, nxt)
    assert first == {"saturday": "6.50", "sunday": "4.00"}
    assert first_notes == [
        "Jo Bar: approved personal/carer's leave holds 4.5h of the 17h rostered on Sat 17 Oct to "
        "Mon 19 Oct, so the other 12.5h is priced from the roster (Sat 17 Oct 16:00-23:00, "
        "Sun 18 Oct 10:00-14:00; 2h of it in the run for the other dates)"]
    assert sum(Decimal(h) for h in second.values()) == Decimal("2.00")
    assert second_notes == [
        "Jo Bar: approved personal/carer's leave holds 4.5h of the 17h rostered on Sat 17 Oct to "
        "Mon 19 Oct, so the other 12.5h is priced from the roster (Mon 19 Oct 16:00-18:00; "
        "10.5h of it in the run for the other dates)"]
