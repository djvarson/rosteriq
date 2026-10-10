"""
Wembley pilot: owner protection and the /my phone clock.

* Lowering a login at one venue never reaches someone who fully runs another
  venue the caller doesn't: a Wembley-only manager can't demote (and then
  erase) the person running both pubs, however that login was set up. The
  venue's creator and RosterIQ support still can.
* An owner RosterIQ support set up with POST /api/auth/invite is that venue's
  owner on record, like one who signed up: nobody else lowers him there, he
  can add his next venue, and the dashboard calls him the owner. A manager
  support invites later to a venue that already has its owner is not one.
* /my treats a forgotten clock-out the way the kiosk does: a rostered one is
  closed at the rostered end, an unrostered one asks "when did you finish?"
  instead of offering a clock-out that records a 20-30 hour shift. Once it's
  closed they clock in as usual. A page left open since before the punch went
  stale rechecks when it comes back into view, and the server refuses a phone
  clock-out of a stale punch without a finish time.
* The /my break buttons say what they record: an unpaid meal break.
"""

import json
import re
import uuid
from datetime import datetime, time as dtime, timedelta
from types import SimpleNamespace

from fastapi.testclient import TestClient

from rosteriq.api import app
from rosteriq.database import get_db
from rosteriq.routes import timeclock
from rosteriq.services.auth import AuthService
from rosteriq.services.clock import venue_today
from rosteriq.services.kiosk import naive_utc
from rosteriq.tests.kiosk_support import kiosk_headers
from rosteriq.tests.test_wembley_a6 import _main, _run, needs_node
from rosteriq.tests.test_wembley_b5 import _fns
from rosteriq.tests.test_wembley_b6 import _monday, _roster, _shift
from rosteriq.tests.test_wembley_g5 import (
    PW, _detail, _emp, _person, _platform_owner, _set_role, _venue, _world,
)


def _invite(w, email, name, venue_ids):
    """RosterIQ support invites a manager login; they set a password."""
    r = w.c.post("/api/auth/invite", json={"email": email, "name": name, "role": "manager",
                                           "venue_ids": venue_ids}, headers=w.sup)
    assert r.status_code == 201, r.text
    db = get_db()
    acct = db.get_user_by_email(email)
    acct["password_hash"] = AuthService.hash_password(PW)
    db.save_user(acct)
    r = w.c.post("/api/auth/login", json={"email": email, "password": PW})
    assert r.status_code == 200, r.text
    return {"Authorization": f"Bearer {r.json()['access_token']}"}


def _invited_world():
    """RosterIQ support created both pubs and invited Ben to run them."""
    c = TestClient(app)
    tag = uuid.uuid4().hex[:6]
    w = SimpleNamespace(c=c, tag=tag, vw=f"wem-{tag}", va=f"apo-{tag}",
                        ben_email=f"ben_{tag}@x.com")
    w.sup = _platform_owner(w)
    assert _venue(c, w.sup, w.vw, "The Wembley Hotel").status_code == 200
    assert _venue(c, w.sup, w.va, "Apothecary").status_code == 200
    w.ben = _invite(w, w.ben_email, "Ben", [w.vw, w.va])
    return w


def _lower_paths(w, emp_id):
    return ((f"/api/employees/{emp_id}/access-role", {"role": "staff"}),
            (f"/api/employees/{emp_id}/access", {"venue_id": w.vw, "level": "staff"}))


# --- [0] owner protection -----------------------------------------------------------

def test_a_one_pub_manager_cannot_demote_or_erase_an_owner_support_set_up():
    w = _invited_world()
    c, db = w.c, get_db()
    ben_rec = _emp(c, w.ben, w.vw, f"{w.vw}-ben", "Ben Owner", email=w.ben_email)
    andy = _person(w, w.vw, "Andy")
    _set_role(w, andy.id)

    for path, body in _lower_paths(w, ben_rec):
        r = c.post(path, json=body, headers=andy.h)
        assert r.status_code == 409 and "RosterIQ support" in _detail(r), r.text
    r = c.post(f"/api/privacy/anonymise/employee/{ben_rec}", headers=andy.h)
    assert r.status_code == 409, r.text
    acct = db.get_user_by_email(w.ben_email)
    assert acct["role"] == "manager" and acct["venue_ids"] == [w.vw, w.va]
    assert not acct.get("staff_venue_ids") and acct["is_active"] is True
    assert db.get_employee(ben_rec).name == "Ben Owner"


def test_a_one_pub_manager_cannot_lower_someone_who_fully_runs_the_other_pub():
    w = _world()
    c, db = w.c, get_db()
    gina = _person(w, w.vw, "Gina")
    _set_role(w, gina.id)
    r = c.post(f"/api/employees/{gina.id}/access", json={"venue_id": w.va, "level": "full"},
               headers=w.ben)
    assert r.status_code == 200, r.text
    andy = _person(w, w.vw, "Andy")
    _set_role(w, andy.id)

    for path, body in _lower_paths(w, gina.id):
        r = c.post(path, json=body, headers=andy.h)
        assert r.status_code == 409 and "RosterIQ support" in _detail(r), r.text
    acct = db.get_user_by_email(gina.email)
    assert acct["venue_ids"] == [w.vw, w.va] and acct["section_grants"] == {}
    # Ben, who runs both, still can.
    r = c.post(f"/api/employees/{gina.id}/access-role", json={"role": "staff"}, headers=w.ben)
    assert r.status_code == 200, r.text


def test_an_owner_support_set_up_is_the_owner_on_record():
    from rosteriq.services.venue_access import created_venue_ids, separate_signups
    w = _invited_world()
    c, db = w.c, get_db()
    ben_id = db.get_user_by_email(w.ben_email)["id"]
    assert created_venue_ids(db, ben_id, [w.vw, w.va]) == [w.vw, w.va]
    grants = db.list_events(venue_id=w.vw, category="audit", action_prefix="user.venue_grant",
                            limit=10)
    assert [(g["resource_id"], g["details"]["reason"]) for g in grants] == [(ben_id, "owner_provisioned")]
    r = c.get("/api/me/access", headers=w.ben)
    assert r.json()["label"] == "Owner (The Wembley Hotel, Apothecary)"

    # A manager of both pubs he appointed can't lower him at either.
    sam = _person(w, w.vw, "Sam")
    _set_role(w, sam.id)
    r = c.post(f"/api/employees/{sam.id}/access", json={"venue_id": w.va, "level": "full"},
               headers=w.ben)
    assert r.status_code == 200, r.text
    ben_rec = _emp(c, w.ben, w.vw, f"{w.vw}-ben", "Ben", email=w.ben_email)
    for path, body in _lower_paths(w, ben_rec):
        r = c.post(path, json=body, headers=sam.h)
        assert r.status_code == 403 and "venue owner's login" in _detail(r), r.text
    acct = db.get_user_by_email(w.ben_email)
    assert acct["venue_ids"] == [w.vw, w.va] and not acct.get("staff_venue_ids")

    # He adds his next pub himself.
    assert _venue(c, w.ben, f"third-{w.tag}", "The Third").status_code == 200

    # Another owner support sets up is a separate business.
    vo = f"oth-{w.tag}"
    assert _venue(c, w.sup, vo, "Olive's Bar").status_code == 200
    _invite(w, f"olive_{w.tag}@x.com", "Olive", [vo])
    assert separate_signups(db, w.vw, vo) is True
    assert separate_signups(db, w.vw, w.va) is False

    # A manager support invites to a pub that already has its owner is not an
    # owner: Ben can lower her.
    gm_email = f"gina_{w.tag}@x.com"
    _invite(w, gm_email, "Gina", [w.vw])
    gm_id = db.get_user_by_email(gm_email)["id"]
    assert created_venue_ids(db, gm_id, [w.vw]) == []
    gm_rec = _emp(c, w.ben, w.vw, f"{w.vw}-gina", "Gina", email=gm_email)
    r = c.post(f"/api/employees/{gm_rec}/access-role", json={"role": "staff"}, headers=w.ben)
    assert r.status_code == 200, r.text
    # Nor is a staff login, even at a venue with no owner yet.
    vn = f"new-{w.tag}"
    assert _venue(c, w.sup, vn, "New Bar").status_code == 200
    r = c.post("/api/auth/invite", json={"email": f"kit_{w.tag}@x.com", "name": "Kit", "role": "staff",
                                         "venue_ids": [vn]}, headers=w.sup)
    assert r.status_code == 201, r.text
    assert db.list_events(venue_id=vn, category="audit", action_prefix="user.venue_grant", limit=10) == []


# --- [18] a forgotten clock-out on /my ---------------------------------------------

def _self_clock(w):
    r = w.c.put("/api/kiosk/settings", json={"venue_id": w.vw, "allow_self_clock": True}, headers=w.ben)
    assert r.status_code == 200, r.text
    return _person(w, w.vw, "Dan")


def _my_clock(w, who):
    r = w.c.get(f"/api/me/clock?venue_id={w.vw}", headers=who.h)
    assert r.status_code == 200, r.text
    return r.json()


def test_my_clock_asks_for_the_finish_time_of_a_forgotten_unrostered_punch():
    w = _world()
    c, db = w.c, get_db()
    dan = _self_clock(w)
    r = c.post("/api/clock/in", json={"venue_id": w.vw, "employee_id": dan.id}, headers=dan.h)
    assert r.status_code == 200 and r.json()["rostered"] is False
    me = _my_clock(w, dan)
    assert me["state"] == "on" and me["stale"] is False

    tz = timeclock._tz(w.vw, db)
    ts = db.get_open_timesheet(w.vw, dan.id)
    ci = datetime.utcnow() - timedelta(hours=30, minutes=21)
    ts["clock_in"], ts["work_date"] = ci, timeclock._local(ci, tz).date()
    db.save_timesheet(ts)
    me = _my_clock(w, dan)
    board = c.get(f"/api/clock/board?venue_id={w.vw}", headers=kiosk_headers(c, w.ben, w.vw)).json()
    card = next(s for s in board["staff"] if s["employee_id"] == dan.id)
    assert me["state"] == "on" and me["stale"] is True and card["stale"] is True
    assert me["clock_in_label"] == card["clock_in_label"] == timeclock._fmt_local(ci, tz)

    # The finish time /my now collects closes it as the kiosk's does.
    finished = (timeclock._local(ci, tz) + timedelta(hours=8)).strftime("%H:%M")
    r = c.post("/api/clock/out", json={"venue_id": w.vw, "employee_id": dan.id,
                                       "finished_at": finished}, headers=dan.h)
    assert r.status_code == 200, r.text
    assert "finish_entered_later" in r.json()["flags"] and r.json()["worked_minutes"] <= 8 * 60
    me = _my_clock(w, dan)
    assert me["state"] == "off" and me["stale"] is False and me["clock_in_label"] is None
    r = c.post("/api/clock/in", json={"venue_id": w.vw, "employee_id": dan.id}, headers=dan.h)
    assert r.status_code == 200, r.text


def test_my_clock_closes_a_forgotten_rostered_punch_at_the_rostered_end():
    w = _world()
    c, db = w.c, get_db()
    dan = _self_clock(w)
    tz = timeclock._tz(w.vw, db)
    yday = venue_today(w.vw, db) - timedelta(days=1)
    _roster(w.vw, _monday(yday), [_shift("dan-y", dan.id, yday, dtime(9, 0), dtime(17, 0))],
            state="published")
    ci = timeclock._utc(datetime.combine(yday, dtime(9, 0)), tz)
    db.save_timesheet({"id": "ts-dan-y", "venue_id": w.vw, "employee_id": dan.id, "work_date": yday,
                       "clock_in": ci, "clock_out": None, "break_minutes": 0, "break_source": None,
                       "break_started_at": None, "status": "open", "pin_verified": False,
                       "rostered_shift_id": "dan-y", "variance_minutes": None, "flags": [],
                       "source": "self", "device_id": None, "created_at": ci})

    me = _my_clock(w, dan)
    assert me["state"] == "off" and me["stale"] is False
    closed = db.get_timesheet("ts-dan-y")
    assert naive_utc(closed["clock_out"]) == timeclock._utc(datetime.combine(yday, dtime(17, 0)), tz)
    assert "auto_closed" in closed["flags"]
    r = c.post("/api/clock/in", json={"venue_id": w.vw, "employee_id": dan.id}, headers=dan.h)
    assert r.status_code == 200, r.text


_MY_CLOCK_JS = r"""
global.toast = function (m, k) { toasts.push({ m: m, k: k }); };
global.loadMyClock = function () {};
global.loadHours = function () {};
global.myVenues = [];
global.reply = {};
global.api = async function (path, opts) { calls.push({ path: path, body: opts && opts.body }); return reply; };
"""


@needs_node
def test_my_asks_a_forgotten_clock_out_when_they_finished():
    src = _main("my.html")
    data = _run(_MY_CLOCK_JS + r"""
var myClocks = [{ linked: true, venue_id: 'wem', employee_id: 'e0', has_pin: true, self_clock: true,
                  state: 'on', stale: true, clock_in_label: 'Fri <b>5:58</b> pm' }];
""" + _fns(src, "clockVenueName", "renderMyClock", "selfPunch", "saveMyFinish") + r"""
async function main() {
  renderMyClock(myClocks);
  const html = el('clock-card').innerHTML;
  await saveMyFinish(0);
  const empty = calls.length;
  el('my-finish-0').value = '23:30';
  reply = { note: 'Finish time saved and flagged for your manager to check.' };
  await saveMyFinish(0);
  await saveMyFinish(0, true);
  console.log(JSON.stringify({ html: html, empty: empty, calls: calls, toasts: toasts }));
}
""")
    html = data["html"]
    assert "Still clocked in from Fri &lt;b&gt;5:58&lt;/b&gt; pm &mdash; when did you finish?" in html
    assert 'id="my-finish-0" type="time"' in html
    assert "Save my finish time" in html and "I finished just now" in html
    assert "meal break" not in html and "Clock in<" not in html and "Clock out<" not in html
    assert data["empty"] == 0 and data["toasts"][0]["k"] == "err"
    saved, now = data["calls"]
    assert saved == {"path": "/api/clock/out",
                     "body": {"venue_id": "wem", "employee_id": "e0", "finished_at": "23:30"}}
    assert now["path"] == "/api/clock/out"
    assert re.fullmatch(r"\d{2}:\d{2}", now["body"]["finished_at"])
    assert "Finish time saved" in data["toasts"][1]["m"]



def _age(db, w, emp_id, hours):
    """Back-date someone's open punch: a clock-out forgotten `hours` ago."""
    tz = timeclock._tz(w.vw, db)
    ts = db.get_open_timesheet(w.vw, emp_id)
    ci = datetime.utcnow() - timedelta(hours=hours)
    ts["clock_in"], ts["work_date"] = ci, timeclock._local(ci, tz).date()
    db.save_timesheet(ts)
    return ci, tz


@needs_node
def test_a_phone_left_open_since_before_the_punch_went_stale_asks_when_they_finished():
    w = _world()
    c, db = w.c, get_db()
    dan = _self_clock(w)
    assert c.post("/api/clock/in", json={"venue_id": w.vw, "employee_id": dan.id},
                  headers=dan.h).status_code == 200
    fresh = _my_clock(w, dan)                      # what the open tab still holds
    ci, tz = _age(db, w, dan.id, 30.35)
    data = _run(_MY_CLOCK_JS + "var myClocks = [%s];\n" % json.dumps(fresh)
                + _fns(_main("my.html"), "clockVenueName", "renderMyClock", "selfPunch") + r"""
async function main() {
  renderMyClock(myClocks);
  await selfPunch('out', 0);
  console.log(JSON.stringify({ calls: calls }));
}
""")
    call = data["calls"][0]
    r = c.post(call["path"], json=call["body"], headers=dan.h)
    assert r.status_code == 409, r.text
    assert f"still clocked in from {timeclock._fmt_local(ci, tz)}" in _detail(r)
    assert "time you finished" in _detail(r)
    assert db.get_open_timesheet(w.vw, dan.id)["clock_out"] is None
    assert _my_clock(w, dan)["stale"] is True

    # The finish time the stale card asks for still closes it.
    finished = (timeclock._local(ci, tz) + timedelta(hours=8)).strftime("%H:%M")
    r = c.post("/api/clock/out", json={"venue_id": w.vw, "employee_id": dan.id,
                                       "finished_at": finished}, headers=dan.h)
    assert r.status_code == 200 and r.json()["worked_minutes"] <= 8 * 60, r.text

    # On the venue's kiosk an old punch is still recorded and flagged, as before.
    k = kiosk_headers(c, w.ben, w.vw)
    assert c.post("/api/clock/in", json={"venue_id": w.vw, "employee_id": dan.id},
                  headers=k).status_code == 200
    _age(db, w, dan.id, 15)
    r = c.post("/api/clock/out", json={"venue_id": w.vw, "employee_id": dan.id}, headers=k)
    assert r.status_code == 200 and "long_shift" in r.json()["flags"], r.text


def test_a_phone_clock_out_of_a_forgotten_rostered_punch_closes_it_at_the_rostered_end():
    w = _world()
    c, db = w.c, get_db()
    dan = _self_clock(w)
    tz = timeclock._tz(w.vw, db)
    yday = venue_today(w.vw, db) - timedelta(days=1)
    _roster(w.vw, _monday(yday), [_shift("dan-y2", dan.id, yday, dtime(9, 0), dtime(17, 0))],
            state="published")
    ci = timeclock._utc(datetime.combine(yday, dtime(9, 0)), tz)
    db.save_timesheet({"id": "ts-dan-y2", "venue_id": w.vw, "employee_id": dan.id, "work_date": yday,
                       "clock_in": ci, "clock_out": None, "break_minutes": 0, "break_source": None,
                       "break_started_at": None, "status": "open", "pin_verified": False,
                       "rostered_shift_id": "dan-y2", "variance_minutes": None, "flags": [],
                       "source": "self", "device_id": None, "created_at": ci})

    r = c.post("/api/clock/out", json={"venue_id": w.vw, "employee_id": dan.id}, headers=dan.h)
    assert r.status_code == 409 and "closed at its rostered finish" in _detail(r), r.text
    closed = db.get_timesheet("ts-dan-y2")
    assert naive_utc(closed["clock_out"]) == timeclock._utc(datetime.combine(yday, dtime(17, 0)), tz)
    assert "auto_closed" in closed["flags"] and "long_shift" not in closed["flags"]
    assert c.post("/api/clock/in", json={"venue_id": w.vw, "employee_id": dan.id},
                  headers=dan.h).status_code == 200


def test_my_clock_says_when_the_open_punch_goes_stale():
    w = _world()
    c, db = w.c, get_db()
    dan = _self_clock(w)
    assert _my_clock(w, dan)["stale_at"] is None
    assert c.post("/api/clock/in", json={"venue_id": w.vw, "employee_id": dan.id},
                  headers=dan.h).status_code == 200
    me = _my_clock(w, dan)
    ci = naive_utc(db.get_open_timesheet(w.vw, dan.id)["clock_in"])
    due = ci + timedelta(hours=timeclock.kiosk.STALE_OPEN_HOURS)
    assert me["stale_at"] == due.isoformat(timespec="seconds")


@needs_node
def test_my_asks_for_the_finish_time_once_the_card_it_holds_has_gone_stale():
    src = _main("my.html")
    data = _run(_MY_CLOCK_JS + r"""
var reloads = 0;
global.loadMyClock = function () { reloads++; };
var card = { linked: true, venue_id: 'wem', employee_id: 'e0', has_pin: true, self_clock: true,
             state: 'on', stale: false, clock_in_label: 'Fri 5:58 pm', stale_at: '2020-01-01T07:58:00' };
var myClocks = [card];
""" + _fns(src, "selfPunch") + r"""
async function main() {
  const out = [];
  for (const a of ['out', 'break/start', 'break/end']) {
    await selfPunch(a, 0);
    out.push({ calls: calls.length, reloads: reloads });
  }
  await selfPunch('out', 0, { finished_at: '23:30' });
  await selfPunch('in', 0);
  myClocks = [Object.assign({}, card, { stale_at: '2999-01-01T00:00:00' })];
  await selfPunch('out', 0);
  myClocks = [Object.assign({}, card, { stale_at: null, state: 'off' })];
  await selfPunch('in', 0);
  console.log(JSON.stringify({ out: out, calls: calls, toasts: toasts }));
}
""")
    assert data["out"] == [{"calls": 0, "reloads": 1}, {"calls": 0, "reloads": 2},
                           {"calls": 0, "reloads": 3}]
    assert all(t["k"] == "err" and "still clocked in from Fri 5:58 pm" in t["m"]
               for t in data["toasts"][:3])
    assert [(c["path"], (c["body"] or {}).get("finished_at")) for c in data["calls"]] == [
        ("/api/clock/out", "23:30"), ("/api/clock/in", None),
        ("/api/clock/out", None), ("/api/clock/in", None)]


@needs_node
def test_my_rechecks_the_clock_when_the_hours_tab_comes_back_into_view():
    src = _main("my.html")
    assert re.search(r"^document\.addEventListener\('visibilitychange', refreshMyClockOnReturn\);$",
                     src, re.M)
    assert re.search(r"^window\.addEventListener\('pageshow', refreshMyClockOnReturn\);$", src, re.M)
    data = _run(_MY_CLOCK_JS + r"""
var reloads = [];
global.loadMyClock = function () { reloads.push(step); };
var step = '';
""" + _fns(src, "refreshMyClockOnReturn") + r"""
async function main() {
  const run = function (name, view, state, ev) {
    step = name;
    el('view-hours').style.display = view;
    document.visibilityState = state;
    refreshMyClockOnReturn(ev);
  };
  run('shown on hours', 'block', 'visible', { type: 'visibilitychange' });
  run('hidden', 'block', 'hidden', { type: 'visibilitychange' });
  run('other tab', 'none', 'visible', { type: 'visibilitychange' });
  run('back-forward cache', 'block', 'visible', { type: 'pageshow', persisted: true });
  run('first load', 'block', 'visible', { type: 'pageshow', persisted: false });
  console.log(JSON.stringify({ reloads: reloads }));
}
""")
    assert data["reloads"] == ["shown on hours", "back-forward cache"]


@needs_node
def test_my_redraws_the_clock_after_a_refused_punch():
    src = _main("my.html")
    data = _run(_MY_CLOCK_JS + r"""
var reloads = 0;
global.loadMyClock = function () { reloads++; };
global.api = async function () { throw new Error("You're still clocked in from Fri 5:58 pm."); };
var myClocks = [{ linked: true, venue_id: 'wem', employee_id: 'e0', self_clock: true, state: 'on' }];
""" + _fns(src, "selfPunch") + r"""
async function main() {
  await selfPunch('out', 0);
  console.log(JSON.stringify({ reloads: reloads, toasts: toasts }));
}
""")
    assert data["reloads"] == 1
    assert data["toasts"] == [{"m": "You're still clocked in from Fri 5:58 pm.", "k": "err"}]


# --- [19] the /my break buttons -----------------------------------------------------

@needs_node
def test_my_break_buttons_say_unpaid_meal_break():
    src = _main("my.html")
    data = _run(_MY_CLOCK_JS + r"""
var myClocks = [{ linked: true, venue_id: 'wem', employee_id: 'e0', has_pin: true, self_clock: true,
                  state: 'on', stale: false }];
""" + _fns(src, "clockVenueName", "renderMyClock", "selfPunch") + r"""
async function main() {
  renderMyClock(myClocks);
  const on = el('clock-card').innerHTML;
  renderMyClock([Object.assign({}, myClocks[0], { state: 'break' })]);
  const brk = el('clock-card').innerHTML;
  renderMyClock([Object.assign({}, myClocks[0], { state: 'off' })]);
  const off = el('clock-card').innerHTML;
  await selfPunch('break/start', 0);
  await selfPunch('break/end', 0);
  console.log(JSON.stringify({ on: on, brk: brk, off: off, calls: calls, toasts: toasts }));
}
""")
    assert "Start meal break (unpaid)" in data["on"] and "Paid rest breaks: stay clocked in." in data["on"]
    assert "End meal break" in data["brk"] and "unpaid meal break" in data["brk"]
    assert "Paid rest breaks" in data["brk"] and "Paid rest breaks" not in data["off"]
    assert "Start break" not in data["on"] and "End break" not in data["brk"]
    assert [c["body"] for c in data["calls"]] == [{"venue_id": "wem", "employee_id": "e0"}] * 2
    assert "Unpaid meal break started" in data["toasts"][0]["m"]
    assert "meal break" in data["toasts"][1]["m"]
