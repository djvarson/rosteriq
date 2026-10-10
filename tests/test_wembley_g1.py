"""Wembley pilot, review G1: the service worker, dashboard JavaScript and cut
advice.

* The service worker only ever answers for the app's own /static/ files.
  Rosters, staff, venues, forecasts and the API go straight to the network,
  the old caches that held them are deleted, and signing out clears the rest.
  A dashboard that opens under the old worker empties its cache before each
  request until the new worker takes over.
* The staff edit modal keeps an imported visa status it has no option for,
  and only sends work-rights fields the manager changed.
* An open shift (nobody on it, e.g. left open by leave) is never a person: not
  on the On-Shift headcount, not in the Day view's counts, not in the cut
  advice's area minimums. It is shown on its own as unfilled.
* On-Shift reads the live roster again when it opens and every minute.
* The Areas & minimums editor and the time-clock settings drop a late answer
  for the venue the manager just left, and never save onto it.
* A roster re-paste reports the shifts it removed and changed.
* An area's follows_demand survives the editor, the wizard and older clients.
* Trading hours take a Closed day, and On-Shift and the Day view apply no
  minimums on it.

As in test_wembley_a6, the JS is pulled out of the real pages and run under
node with a small fake DOM.
"""
import asyncio
import json
import os
import re
import uuid
from datetime import date, datetime, time, timedelta

import pytest
from fastapi.testclient import TestClient

from rosteriq.api import app
from rosteriq.database import get_db
from rosteriq.models import Shift, ShiftStatus
from rosteriq.services import cut_advice, leave_shifts
from rosteriq.tests.test_wembley_a6 import STATIC, _dash, _fns, _html, _run, _top, needs_node
from rosteriq.tests.test_wembley_b8 import _emp, _roster, _shift, _venue

DAYS = ("mon", "tue", "wed", "thu", "fri", "sat", "sun")
FRI = date(2026, 10, 9)
MON = FRI - timedelta(days=4)
PW = "Passw0rd!234"

TICK = r"""
const tick = function (ms) { return new Promise(function (r) { setTimeout(r, ms || 0); }); };
"""


def _sw() -> str:
    with open(os.path.join(STATIC, "sw.js"), encoding="utf-8") as fh:
        return fh.read()


# --------------------------------------------------------------------------
# [31] Service worker
# --------------------------------------------------------------------------

SW_STUBS = TICK + r"""
const listeners = {};
const stored = new Map();
const deleted = [];
global.fetched = [];
global.claimed = 0;
global.skipped = 0;
global.self = {
  location: { origin: 'https://rosteriq.example' },
  addEventListener(t, f) { listeners[t] = f; },
  skipWaiting() { skipped++; },
  clients: { claim() { claimed++; return Promise.resolve(); }, matchAll: async () => [] },
  registration: {},
};
const keyOf = function (r) { return typeof r === 'string' ? r : r.url; };
global.caches = {
  names: ['rosteriq-v1', 'someone-elses-cache'],
  async keys() { return this.names.slice(); },
  async delete(n) { deleted.push(n); this.names = this.names.filter(function (x) { return x !== n; }); return true; },
  async open(n) {
    if (this.names.indexOf(n) < 0) this.names.push(n);
    return { put: async function (r, res) { stored.set(keyOf(r), res); }, match: async function (r) { return stored.get(keyOf(r)); },
             addAll: async function () {} };
  },
  async match(r) { return stored.get(keyOf(r)); },
};
global.fetch = async function (r) { fetched.push(keyOf(r)); return { status: 200, from: 'network', clone() { return this; } }; };
global.Response = function (body, init) { this.body = body; this.status = (init || {}).status; };
function request(path, opts) {
  opts = opts || {};
  return { url: /^https?:/.test(path) ? path : 'https://rosteriq.example' + path, method: opts.method || 'GET',
           mode: opts.mode || 'cors',
           headers: { get: function (k) { return String(k).toLowerCase() === 'accept' ? (opts.accept || '*/*') : null; } } };
}
async function dispatch(path, opts) {
  let answer = null;
  listeners.fetch({ request: request(path, opts), respondWith: function (p) { answer = Promise.resolve(p); } });
  return answer ? { intercepted: true, response: await answer } : { intercepted: false };
}
"""


@needs_node
def test_service_worker_sends_api_and_data_routes_straight_to_the_network():
    data = _run(SW_STUBS + _sw() + r"""
async function main() {
  // What a v1 worker left behind must never be served again.
  stored.set('https://rosteriq.example/rosters?current=true&venue_id=wem&limit=200&offset=0', { from: 'cache' });
  stored.set('https://rosteriq.example/venues', { from: 'cache' });
  stored.set('https://rosteriq.example/static/app.js', { from: 'cache' });
  const paths = ['/rosters?current=true&venue_id=wem&limit=200&offset=0', '/venues', '/venues/x',
    '/employees?venue_id=wem&limit=500&offset=0', '/forecasts?venue_id=wem', '/rosters/r1/changes',
    '/api/ops/cut-advice?venue_id=wem', '/api/auth/refresh', '/my', '/static-ish/x.js'];
  const passed = {};
  for (const p of paths) passed[p] = (await dispatch(p)).intercepted;
  const post = (await dispatch('/static/app.js', { method: 'POST' })).intercepted;
  const cross = (await dispatch('https://cdn.example.com/static/app.js')).intercepted;
  const asset = await dispatch('/static/app.js');
  const assetFetches = fetched.length;
  const page = await dispatch('/static/dashboard.html', { mode: 'navigate', accept: 'text/html' });
  console.log(JSON.stringify({ passed: passed, post: post, cross: cross,
    asset: asset.intercepted ? asset.response.from : null, assetFetches: assetFetches,
    page: page.intercepted ? page.response.from : null, fetched: fetched, version: CACHE_NAME }));
}
""")
    assert not any(data["passed"].values()), data["passed"]
    assert data["post"] is False and data["cross"] is False
    assert data["asset"] == "cache" and data["assetFetches"] == 0          # cache-first for /static/ assets
    assert data["page"] == "network"                                        # network-first for pages
    assert data["fetched"] == ["https://rosteriq.example/static/dashboard.html"]
    assert data["version"] != "rosteriq-v1"


@needs_node
def test_service_worker_activation_deletes_every_older_cache_and_takes_over():
    data = _run(SW_STUBS + _sw() + r"""
async function main() {
  let installing = null, activating = null;
  listeners.install({ waitUntil: function (p) { installing = p; } });
  await installing;
  listeners.activate({ waitUntil: function (p) { activating = p; } });
  await activating;
  console.log(JSON.stringify({ deleted: deleted, names: caches.names, claimed: claimed, skipped: skipped,
                               version: CACHE_NAME }));
}
""")
    assert sorted(data["deleted"]) == ["rosteriq-v1", "someone-elses-cache"]
    assert data["names"] == [data["version"]]
    assert data["claimed"] == 1 and data["skipped"] == 1


@needs_node
def test_signing_out_or_an_expired_session_clears_cached_pages():
    src = _dash()
    data = _run(TICK + _top(src, "_sessionEnding") + "\n" + _fns(src, "handleLogout", "_sessionExpired") + r"""
const deleted = [];
global.caches = { names: ['rosteriq-v1', 'rosteriq-v2'], async keys() { return this.names.slice(); },
                  async delete(n) { deleted.push(n); return true; } };
async function main() {
  handleLogout();
  await tick(5);
  const onLogout = deleted.splice(0);
  const loggedOutTo = location.href;
  location.href = '';
  _sessionExpired();
  await tick(5);
  console.log(JSON.stringify({ onLogout: onLogout, loggedOutTo: loggedOutTo, onExpiry: deleted, expiredTo: location.href }));
}
""")
    assert sorted(data["onLogout"]) == ["rosteriq-v1", "rosteriq-v2"]
    assert sorted(data["onExpiry"]) == ["rosteriq-v1", "rosteriq-v2"]
    assert data["loggedOutTo"] == data["expiredTo"] == "/static/login.html"


# The first dashboard load after the deploy still runs under the old worker for
# a few seconds. It answered data GETs from 'rosteriq-v1' first, then the
# network, caching what it fetched.
OLD_WORKER_PAGE = r"""
global.STORE = { 'rosteriq-v1': new Map([
  ['/venues', [{ id: 'wem-v' }]],
  ['/employees?venue_id=wem-v', { items: [{ id: 'bob', phone: '0400 111 222' }] }]]) };
global.SERVER = { '/venues': [{ id: 'wem-v' }, { id: 'apo-v' }],
                  '/employees?venue_id=wem-v': { items: [{ id: 'bob', phone: '0499 000 111' }] } };
global.cacheCalls = [];
global.caches = {
  async has(n) { cacheCalls.push('has ' + n); return n in STORE; },
  async delete(n) { cacheCalls.push('delete ' + n); const had = n in STORE; delete STORE[n]; return had; },
};
const swListeners = {};
Object.defineProperty(globalThis, 'navigator', { configurable: true, writable: true, value: {
  serviceWorker: { controller: CONTROLLER, addEventListener(t, f) { swListeners[t] = f; } } } });
global.oldWorker = true;
global.answeredFrom = [];
function resp(body) {
  return { ok: true, status: 200, statusText: '200', headers: { get: function () { return 'application/json'; } },
           json: async function () { return JSON.parse(JSON.stringify(body)); } };
}
global.fetch = async function (url, opts) {
  const get = !opts || !opts.method || opts.method === 'GET';
  if (oldWorker && get && url.indexOf('/api/') !== 0) {
    for (const n of Object.keys(STORE)) {
      if (STORE[n].has(url)) { answeredFrom.push('cache'); return resp(STORE[n].get(url)); }
    }
    (STORE['rosteriq-v1'] = STORE['rosteriq-v1'] || new Map()).set(url, SERVER[url]);
  }
  answeredFrom.push('network');
  return resp(SERVER[url]);
};
global.state = { baseUrl: '', authToken: 'tok', isDemo: false };
global._lastErrorReference = null;
global._refreshInFlight = null;
global._sessionEnding = false;
"""


def _legacy_purge_js():
    src = _dash()
    return (_between(src, "window._legacySwPurge = ", "\nasync function fetchAPI") + "\n"
            + _fns(src, "_refreshAccessToken", "_sessionExpired", "fetchAPI"))


@needs_node
def test_first_load_under_the_old_worker_reads_the_server_not_its_cache():
    data = _run("const CONTROLLER = { scriptURL: '/static/sw.js' };\n" + OLD_WORKER_PAGE + _legacy_purge_js() + r"""
async function main() {
  const venues = (await fetchAPI('/venues')).map(function (v) { return v.id; });
  const phone = (await fetchAPI('/employees?venue_id=wem-v')).items[0].phone;
  // Another manager edits Bob while the old worker still has this page: the
  // copy it just cached must not answer the next read either.
  SERVER['/employees?venue_id=wem-v'] = { items: [{ id: 'bob', phone: '0411 222 333' }] };
  const again = (await fetchAPI('/employees?venue_id=wem-v')).items[0].phone;
  const underOld = cacheCalls.splice(0);
  // The new worker takes over (it deleted 'rosteriq-v1' on activation).
  oldWorker = false;
  swListeners.controllerchange();
  await fetchAPI('/venues');
  console.log(JSON.stringify({ venues: venues, phone: phone, again: again, answeredFrom: answeredFrom,
                               underOld: underOld, afterTakeover: cacheCalls }));
}
""")
    assert data["venues"] == ["wem-v", "apo-v"]
    assert data["phone"] == "0499 000 111"
    assert data["again"] == "0411 222 333"
    assert data["answeredFrom"] == ["network"] * 4
    assert data["underOld"][0] == "has rosteriq-v1"
    assert data["underOld"].count("delete rosteriq-v1") == 3
    assert data["afterTakeover"] == []


@needs_node
def test_a_page_with_no_old_worker_never_deletes_caches_per_request():
    prog = r"""
async function main() {
  oldWorker = false;
  await fetchAPI('/venues');
  await fetchAPI('/venues');
  console.log(JSON.stringify({ purge: window._legacySwPurge === null, cacheCalls: cacheCalls }));
}
"""
    uncontrolled = _run("const CONTROLLER = null;\n" + OLD_WORKER_PAGE + _legacy_purge_js() + prog)
    assert uncontrolled == {"purge": True, "cacheCalls": []}
    # Under the new worker, which deleted 'rosteriq-v1' when it took over.
    under_new = _run("const CONTROLLER = { scriptURL: '/static/sw.js' };\n" + OLD_WORKER_PAGE
                     + "delete STORE['rosteriq-v1'];\n" + _legacy_purge_js() + prog)
    assert under_new["cacheCalls"] == ["has rosteriq-v1"]


# --------------------------------------------------------------------------
# [32] Staff edit modal: the recorded visa status
# --------------------------------------------------------------------------

def _visa_options():
    html = _html("dashboard.html")
    block = re.search(r'<select id="ee-visa">([\s\S]*?)</select>', html).group(1)
    return re.findall(r'value="([^"]*)"', block)


VISA_STUBS = r"""
function mkSelect(id, values) {
  const sel = el(id);
  sel.options = [];
  sel.appendChild = function (o) {
    o.parentNode = sel; sel.options.push(o);
    if (o.id) _els[o.id] = o;
    return o;
  };
  sel.removeChild = function (o) {
    sel.options = sel.options.filter(function (x) { return x !== o; });
    o.parentNode = null;
    if (o.id && _els[o.id] === o) delete _els[o.id];
    return o;
  };
  values.forEach(function (v) { const o = mkEl(''); o.value = v; o.textContent = v; sel.appendChild(o); });
  let v = '';
  // A select can only hold one of its options' values; anything else reads back ''.
  Object.defineProperty(sel, 'value', { configurable: true, get: function () { return v; },
    set: function (x) { x = String(x); v = sel.options.some(function (o) { return o.value === x; }) ? x : ''; } });
  return sel;
}
function mkInput(id) {
  const input = el(id);
  let v = '';
  Object.defineProperty(input, 'value', { configurable: true, get: function () { return v; },
    set: function (x) { v = String(x); } });
  return input;
}
global.state = { isDemo: false, currentVenue: 'wem', employees: [
  { id: 'bob', name: 'Bob Smith', phone: '0412 345 678', hourly_base_rate: '31.50', employment_type: 'casual',
    visa_status: 'working_holiday_maker', visa_expiry: '2027-03-01', visa_work_limit_fortnight: 48 },
  { id: 'cat', name: 'Cat Jones', phone: '0400 000 000', hourly_base_rate: '30.00', employment_type: 'casual',
    visa_status: 'student', visa_expiry: null, visa_work_limit_fortnight: null },
  { id: 'dee', name: 'Dee', hourly_base_rate: '30.00', employment_type: 'casual',
    visa_status: '<img_src=x_onerror=alert(1)>' } ] };
var _staffRowIds = ['bob', 'cat', 'dee'], _editEmp = null, _editAreaChoices = [];
global.localIso = function () { return '2026-10-10'; };
global._editAreaPills = function () {};
global.audiencePillOptions = async function () { return []; };
global.staffRateProblem = function () { return null; };
global.openModal = function () {};
global.closeModal = function () {};
global.loadVenueData = function () {};
global.puts = [];
global.fetchAPI = async function (url, opts) { puts.push([url, JSON.parse(opts.body)]); return { status: 'updated', notes: [] }; };
"""


@needs_node
def test_edit_modal_keeps_an_imported_visa_status_and_sends_only_changed_work_rights():
    data = _run(VISA_STUBS + _fns(_dash(), "openEditEmployee", "saveEditEmployee") + r"""
const BASE = %s;
function shown() {
  const sel = el('ee-visa');
  return { value: sel.value, options: sel.options.map(function (o) { return [o.value, String(o.textContent), o.innerHTML]; }) };
}
async function main() {
  mkSelect('ee-visa', BASE);
  ['ee-visa-cap', 'ee-max-hours', 'ee-visa-expiry'].forEach(mkInput);
  await openEditEmployee(0);
  const bob = shown();
  el('ee-phone').value = '0499 000 111';
  await saveEditEmployee();
  await openEditEmployee(1);
  const cat = shown();
  el('ee-visa').value = 'citizen';
  await saveEditEmployee();
  await openEditEmployee(2);
  const dee = shown();
  console.log(JSON.stringify({ bob: bob, cat: cat, dee: dee, puts: puts }));
}
""" % json.dumps(_visa_options()))
    base = _visa_options()
    assert "working_holiday_maker" not in base
    bob = data["bob"]
    assert bob["value"] == "working_holiday_maker"
    assert bob["options"][-1][:2] == ["working_holiday_maker", "Recorded: working holiday maker"]
    url, body = data["puts"][0]
    assert url == "/employees/bob" and body["phone"] == "0499 000 111"
    for field in ("visa_status", "visa_expiry", "visa_work_limit_fortnight"):
        assert field not in body, field
    # The next person's modal carries no leftover option from Bob.
    assert [o[0] for o in data["cat"]["options"]] == base and data["cat"]["value"] == "student"
    url, body = data["puts"][1]
    assert url == "/employees/cat" and body["visa_status"] == "citizen"
    assert "visa_expiry" not in body and "visa_work_limit_fortnight" not in body
    # Set as text, never as markup.
    dee = data["dee"]["options"][-1]
    assert dee[1] == "Recorded: <img src=x onerror=alert(1)>" and dee[2] == ""
    assert [o[0] for o in data["dee"]["options"]].count("<img_src=x_onerror=alert(1)>") == 1


# --------------------------------------------------------------------------
# [33]/[39]/[40] Open shifts are never people; [34] On-Shift re-reads the roster
# --------------------------------------------------------------------------

def _between(src, start, end):
    i = src.index(start)
    return src[i:src.index(end, i)]


OPS_STUBS = TICK + r"""
global.intervals = [];
global.setInterval = function (f, ms) { intervals.push([f, ms]); };
global.localIso = function (d) { return d.toISOString().slice(0, 10); };
global.state = { venues: [], currentVenue: 'v1', currentPage: 'on-shift', rosters: [], employees: [], isDemo: false };
const venue = { id: 'v1', name: 'The Wembley', timezone: 'Australia/Perth',
  areas: [{ name: 'front bar', label: 'Front Bar', min_by_day: { fri: 2 } }],
  trading_hours: { fri: { open: '11:00', close: '00:00' } } };
const AVA = { id: 'a1', employee_id: 'ava', date: '2026-10-09', start_time: '17:00', end_time: '23:30', role: 'front bar', status: 'scheduled' };
const OPEN_BAR = { id: 'o1', employee_id: '', date: '2026-10-09', start_time: '19:00', end_time: '23:00', role: 'front bar', status: 'scheduled' };
const OPEN_KITCHEN = { id: 'o2', employee_id: null, date: '2026-10-09', start_time: '18:00', end_time: '22:00', role: 'kitchen', status: 'scheduled' };
const OPEN_CANCELLED = { id: 'o3', employee_id: '', date: '2026-10-09', start_time: '19:00', end_time: '23:00', role: 'front bar', status: 'cancelled' };
"""


def _ops_js():
    src = _dash()
    return _between(src, "var OPS_WEEK_KEYS", "// ── Staff Contact Export")


@needs_node
def test_on_shift_now_and_day_view_show_open_shifts_apart_from_heads():
    data = _run(OPS_STUBS + _ops_js() + r"""
async function main() {
  const shifts = [AVA, OPEN_BAR, OPEN_KITCHEN, OPEN_CANCELLED];
  state.venues = [venue];
  state.rosters = [{ venue_id: 'v1', shifts: shifts }];
  state.employees = [{ id: 'ava', name: 'Ava Nguyen' }];
  opsVenueNow = function () { return { iso: '2026-10-09', minute: 20 * 60 }; };
  renderOnShiftNow();
  const d = dayTimelineData(shifts, venue, '2026-10-09');
  const i = d.slots.indexOf(20 * 60);
  const bar = d.rows.find(function (r) { return r.name === 'front bar'; });
  const only = dayTimelineData(shifts, venue, '2026-10-09', { area: 'front bar' });
  console.log(JSON.stringify({
    onAt: opsOnShiftAt(shifts, '2026-10-09', 20 * 60).map(function (s) { return s.id; }),
    head: el('headcount-now').textContent, sub: el('headcount-sub').textContent,
    chips: el('headcount-areas').innerHTML, list: el('on-shift-list').innerHTML,
    bar: [bar.counts[i], bar.below[i]], total: d.total[i], rows: d.rows.map(function (r) { return r.name; }),
    unfilled: d.unfilled && [d.unfilled.shifts, d.unfilled.counts[i]],
    onlyUnfilled: only.unfilled && only.unfilled.shifts,
    html: renderDayTimeline(d, { nowMinute: 20 * 60 }), timeline: el('onshift-timeline').innerHTML }));
}
""")
    assert data["onAt"] == ["a1"]
    assert data["head"] == "1"
    assert "2 open shifts unfilled" in data["sub"]
    assert '<div class="ops-area-chip below"><strong>1</strong> / 2 Front Bar</div>' in data["chips"]
    assert "Kitchen" not in data["chips"]
    lst = data["list"]
    assert "Ava Nguyen" in lst and "Staff member" not in lst
    assert "1 on (min 2)" in lst and "Open: unfilled (2)" in lst
    assert lst.count("Nobody on it") == 2
    assert data["bar"] == [1, True]                       # below the minimum: red
    assert data["total"] == 1
    assert data["rows"] == ["front bar"]
    assert data["unfilled"] == [2, 2]
    assert data["onlyUnfilled"] == 1
    assert "Open: unfilled (2)" in data["html"] and "Open: unfilled (2)" in data["timeline"]


@needs_node
def test_on_shift_reads_the_live_roster_again_when_opened_and_each_minute():
    data = _run(OPS_STUBS + _ops_js() + r"""
async function main() {
  state.venues = [venue];
  state.employees = [{ id: 'ava', name: 'Ava Nguyen' }, { id: 'meca', name: 'Meca' }, { id: 'zoe', name: 'Zoe' }];
  state.rosters = [{ venue_id: 'v1', shifts: [AVA] }];
  opsVenueNow = function () { return { iso: '2026-10-09', minute: 20 * 60 }; };
  loadSosCard = function () {};
  loadCutAdvice = function () {};
  const MECA = Object.assign({}, AVA, { id: 'm1', employee_id: 'meca' });
  const ZOE = Object.assign({}, AVA, { id: 'z1', employee_id: 'zoe' });
  let serve = [{ venue_id: 'v1', shifts: [AVA, MECA] }, { venue_id: 'v2', shifts: [ZOE] }];
  global.reads = [];
  fetchVenueRosters = async function (vid) { reads.push(vid); await tick(5); return { items: serve, total: serve.length }; };

  openOnShiftTools();
  const drawnFirst = el('headcount-now').textContent;
  await tick(30);
  const afterOpen = el('headcount-now').textContent;

  serve = [{ venue_id: 'v1', shifts: [AVA, MECA, ZOE] }];
  const minute = intervals.filter(function (x) { return x[1] === 60000; });
  minute.forEach(function (x) { x[0](); });
  await tick(30);
  const afterMinute = el('headcount-now').textContent;

  serve = [{ venue_id: 'v1', shifts: [] }];
  const late = refreshOnShiftRosters();
  state.currentVenue = 'v2';
  await late;
  console.log(JSON.stringify({ drawnFirst: drawnFirst, afterOpen: afterOpen, afterMinute: afterMinute,
    minuteTimers: minute.length, reads: reads, keptAfterSwitch: state.rosters[0].shifts.length,
    venues: state.rosters.map(function (r) { return r.venue_id; }) }));
}
""")
    assert data["drawnFirst"] == "1"                      # drawn straight away from what's in hand
    assert data["afterOpen"] == "2"                       # then from the roster as it is now
    assert data["minuteTimers"] == 1 and data["afterMinute"] == "3"
    assert data["reads"] == ["v1", "v1", "v1"]
    assert data["keptAfterSwitch"] == 3 and data["venues"] == ["v1"]


@needs_node
def test_a_day_recorded_closed_has_no_minimums_on_shift_or_in_the_day_view():
    data = _run(OPS_STUBS + _ops_js() + r"""
async function main() {
  // Friday recorded closed (stray hours alongside are ignored); Ava is in for a stocktake.
  venue.trading_hours = { fri: { closed: true, open: '11:00', close: '00:00' }, sat: { closed: true } };
  state.venues = [venue];
  state.rosters = [{ venue_id: 'v1', shifts: [AVA] }];
  state.employees = [{ id: 'ava', name: 'Ava Nguyen' }];
  opsVenueNow = function () { return { iso: '2026-10-09', minute: 20 * 60 }; };
  renderOnShiftNow();
  const d = dayTimelineData([AVA], venue, '2026-10-09');
  const html = renderDayTimeline(d);
  const empty = renderDayTimeline(dayTimelineData([], venue, '2026-10-10'));
  console.log(JSON.stringify({
    window: opsTradingWindow(venue, '2026-10-09'), chips: el('headcount-areas').innerHTML,
    label: el('headcount-areas-label').textContent, list: el('on-shift-list').innerHTML,
    closed: d.closed, below: d.below, mins: d.rows.map(function (r) { return r.min; }),
    anyOpen: d.open.some(Boolean), html: html, empty: empty }));
}
""")
    assert data["window"] is None
    assert data["chips"] == '<div class="ops-area-chip"><strong>1</strong> Front Bar</div>'
    assert "closed today" in data["label"]
    assert "1 on</span>" in data["list"] and "(min" not in data["list"] and 'class="below"' not in data["list"]
    assert data["closed"] is True and data["below"] == 0 and data["mins"] == [0]
    assert data["anyOpen"] is False
    assert "Closed all day (recorded)" in data["html"] and "No trading hours recorded" not in data["html"]
    assert " below" not in data["html"]
    assert "closed all day" in data["empty"] and "no trading hours" not in data["empty"]


def _open_world(db, vid):
    _venue(db, vid=vid, areas=[{"name": "front bar", "min_by_day": {d: 1 for d in DAYS}}])
    ava = _emp(f"{vid}-ava", "Ava Nguyen", vid=vid)
    sam = _emp(f"{vid}-sam", "Sam Lee", vid=vid)
    db.save_employees([ava, sam])
    _roster(db, [_shift(ava.id, FRI, "17:00", "23:59"), _shift(sam.id, FRI, "17:00", "23:59")], vid=vid)
    return ava, sam


@pytest.fixture
def no_afl(monkeypatch):
    async def none(venue_id, db, aware_now):
        return {"status": "ok", "games": []}
    monkeypatch.setattr(cut_advice, "_afl_games", none)


def test_cut_advice_never_counts_an_open_shift_as_a_person(no_afl, monkeypatch):
    db = get_db()
    vid = f"g1-{uuid.uuid4().hex[:6]}"
    ava, sam = _open_world(db, vid)
    at = datetime(2026, 10, 9, 21, 0)
    before = asyncio.run(cut_advice.advise(db, vid, takings=100, now=at))
    assert before["recommendation"] == "cut"            # two on, minimum one: one can go

    # Sam rings in sick at 3pm; approving the leave leaves the shift open.
    monkeypatch.setattr(leave_shifts, "_venue_now", lambda db_, v: datetime(2026, 10, 9, 15, 0))
    opened = leave_shifts.open_leave_shifts(db, vid, sam, FRI, FRI)
    assert len(opened) == 1

    r = asyncio.run(cut_advice.advise(db, vid, takings=100, now=at))
    assert r["recommendation"] == "hold" and r["candidates"] == []
    assert r["area_minimums"]["front bar"] == {"on": 1, "minimum": 1}
    assert [p["name"] for p in r["on_shift"]] == ["Ava Nguyen"] and r["on_shift_count"] == 1
    assert not any("not on file" in n for n in r["notes"])
    assert r["open_shifts"] == [{"area": "front bar", "start": "17:00", "end": "23:59"}]
    assert any(line.startswith("Open front bar shift 17:00-23:59 — nobody on it") for line in r["information"])
    assert [s.id for s, _, _ in cut_advice.on_shift_now(db, vid, at)] == [
        s.id for s in db.get_roster(f"{vid}-r-{MON.isoformat()}").shifts if s.employee_id == ava.id]

    bar = asyncio.run(cut_advice.advise(db, vid, areas=["kitchen"], takings=100, now=at))
    assert bar["open_shifts"] == []


def test_hourly_staff_count_leaves_open_shifts_out():
    from rosteriq.services.labour_tracker import LabourTracker
    tracker = LabourTracker()
    shifts = [Shift(id="p", employee_id="ava", date=FRI, start_time=time(17), end_time=time(20),
                    status=ShiftStatus.scheduled, role="front bar"),
              Shift(id="o", employee_id="", date=FRI, start_time=time(17), end_time=time(20),
                    status=ShiftStatus.scheduled, role="front bar")]
    tracker._todays_shifts = lambda venue_id, day: shifts
    assert tracker._get_staff_count_by_hour("v", FRI) == {17: 1, 18: 1, 19: 1}


@needs_node
def test_client_person_counters_leave_open_shifts_out():
    src = _dash()
    data = _run(_fns(src, "_weekHoursByEmployee", "_realReportRows") + r"""
global.state = { rosters: [{ shifts: [] }] };
global._reportRange = function () { return { s: '2000-01-01', e: '2100-01-01' }; };
async function main() {
  const now = new Date();
  const iso = now.toISOString().slice(0, 10);
  const sh = function (eid) { return { employee_id: eid, date: iso, start_time: '00:00', end_time: '23:00', break_minutes: 0, cost: 10 }; };
  state.rosters[0].shifts = [sh('ava'), sh(''), sh(''), sh('')];
  const hours = _weekHoursByEmployee();
  const rows = _realReportRows();
  console.log(JSON.stringify({ people: Object.keys(hours), staff: rows[0].staff }));
}
""")
    assert data["people"] == ["ava"]
    assert data["staff"] == 1


# --------------------------------------------------------------------------
# [35]/[44] Areas & minimums and time clock: a late answer for the old venue
# --------------------------------------------------------------------------

def _areas_js():
    d = _dash()
    return "\n".join([_top(d, "AE_DAYS"), _top(d, "AE_DAY_LABELS"), _top(d, "aeState"), _top(d, "aeLoadSeq"),
                      _fns(d, "aeAttr", "aeNorm", "aeTime", "aeNeedsSkill", "aeFollowsDefault", "aeLoad", "aeRender",
                           "_aeHint", "_aeEdit", "aeSetMin", "aeSetSkill", "aeSetFollows", "aeSetHours",
                           "aeSetClosed", "_aeAdd", "aeSave")])


AREA_STUBS = TICK + r"""
global.state = { isDemo: false, currentVenue: 'wem', venues: [] };
global.rosterSectionPills = function () {};
global.SETUP = {
  wem: { areas: [{ name: 'wem bar', label: 'Wem Bar', min_by_day: { fri: 3 }, requires_skill: false,
                   follows_demand: true, follows_demand_set: false }], trading_hours: {} },
  apo: { areas: [{ name: 'apo bar', label: 'Apo Bar', min_by_day: { fri: 2 }, requires_skill: false,
                   follows_demand: true, follows_demand_set: false }], trading_hours: {} },
};
global.DELAY = { wem: 50, apo: 5 };
global.FAIL = {};
global.posts = [];
global.fetchAPI = async function (url, opts) {
  const method = (opts && opts.method) || 'GET';
  calls.push(method + ' ' + url);
  let m = /\/api\/venue-setup\?venue_id=(\w+)/.exec(url);
  if (m) {
    await tick(DELAY[m[1]] || 1);
    if (FAIL[m[1]]) throw new Error('busy');
    return JSON.parse(JSON.stringify(SETUP[m[1]]));
  }
  m = /^\/venues\/(\w+)/.exec(url);
  if (m) return { id: m[1], name: m[1], areas: [], trading_hours: {} };
  if (method === 'POST') { posts.push({ url: url, body: JSON.parse(opts.body) }); return { status: 'created' }; }
  return {};
};
"""


@needs_node
def test_areas_editor_drops_a_late_answer_for_the_venue_just_left_and_never_saves_onto_it():
    data = _run(AREA_STUBS + _areas_js() + r"""
async function main() {
  const slow = aeLoad('settings-areas', 'wem', { save: true });
  state.currentVenue = 'apo';
  const fast = aeLoad('settings-areas', 'apo', { save: true });
  await Promise.all([slow, fast]);
  const card = { venue: aeState['settings-areas'].venueId, html: el('settings-areas').innerHTML };
  aeSetMin('settings-areas', 0, 'fri', '5');
  await aeSave('settings-areas');
  const saved = posts.splice(0);

  // A failed late answer doesn't blank the new venue's card either.
  FAIL.wem = true;
  state.currentVenue = 'wem';
  const failing = aeLoad('settings-areas', 'wem', { save: true });
  state.currentVenue = 'apo';
  const again = aeLoad('settings-areas', 'apo', { save: true });
  await Promise.all([failing, again]);
  const afterFail = { venue: aeState['settings-areas'].venueId, loaded: aeState['settings-areas'].loaded,
                      html: el('settings-areas').innerHTML };
  FAIL.wem = false;

  // The wizard's step follows its own venue: the latest load owns it.
  const w1 = aeLoad('ob-areas', 'wem', { save: false });
  const w2 = aeLoad('ob-areas', 'apo', { save: false });
  await Promise.all([w1, w2]);
  const wizard = aeState['ob-areas'].venueId;

  // Belt and braces: the card never saves one venue's areas while another is picked.
  aeState['settings-areas'].venueId = 'wem';
  aeState['settings-areas'].dirty = true;
  const refused = await aeSave('settings-areas');
  console.log(JSON.stringify({ card: card, saved: saved, afterFail: afterFail, wizard: wizard,
    refused: refused, postsAfterRefusal: posts.length, toasts: toasts }));
}
""")
    assert data["card"]["venue"] == "apo"
    assert "Apo Bar" in data["card"]["html"] and "Wem Bar" not in data["card"]["html"]
    assert len(data["saved"]) == 1
    body = data["saved"][0]["body"]
    assert body["id"] == "apo" and [a["name"] for a in body["areas"]] == ["apo bar"]
    assert body["areas"][0]["min_by_day"]["fri"] == 5
    assert data["afterFail"]["venue"] == "apo" and data["afterFail"]["loaded"] is True
    assert "Could not load" not in data["afterFail"]["html"]
    assert data["wizard"] == "apo"
    assert data["refused"] is False and data["postsAfterRefusal"] == 0
    assert any("another venue" in t["m"] for t in data["toasts"])


@needs_node
def test_time_clock_settings_drop_a_late_answer_for_the_venue_just_left():
    src = _dash()
    data = _run(TICK + _fns(src, "_timeClockVenueName", "_whenLabel", "loadTimeClockSettings", "renderKioskDevices") + r"""
global.state = { currentVenue: 'wem', venues: [{ id: 'wem', name: 'The Wembley' }, { id: 'apo', name: 'Apothecary' }] };
const ANSWERS = {
  wem: { devices: [{ id: 'd1', name: 'Wembley iPad', active: true, created_at: '2026-10-01T00:00:00' }],
         settings: { auto_meal_break: true, allow_self_clock: true } },
  apo: { devices: [], settings: { auto_meal_break: false, allow_self_clock: false } },
};
const DELAY = { wem: 40, apo: 5 };
global.fetchAPI = async function (url) {
  const vid = /venue_id=(\w+)/.exec(url)[1];
  await tick(DELAY[vid]);
  return url.indexOf('/devices') >= 0 ? { devices: ANSWERS[vid].devices } : ANSWERS[vid].settings;
};
async function main() {
  const slow = loadTimeClockSettings();
  state.currentVenue = 'apo';
  const fast = loadTimeClockSettings();
  await Promise.all([slow, fast]);
  console.log(JSON.stringify({ autoBreak: el('tc-auto-break').checked, selfClock: el('tc-self-clock').checked,
                               devices: el('tc-devices').innerHTML }));
}
""")
    assert data["autoBreak"] is False and data["selfClock"] is False
    assert "Wembley iPad" not in data["devices"] and "No time clock set up yet" in data["devices"]


# --------------------------------------------------------------------------
# [46] follows_demand survives; Closed days in the trading-hours editor
# --------------------------------------------------------------------------

@needs_node
def test_areas_editor_keeps_follows_demand_and_offers_a_toggle():
    data = _run(AREA_STUBS + _areas_js() + r"""
SETUP.apo = { trading_hours: {}, areas: [
  { name: 'front bar', label: 'Front Bar', min_by_day: { fri: 2 }, requires_skill: false, follows_demand: true, follows_demand_set: false },
  { name: 'bottle shop', label: 'Bottle Shop', min_by_day: { fri: 1 }, requires_skill: false, follows_demand: true, follows_demand_set: true },
  { name: 'restaurant', label: 'Restaurant', min_by_day: { fri: 1 }, requires_skill: false, follows_demand: true, follows_demand_set: false },
  { name: 'duty manager', label: 'Duty Manager', min_by_day: { fri: 1 }, requires_skill: true, follows_demand: false, follows_demand_set: false } ] };
async function main() {
  state.currentVenue = 'apo';
  await aeLoad('settings-areas', 'apo', { save: true });
  const html = el('settings-areas').innerHTML;
  await aeSave('settings-areas');
  const untouched = posts.splice(0)[0].body.areas;
  aeSetFollows('settings-areas', 0, false);
  aeSetSkill('settings-areas', 2, true);
  const box = el('settings-areas-fd-2').checked;
  _aeAdd('settings-areas', 'Bottle O', '');
  _aeAdd('settings-areas', 'Lounge', '');
  await aeSave('settings-areas');
  console.log(JSON.stringify({ html: html, untouched: untouched, edited: posts[0].body.areas, box: box,
    added: aeState['settings-areas'].areas.slice(4).map(function (a) { return [a.name, a.follows_demand]; }) }));
}
""")
    html = data["html"]
    assert "More when busy" in html
    assert 'id="settings-areas-fd-1" checked' in html and 'id="settings-areas-fd-3"' in html
    assert 'id="settings-areas-fd-3" checked' not in html
    fd = {a["name"]: a.get("follows_demand", "unset") for a in data["untouched"]}
    # Only the venue's own setting goes back; a default is left to the server.
    assert fd == {"front bar": "unset", "bottle shop": True, "restaurant": "unset", "duty manager": "unset"}
    fd = {a["name"]: a.get("follows_demand", "unset") for a in data["edited"]}
    assert fd["front bar"] is False and fd["bottle shop"] is True
    assert fd["restaurant"] == "unset" and data["box"] is False   # a default follows "needs skill"
    assert fd["bottle o"] == "unset" and fd["lounge"] == "unset"
    assert data["added"] == [["bottle o", False], ["lounge", True]]


def _owner(c):
    email = f"wg1{uuid.uuid4().hex[:8]}@wembley-synthetic.com.au"
    c.post("/api/auth/register", json={"email": email, "password": PW, "name": "Ben"})
    tok = c.post("/api/auth/login", json={"email": email, "password": PW}).json()["access_token"]
    return {"Authorization": f"Bearer {tok}"}


def _venue_body(vid, areas):
    return {"id": vid, "name": "The Wembley Hotel", "state": "wa", "max_labour_pct": 30, "tanda_org_id": "",
            "created_at": "2026-07-01T00:00:00", "areas": areas}


def test_saving_areas_without_follows_demand_keeps_the_stored_setting():
    from rosteriq.services.venue_areas import venue_areas
    c = TestClient(app)
    h = _owner(c)
    vid = f"wg1-{uuid.uuid4().hex[:8]}"
    mins = {d: 1 for d in DAYS}
    r = c.post("/venues", json=_venue_body(vid, [
        {"name": "front bar", "min_by_day": mins},
        {"name": "bottle shop", "min_by_day": mins, "follows_demand": True},
        {"name": "restaurant", "min_by_day": mins, "follows_demand": False}]), headers=h)
    assert r.status_code == 200, r.text

    setup = c.get(f"/api/venue-setup?venue_id={vid}", headers=h).json()
    flags = {a["name"]: (a["follows_demand"], a["follows_demand_set"]) for a in setup["areas"]}
    assert flags == {"front bar": (True, False), "bottle shop": (True, True), "restaurant": (False, True)}

    # An older client (or one that leaves it out) posts the areas without the key.
    r = c.post("/venues", json=_venue_body(vid, [
        {"name": "front bar", "min_by_day": mins},
        {"name": "bottle shop", "min_by_day": {**mins, "fri": 2}},
        {"name": "restaurant", "min_by_day": mins, "follows_demand": True}]), headers=h)
    assert r.status_code == 200, r.text
    stored = {a["name"]: a for a in get_db().get_venue(vid).areas}
    assert stored["bottle shop"]["follows_demand"] is True and stored["bottle shop"]["min_by_day"]["fri"] == 2
    assert stored["restaurant"]["follows_demand"] is True          # sent explicitly: the new value wins
    assert "follows_demand" not in stored["front bar"]
    effective = {a["name"]: a["follows_demand"] for a in venue_areas(get_db().get_venue(vid))}
    assert effective == {"front bar": True, "bottle shop": True, "restaurant": True}


@needs_node
def test_trading_hours_editor_records_a_closed_day():
    data = _run(AREA_STUBS + _areas_js() + r"""
SETUP.apo.trading_hours = { mon: { closed: true }, fri: { open: '11:00', close: '01:00' } };
async function main() {
  state.currentVenue = 'apo';
  await aeLoad('settings-areas', 'apo', { save: true });
  const html = el('settings-areas').innerHTML;
  const loaded = JSON.parse(JSON.stringify(aeState['settings-areas'].hours.mon));
  aeSetHours('settings-areas', 'tue', 'open', '10:00');
  aeSetClosed('settings-areas', 'tue', true);
  const tueDisabled = [el('settings-areas-open-tue').disabled, el('settings-areas-close-tue').disabled];
  const hint = el('settings-areas-hint-tue').textContent;
  const ok = await aeSave('settings-areas');
  const first = posts.splice(0)[0];
  aeSetClosed('settings-areas', 'mon', false);
  const monEnabled = !el('settings-areas-open-mon').disabled;
  aeSetHours('settings-areas', 'mon', 'open', '12:00');
  aeSetHours('settings-areas', 'mon', 'close', '22:00');
  await aeSave('settings-areas');
  console.log(JSON.stringify({ html: html, loaded: loaded, tueDisabled: tueDisabled, hint: hint, ok: ok,
    first: first, second: posts[0], monEnabled: monEnabled }));
}
""")
    html = data["html"]
    assert data["loaded"]["closed"] is True
    assert 'id="settings-areas-open-mon" value="" disabled' in html
    assert 'id="settings-areas-close-mon" value="" disabled' in html
    assert '<input type="checkbox" checked aria-label="Closed all day Mon"' in html
    assert 'id="settings-areas-open-fri" value="11:00" aria' in html
    assert "Closed all day" in html and "<th>Closed</th>" in html
    assert data["tueDisabled"] == [True, True] and data["hint"] == "Closed all day"
    assert data["ok"] is True
    assert data["first"]["url"] == "/venues"
    assert data["first"]["body"]["trading_hours"] == {"mon": {"closed": True}, "tue": {"closed": True},
                                                      "fri": {"open": "11:00", "close": "01:00"}}
    assert data["monEnabled"] is True
    assert data["second"]["body"]["trading_hours"] == {"mon": {"open": "12:00", "close": "22:00"},
                                                       "tue": {"closed": True},
                                                       "fri": {"open": "11:00", "close": "01:00"}}


# --------------------------------------------------------------------------
# [45] Roster re-paste: removed and changed shifts
# --------------------------------------------------------------------------

def _repaste_responses():
    c = TestClient(app)
    h = _owner(c)
    vid = f"wg1-{uuid.uuid4().hex[:8]}"
    assert c.post("/venues", json=_venue_body(vid, []), headers=h).status_code == 200
    staff = "Name,Email,Employment Type,Hourly Rate\nJo <b>Smith</b>,jo@x.com,casual,30\nAl Brown,al@x.com,casual,30\n"
    assert c.post("/api/setup/import-staff", json={"venue_id": vid, "content": staff}, headers=h).status_code == 200

    def paste(content):
        r = c.post("/api/setup/import-roster", json={"venue_id": vid, "content": content}, headers=h)
        assert r.status_code == 200, r.text
        return r.json()

    paste("Name,Date,Start,End\nJo <b>Smith</b>,09/10/2026,17:00,22:00\nJo <b>Smith</b>,10/10/2026,17:00,22:00\n"
          "Al Brown,09/10/2026,10:00,16:00\n")
    removed = paste("Name,Date,Start,End\nJo <b>Smith</b>,10/10/2026,17:00,22:00\nAl Brown,09/10/2026,10:00,16:00\n")
    changed = paste("Name,Date,Start,End,Break\nJo <b>Smith</b>,10/10/2026,17:00,22:00,30\nAl Brown,09/10/2026,10:00,16:00,0\n")
    return removed, changed


@needs_node
def test_roster_repaste_reports_removed_and_changed_shifts_and_refreshes():
    removed, changed = _repaste_responses()
    assert removed["shifts_imported"] == 0 and removed["shifts_removed"] == 1
    assert changed["shifts_imported"] == 0 and changed["shifts_replaced"] == 1
    src = _dash()
    data = _run(_fns(src, "importFlagText", "importFlagsSummary", "_importExtras", "renderRosterImportReport",
                     "runRosterImport") + r"""
global.state = { currentVenue: 'wem' };
global.ANSWERS = [%s, %s];
global.loadVenueData = function () { log.push('data'); };
global.refreshRealRoster = function () { log.push('roster'); };
global.fetchAPI = async function () { return ANSWERS.shift(); };
async function main() {
  el('roster-import-text').value = 'x';
  el('roster-import-location').value = '';
  await runRosterImport();
  const first = { report: el('roster-import-report').innerHTML, result: el('roster-import-result').textContent,
                  toast: toasts[0], log: log.splice(0) };
  await runRosterImport();
  console.log(JSON.stringify({ first: first, second: { report: el('roster-import-report').innerHTML,
    result: el('roster-import-result').textContent, toast: toasts[1], log: log } }));
}
""" % (json.dumps(removed), json.dumps(changed)))
    first, second = data["first"], data["second"]
    assert "1 shift removed — no longer in the export" in first["report"]
    assert "Removed: no longer in the export" in first["report"]
    assert "Jo &lt;b&gt;Smith&lt;/b&gt;" in first["report"] and "<b>Smith</b>" not in first["report"]
    assert "2026-10-09" in first["report"] and "17:00–22:00" in first["report"]
    assert "1 removed" in first["result"]
    assert first["toast"]["k"] == "success" and first["log"] == ["data", "roster"]
    assert "1 shift changed to match the export" in second["report"]
    assert "meal break or area changed" in second["report"]
    assert "1 changed" in second["result"]
    assert second["toast"]["k"] == "success" and second["log"] == ["data", "roster"]
