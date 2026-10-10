"""Wembley pilot, dashboard and staff-app JavaScript:

* Import reports say which location was kept and how many rows were dropped;
  a multi-location export the server can't place offers a location picker
  and re-sends the import with the choice. Roster imports show their flags.
* A failed token refresh ends the session only when the server refuses the
  refresh token. Busy, broken or offline keeps the pair and says try again;
  tabs share one exchange instead of revoking each other's tokens.
* A venue switch never leaves the old venue's numbers on screen, and a slow
  response for the old venue never paints the new one.
* The optimiser panel never shows invented results on a real venue.

As in test_wembley_a6, the JS is pulled out of the real pages and run under
node with a small fake DOM.
"""
import json
import re
import uuid

from fastapi.testclient import TestClient

from rosteriq.api import app
from rosteriq.tests.test_wembley_a6 import _dash, _fn, _fns, _html, _main, _run, _top, needs_node

RESP = r"""
function resp(status, body, hdrs) {
  hdrs = hdrs || {};
  return { ok: status >= 200 && status < 300, status: status, statusText: String(status),
           headers: { get: function (k) { k = String(k).toLowerCase();
             if (k in hdrs) return hdrs[k];
             return k === 'content-type' ? 'application/json' : null; } },
           json: async function () { return body; }, text: async function () { return JSON.stringify(body); } };
}
const tick = function (ms) { return new Promise(function (r) { setTimeout(r, ms || 0); }); };
"""


# --------------------------------------------------------------------------
# Import location: report and picker (findings 14/29/30)
# --------------------------------------------------------------------------

def _owner(c):
    email = f"wf3{uuid.uuid4().hex[:8]}@wembley-synthetic.com.au"
    c.post("/api/auth/register", json={"email": email, "password": "Passw0rd!234", "name": "Ben"})
    tok = c.post("/api/auth/login", json={"email": email, "password": "Passw0rd!234"}).json()["access_token"]
    return {"Authorization": f"Bearer {tok}"}


def _venue(c, h, name):
    vid = f"wf3-{uuid.uuid4().hex[:8]}"
    r = c.post("/venues", json={"id": vid, "name": name, "state": "wa", "max_labour_pct": 30,
                                "tanda_org_id": "", "created_at": "2026-07-01T00:00:00"}, headers=h)
    assert r.status_code == 200, r.text
    return vid


STAFF_CSV = ("First Name,Last Name,Email,Pay Rate,Employment Type,Location\n"
             "Amy,Lee,amy@wembley-synthetic.com.au,30.00,casual,Wembley Hotel Pty Ltd\n"
             "Bob,Ray,bob@wembley-synthetic.com.au,30.00,casual,Wembley Hotel Pty Ltd\n"
             "Cat,Day,cat@wembley-synthetic.com.au,30.00,casual,Apothecary Murdoch Square\n")
ROSTER_CSV = ("Name,Date,Start,End,Break,Location\n"
              "Amy Lee,05/10/2026,09:00,17:00,9,Wembley Hotel Pty Ltd\n"
              "Bob Ray,05/10/2026,09:00,17:00,\"0,5\",Wembley Hotel Pty Ltd\n"
              "Cat Day,05/10/2026,09:00,17:00,30,Apothecary Murdoch Square\n")
KEEP = "Wembley Hotel Pty Ltd"


def _server_exchanges():
    """What the server really answers: a venue called 'TWH' can't be matched
    to either Deputy location, so each import asks; naming one imports it."""
    c = TestClient(app)
    h = _owner(c)
    vid = _venue(c, h, "TWH")
    out = {"vid": vid}
    for kind, csv in (("staff", STAFF_CSV), ("roster", ROSTER_CSV)):
        url = "/api/setup/import-" + kind
        ask = c.post(url, json={"venue_id": vid, "content": csv, "update_existing": False}, headers=h)
        assert ask.status_code == 422, ask.text
        done = c.post(url, json={"venue_id": vid, "content": csv, "update_existing": False, "location": KEEP},
                      headers=h)
        assert done.status_code == 200, done.text
        out[kind] = {"ask": ask.json(), "done": done.json()}
    assert out["roster"]["done"]["location"]["kept"] == KEEP
    assert len(out["roster"]["done"]["flagged"]) == 2
    return out


def _import_js():
    return _top(_dash(), "_refreshInFlight") + "\n" + _top(_dash(), "_sessionEnding") + "\n" + _fns(
        _dash(), "_refreshAccessToken", "_sessionExpired", "fetchAPI",
        "importFlagText", "importFlagsSummary", "staffImportCounts", "_importExtras",
        "importLocationChoices", "showImportLocationPicker",
        "renderStaffImportReport", "runStaffImport", "renderRosterImportReport", "runRosterImport",
        "obImportStaff")


def _fake_server(ex):
    return RESP + r"""
global.EX = """ + json.dumps(ex) + r""";
global.state = { baseUrl: '', authToken: 'tok', isDemo: false, currentVenue: EX.vid };
global.obState = { venueId: EX.vid };
global._lastErrorReference = null;
global.loadVenueData = function () { log.push('reload'); };
global.refreshRealRoster = function () { log.push('roster'); };
global.fetch = async function (url, opts) {
  const body = JSON.parse(opts.body);
  calls.push({ url: url, body: body });
  const kind = url.indexOf('import-roster') >= 0 ? 'roster' : 'staff';
  return body.location ? resp(200, EX[kind].done) : resp(422, EX[kind].ask);
};
async function choose(reportId, label) {
  const box = el(reportId).children[el(reportId).children.length - 1];
  const buttons = box.children.filter(function (c) { return c.tagName === 'BUTTON'; });
  const pick = buttons.find(function (b) { return b.textContent === label; });
  await pick.fire('click');
  await tick(20);
  return buttons.map(function (b) { return b.textContent; });
}
"""


@needs_node
def test_staff_import_asks_which_location_and_resends_with_the_choice():
    ex = _server_exchanges()
    data = _run(_fake_server(ex) + _import_js() + r"""
async function main() {
  el('staff-import-text').value = 'pasted';
  await runStaffImport();
  const offered = await choose('staff-import-report', '""" + KEEP + r"""');
  console.log(JSON.stringify({ offered: offered, calls: calls, toasts: toasts,
    field: el('staff-import-location').value, report: el('staff-import-report').innerHTML }));
}
""")
    assert data["offered"] == ["Wembley Hotel Pty Ltd", "Apothecary Murdoch Square"]
    assert "location" not in data["calls"][0]["body"]
    assert data["calls"][1]["body"]["location"] == KEEP
    assert data["calls"][1]["body"]["update_existing"] is False
    assert data["field"] == KEEP
    assert not any("Import failed" in t["m"] for t in data["toasts"])
    rep = data["report"]
    assert "[object Object]" not in rep
    assert "Kept rows for <b>Wembley Hotel Pty Ltd</b>" in rep
    assert "1 row for other locations skipped" in rep
    assert "from the Location column" in rep


@needs_node
def test_roster_import_asks_which_location_and_reports_its_flags():
    ex = _server_exchanges()
    data = _run(_fake_server(ex) + _import_js() + r"""
async function main() {
  el('roster-import-text').value = 'pasted';
  await runRosterImport();
  const offered = await choose('roster-import-report', '""" + KEEP + r"""');
  console.log(JSON.stringify({ offered: offered, calls: calls,
    result: el('roster-import-result').textContent, report: el('roster-import-report').innerHTML }));
}
""")
    assert KEEP in data["offered"]
    assert data["calls"][1]["body"]["location"] == KEEP
    rep = data["report"]
    assert "[object Object]" not in rep
    assert "Kept rows for <b>Wembley Hotel Pty Ltd</b>" in rep
    # each flagged shift: line, who, date and what to check
    assert "2 imported shifts need checking" in rep
    assert "Amy Lee" in rep and "Bob Ray" in rep and "2026-10-05" in rep
    assert "read as 540 minutes" in rep
    assert "couldn&#39;t be read" in rep
    assert "unusually long meal break" in rep and "unreadable meal break" in rep
    assert "2 to check" in data["result"]


@needs_node
def test_roster_import_report_lists_flagged_shifts():
    resp = {"status": "imported", "shifts_imported": 3, "weeks": 1, "skipped_count": 0, "rows": [],
            "flagged": [{"line": 2, "name": "Amy <b>Lee</b>", "date": "2026-10-05", "flag": "break_over_120",
                         "detail": "meal break '9' read as 540 minutes — check it"},
                        {"line": 3, "name": "Bob Ray", "date": "2026-10-05", "flag": "break_unreadable", "detail": ""}],
            "flags_summary": {"break_over_120": 1, "break_unreadable": 1}}
    js = _fns(_dash(), "importFlagText", "importFlagsSummary", "_importExtras", "renderRosterImportReport")
    data = _run(js + r"""
async function main() {
  console.log(JSON.stringify({ report: renderRosterImportReport(""" + json.dumps(resp) + r""") }));
}
""")
    rep = data["report"]
    assert "2 imported shifts need checking" in rep
    assert "Amy &lt;b&gt;Lee&lt;/b&gt;" in rep and "<b>Lee</b>" not in rep
    assert "read as 540 minutes" in rep and "2026-10-05" in rep
    assert "Bob Ray" in rep and "unreadable meal break" in rep
    assert "1 × unusually long meal break" in rep


@needs_node
def test_wizard_staff_import_offers_the_same_picker():
    ex = _server_exchanges()
    data = _run(_fake_server(ex) + _import_js() + r"""
async function main() {
  el('ob-staff-csv').value = 'pasted';
  await obImportStaff();
  const offered = await choose('ob-staff-report', '""" + KEEP + r"""');
  console.log(JSON.stringify({ offered: offered, calls: calls, out: el('ob-staff-result').textContent,
    report: el('ob-staff-report').innerHTML }));
}
""")
    assert KEEP in data["offered"]
    assert data["calls"][1]["body"]["location"] == KEEP
    assert "Import failed" not in data["out"]
    assert "Kept rows for <b>Wembley Hotel Pty Ltd</b>" in data["report"]


@needs_node
def test_location_choice_read_from_a_structured_422_or_the_message():
    js = _fn(_dash(), "importLocationChoices")
    data = _run(js + r"""
function err(status, message, data) { const e = new Error(message); e.status = status; e.data = data; return e; }
async function main() {
  const structured = importLocationChoices(err(422, 'Choose the location to import',
    { error: { code: 'location_choice_needed', message: 'Choose', locations: ['Front Bar', 'Kitchen', 'Front Bar'] } }));
  const detail = importLocationChoices(err(422, 'Choose',
    { detail: { code: 'location_choice_needed', locations: ['A (Perth)', 'B'] } }));
  const codeOnly = importLocationChoices(err(422, 'Pick one', { error: { code: 'location_choice_needed' } }));
  const named = importLocationChoices(err(422,
    "No rows have location 'Wemb'. Locations in this export: St. Kilda, The Wembley Hotel.", {}));
  const other = importLocationChoices(err(422, 'Export is missing required column(s): name or email', {}));
  const notFound = importLocationChoices(err(404, 'Venue not found', {}));
  console.log(JSON.stringify({ structured, detail, codeOnly, named, other, notFound }));
}
""")
    assert data["structured"] == ["Front Bar", "Kitchen"]
    assert data["detail"] == ["A (Perth)", "B"]
    assert data["codeOnly"] == []
    assert data["named"] == ["St. Kilda", "The Wembley Hotel"]
    assert data["other"] is None and data["notFound"] is None


@needs_node
def test_typed_location_is_sent_and_cleared_for_the_next_venue():
    js = _fns(_dash(), "importFlagText", "importFlagsSummary", "staffImportCounts", "_importExtras",
              "importLocationChoices", "showImportLocationPicker", "renderStaffImportReport",
              "runStaffImport", "openStaffImport", "renderRosterImportReport", "runRosterImport",
              "openRosterImport")
    data = _run(r"""
global.state = { currentVenue: 'wem' };
global.loadVenueData = function () {};
global.refreshRealRoster = function () {};
global.fetchAPI = async function (url, opts) { calls.push(JSON.parse(opts.body));
  return { status: 'imported', created_count: 1, shifts_imported: 1, weeks: 1 }; };
""" + js + r"""
async function main() {
  el('staff-import-text').value = 'x';
  el('staff-import-location').value = '  Front Bar ';
  await runStaffImport();
  el('roster-import-text').value = 'x';
  el('roster-import-location').value = 'Front Bar';
  await runRosterImport();
  openStaffImport(); openRosterImport();
  console.log(JSON.stringify({ calls: calls, staffField: el('staff-import-location').value,
    rosterField: el('roster-import-location').value }));
}
""")
    assert data["calls"][0]["location"] == "Front Bar"
    assert data["calls"][1]["location"] == "Front Bar"
    assert data["staffField"] == "" and data["rosterField"] == ""
    html = _html("dashboard.html")
    for el_id in ("staff-import-location", "roster-import-location", "ob-staff-location"):
        assert 'id="%s"' % el_id in html, el_id
    assert "ob-staff-location" in _fn(_dash(), "showOnboardingWizard")


@needs_node
def test_import_location_values_are_escaped():
    js = _fns(_dash(), "_importExtras", "showImportLocationPicker")
    data = _run(js + r"""
async function main() {
  const extras = _importExtras({ location: { column: '<i>Loc</i>', kept: '<img src=x onerror=alert(1)>', skipped_other: 2 } });
  showImportLocationPicker(el('rep'), 'loc', ['<b>Bar</b>'], 'm', function () {});
  const btn = el('rep').children[0].children[1];
  console.log(JSON.stringify({ extras: extras, btnText: btn.textContent, btnHtml: btn.innerHTML }));
}
""")
    assert "<img" not in data["extras"] and "&lt;img" in data["extras"]
    assert "<i>" not in data["extras"]
    assert "2 rows for other locations skipped" in data["extras"]
    assert data["btnText"] == "<b>Bar</b>" and data["btnHtml"] == ""


# --------------------------------------------------------------------------
# Token refresh: only a refusal ends the session (findings 28/34)
# --------------------------------------------------------------------------

SERVER = RESP + r"""
// /api/auth/refresh rotates: the current refresh token works once, then is
// revoked. Only the newest access token is accepted anywhere else.
global.server = { rt: 'rt-0', at: null, n: 0, refreshes: 0, mode: 'ok', retryAfter: null };
global.serverFetch = async function (url, opts) {
  opts = opts || {};
  await tick(5);
  if (url.indexOf('/api/auth/refresh') >= 0) {
    server.refreshes++;
    if (server.mode === 'offline') throw new TypeError('Failed to fetch');
    if (server.mode === '429') return resp(429, { detail: 'Rate limit exceeded' },
                                          server.retryAfter ? { 'retry-after': server.retryAfter } : {});
    if (server.mode === '503') return resp(503, { error: { message: 'Service unavailable' } });
    if (server.mode === '403') return resp(403, { detail: 'Account disabled' });
    const sent = JSON.parse(opts.body).refresh_token;
    if (sent !== server.rt) return resp(401, { detail: 'Invalid or expired refresh token' });
    server.n++; server.rt = 'rt-' + server.n; server.at = 'at-' + server.n;
    return resp(200, { access_token: server.at, refresh_token: server.rt });
  }
  const auth = (opts.headers || {})['Authorization'] || '';
  if (server.at && auth === 'Bearer ' + server.at) return resp(200, { ok: true, url: url });
  return resp(401, { detail: 'Invalid or expired token' });
};
const vm = require('vm');
global.shared = mkStorage({ jwt_token: 'at-0', jwt_refresh_token: 'rt-0' });
function tabContext(withLocks, extra) {
  const ctx = Object.assign({
    localStorage: shared, sessionStorage: mkStorage(), fetch: serverFetch, console: console,
    window: { location: { href: '', reload: function () { this.reloaded = true; } } },
  }, extra || {});
  if (withLocks) ctx.navigator = navigator;
  vm.createContext(ctx);
  return ctx;
}
global.dashTab = function (withLocks, token) {
  const ctx = tabContext(withLocks, { state: { baseUrl: '', authToken: token || 'at-0', isDemo: false } });
  vm.runInContext(DASH_JS + '\nvar _lastErrorReference = null;', ctx);
  ctx.call = async function (path) {
    try { return { ok: await ctx.fetchAPI(path) }; }
    catch (e) { return { error: e.message, retryable: !!e.retryable, retryAfter: e.retryAfter || 0 }; }
  };
  ctx.href = function () { return ctx.window.location.href; };
  return ctx;
};
global.myTab = function (withLocks, token) {
  const ctx = tabContext(withLocks);
  vm.runInContext('var demoStaff = false; var token = ' + JSON.stringify(token || 'at-0') +
    '; var demoRetried = false; var reauthing = false; var refreshing = null;\n' + MY_JS, ctx);
  ctx.call = async function (path) {
    try { return { ok: await ctx.api(path) }; }
    catch (e) { return { error: e.message, retryable: !!e.retryable, retryAfter: e.retryAfter || 0 }; }
  };
  ctx.href = function () { return ctx.window.location.href; };
  return ctx;
};
"""


def _refresh_js():
    dash = _top(_dash(), "_refreshInFlight") + "\n" + _top(_dash(), "_sessionEnding") + "\n" + _fns(
        _dash(), "_refreshAccessToken", "_sessionExpired", "fetchAPI")
    my = _fns(_main("my.html"), "refreshSession", "api", "apiErrorMessage")
    return "global.DASH_JS = %s;\nglobal.MY_JS = %s;\n" % (json.dumps(dash), json.dumps(my)) + SERVER


@needs_node
def test_rate_limited_refresh_keeps_the_session_and_waits_out_retry_after():
    data = _run(_refresh_js() + r"""
async function main() {
  const tab = dashTab(true);
  server.mode = '429'; server.retryAfter = '30';
  const first = await tab.call('/venues');
  const again = await tab.call('/venues');           // inside the Retry-After window
  const kept = { at: shared.getItem('jwt_token'), rt: shared.getItem('jwt_refresh_token') };
  const refreshesWhileWaiting = server.refreshes;
  const href = tab.href();
  shared.setItem('rosteriq_refresh_retry_at', '0');  // the window has passed
  server.mode = 'ok';
  const later = await tab.call('/venues');
  console.log(JSON.stringify({ first, again, kept, refreshesWhileWaiting, href, later,
    stored: shared.getItem('jwt_token'), hrefAfter: tab.href() }));
}
""")
    assert data["first"]["retryable"] and data["first"]["retryAfter"] == 30
    assert "30 s" in data["first"]["error"] and "still signed in" in data["first"]["error"]
    assert data["again"]["retryable"]
    assert data["refreshesWhileWaiting"] == 1
    assert data["kept"] == {"at": "at-0", "rt": "rt-0"}
    assert data["href"] == ""
    assert data["later"]["ok"]["ok"] is True
    assert data["stored"] == "at-1" and data["hrefAfter"] == ""


@needs_node
def test_server_error_or_dropped_connection_keeps_the_session():
    data = _run(_refresh_js() + r"""
async function main() {
  const out = {};
  for (const mode of ['503', 'offline']) {
    const tab = dashTab(true);
    server.mode = mode;
    const r = await tab.call('/venues');
    out[mode] = { r: r, href: tab.href(), at: shared.getItem('jwt_token'), rt: shared.getItem('jwt_refresh_token') };
  }
  server.mode = 'ok';
  const tab = dashTab(true);
  out.recovered = await tab.call('/venues');
  console.log(JSON.stringify(out));
}
""")
    for mode in ("503", "offline"):
        r = data[mode]
        assert r["r"]["retryable"], mode
        assert r["href"] == "", mode
        assert r["at"] == "at-0" and r["rt"] == "rt-0", mode
    assert data["recovered"]["ok"]["ok"] is True


@needs_node
def test_refused_refresh_still_ends_the_session():
    data = _run(_refresh_js() + r"""
async function main() {
  const tab = dashTab(true);
  server.mode = '403';
  const r = await tab.call('/venues');
  console.log(JSON.stringify({ r: r, href: tab.href(), at: shared.getItem('jwt_token'), rt: shared.getItem('jwt_refresh_token') }));
}
""")
    assert not data["r"]["retryable"]
    assert "login" in data["href"]
    assert data["at"] is None and data["rt"] is None


@needs_node
def test_two_dashboard_tabs_refreshing_together_both_stay_signed_in():
    data = _run(_refresh_js() + r"""
async function main() {
  const out = {};
  for (const withLocks of [true, false]) {
    server.rt = 'rt-0'; server.at = null; server.n = 0; server.refreshes = 0; server.mode = 'ok';
    shared.setItem('jwt_token', 'at-0'); shared.setItem('jwt_refresh_token', 'rt-0');
    const a = dashTab(withLocks), b = dashTab(withLocks);
    const [ra, rb] = await Promise.all([a.call('/employees'), b.call('/rosters')]);
    out[withLocks ? 'locks' : 'nolocks'] = { ra: ra, rb: rb, hrefs: [a.href(), b.href()],
      stored: shared.getItem('jwt_refresh_token'), live: server.rt, refreshes: server.refreshes };
  }
  console.log(JSON.stringify(out));
}
""")
    for mode in ("locks", "nolocks"):
        r = data[mode]
        assert r["ra"]["ok"]["ok"] and r["rb"]["ok"]["ok"], (mode, r)
        assert r["hrefs"] == ["", ""], mode
        assert r["stored"] == r["live"], mode
    assert data["locks"]["refreshes"] == 1


@needs_node
def test_dashboard_and_staff_app_share_one_exchange():
    data = _run(_refresh_js() + r"""
async function main() {
  const dash = dashTab(true), my = myTab(true);
  const [rd, rm] = await Promise.all([dash.call('/venues'), my.call('/api/me/shifts')]);
  console.log(JSON.stringify({ rd: rd, rm: rm, hrefs: [dash.href(), my.href()], refreshes: server.refreshes,
    stored: shared.getItem('jwt_refresh_token'), live: server.rt }));
}
""")
    assert data["rd"]["ok"]["ok"] and data["rm"]["ok"]["ok"]
    assert data["hrefs"] == ["", ""]
    assert data["refreshes"] == 1
    assert data["stored"] == data["live"]


@needs_node
def test_staff_app_keeps_people_signed_in_when_the_refresh_is_rate_limited():
    data = _run(_refresh_js() + r"""
async function main() {
  const my = myTab(true);
  server.mode = '429'; server.retryAfter = '20';
  const first = await my.call('/api/me/shifts');
  const again = await my.call('/api/me/leave');
  console.log(JSON.stringify({ first, again, href: my.href(), refreshes: server.refreshes,
    at: shared.getItem('jwt_token'), rt: shared.getItem('jwt_refresh_token') }));
}
""")
    assert data["first"]["retryable"] and "20 s" in data["first"]["error"]
    assert data["again"]["retryable"]
    assert data["refreshes"] == 1
    assert data["href"] == ""
    assert data["at"] == "at-0" and data["rt"] == "rt-0"


@needs_node
def test_a_borrowed_token_that_has_expired_still_gets_a_real_exchange():
    data = _run(_refresh_js() + r"""
async function main() {
  // another tab stored a token an hour ago; it has expired too
  shared.setItem('jwt_token', 'at-old-other-tab');
  const dash = dashTab(false, 'at-mine'), my = myTab(false, 'at-mine');
  const rd = await dash.call('/venues');
  const rm = await my.call('/api/me/shifts');
  console.log(JSON.stringify({ rd: rd, rm: rm, hrefs: [dash.href(), my.href()], refreshes: server.refreshes }));
}
""")
    assert data["rd"]["ok"]["ok"] and data["rm"]["ok"]["ok"]
    assert data["hrefs"] == ["", ""]
    assert data["refreshes"] == 1          # the dashboard exchanged; /my took up its pair


@needs_node
def test_venue_list_failure_from_a_busy_refresh_is_not_the_setup_wizard():
    js = _fn(_dash(), "loadVenues")
    data = _run(r"""
global.state = { isDemo: false };
global.showOnboardingWizard = function () { log.push('wizard'); };
global.fetchAPI = async function () { const e = new Error('busy — try again in 30 s.'); e.retryable = true; throw e; };
""" + js + r"""
async function main() {
  await loadVenues();
  console.log(JSON.stringify({ log: log, toasts: toasts }));
}
""")
    assert data["log"] == []
    assert any("try again" in t["m"] for t in data["toasts"])


# --------------------------------------------------------------------------
# Venue switching (findings 31/32)
# --------------------------------------------------------------------------

HOME_STUBS = RESP + r"""
global.state = { currentVenue: 'wem', currentPage: 'dashboard', isDemo: false, employees: [], rosters: [],
  ws: null, venues: [{ id: 'wem' }, { id: 'apo' }] };
global._getVenueId = function () { return state.currentVenue; };
global.annAudience = [];
['annAudiencePills', 'showLoading', 'clearState', 'showEmptyVenueStates', 'initRevenueLabourChart',
 'initLabourPctTrendChart', 'initHourlyBreakdownChart', 'renderStaffTable', 'checkDeputyStatus',
 'checkMYOBStatus', 'checkHumanForceStatus', 'loadSetupWizard', 'loadBriefing', 'loadSnapshot',
 'loadRealRevenueCharts', 'fetchAndUpdateLiveRevenue', 'connectWebSocket', 'clearAIChat', 'showPage'].forEach(function (n) {
  global[n] = function () { log.push(n); };
});
global.connectionManager = { hide() {}, show() {} };
global.wsReconnectTimeout = null;
['tsRows', 'lvRows', 'anRows', 'mfPosts', 'sopDocs', 'cvRows', 'ivRows', 'ivOpenStocktakes', 'ivOrders', 'siLines',
 'ivInvoices', 'msRecipes', 'msIngredients', 'rbLines', 'piUnmapped'].forEach(function (n) { global[n] = []; });
global.ivXeroPushes = {}; global.ivMyobPushes = {};
global.ivOpenStocktake = null; global.rbEditingId = null; global.ckRun = null; global.etsIndex = null;
global.rosterSection = ''; global.ivSection = '';
const now = new Date();
const today = now.getFullYear() + '-' + String(now.getMonth() + 1).padStart(2, '0') + '-' + String(now.getDate()).padStart(2, '0');
global.fetchVenueEmployees = async function (vid) {
  return { items: vid === 'wem' ? [{ id: 'e1', name: 'Emma Thompson', venue_id: 'wem' }] : [], total: vid === 'wem' ? 1 : 0 };
};
global.fetchVenueRosters = async function (vid) {
  return { items: vid === 'wem' ? [{ id: 'r1', venue_id: 'wem', shifts: [
    { employee_id: 'e1', date: today, start_time: '00:00', end_time: '00:00', role: 'bar' }] }] : [] };
};
global.fetchAPI = async function (url) {
  calls.push(url);
  const wem = url.indexOf('venue_id=wem') >= 0;
  if (url.indexOf('/api/briefing') === 0) {
    return wem ? { today: { staff_rostered: 2, labour_cost: 827 }, approvals: { pending_leave: 2 } }
               : { today: { staff_rostered: 0, labour_cost: 0 }, approvals: {} };
  }
  if (url.indexOf('/api/snapshot') === 0) return { net_sales: 0, days: [] };
  return { items: [] };
};
"""


@needs_node
def test_switching_to_an_empty_venue_clears_the_previous_venues_tiles_and_lists():
    src = _dash()
    js = "\n".join(_top(src, n) for n in ("_ICON_WARN", "_ICON_URGENT", "_ICON_INFO")) + "\n" + _fns(
        src, "_setKpi", "updateDashboardKPIs", "populateAlerts", "renderAlerts", "populateStaffList",
        "loadVenueData", "resetVenuePageState", "onVenueSelected")
    data = _run(HOME_STUBS + js + r"""
// The home list is drawn by renderOnShiftNow (tested on its own); a stand-in
// that lists today's rostered names is enough to see the switch clear it.
function renderOnShiftNow() {
  const names = {};
  (state.employees || []).forEach(function (e) { names[e.id] = e.name; });
  const on = [];
  (state.rosters || []).forEach(function (r) { (r.shifts || []).forEach(function (s) { on.push(names[s.employee_id] || 'Staff member'); }); });
  el('on-shift-list').innerHTML = on.length ? on.join(', ') : 'Nobody rostered on right now';
}
function screen() {
  return { staff: el('kpi-staff-count').textContent, labour: el('kpi-labour-cost').textContent,
           alerts: el('alerts-panel-top').innerHTML, onShift: el('on-shift-list').innerHTML };
}
async function main() {
  await loadVenueData();
  await tick(20);
  const wem = screen();
  await onVenueSelected('apo');
  await tick(20);
  console.log(JSON.stringify({ wem: wem, apo: screen() }));
}
""")
    wem, apo = data["wem"], data["apo"]
    assert wem["staff"] == 2 and wem["labour"] == "827"
    assert "Leave waiting on you" in wem["alerts"] and "Emma Thompson" in wem["onShift"]
    assert apo["staff"] == 0
    assert apo["labour"] == "—"
    assert "Leave waiting" not in apo["alerts"] and "No active alerts" in apo["alerts"]
    assert "Emma" not in apo["onShift"] and "Nobody rostered on right now" in apo["onShift"]


@needs_node
def test_switch_blanks_the_home_tiles_before_the_new_venue_loads():
    js = _fns(_dash(), "resetVenuePageState")
    data = _run(HOME_STUBS + js + r"""
async function main() {
  el('kpi-staff-count').textContent = '2';
  el('kpi-labour-cost').textContent = '827';
  el('kpi-labour-sub').textContent = "Today's rostered labour";
  el('alerts-panel-top').innerHTML = 'Short-staffed today';
  el('alerts-count-badge').style.display = 'inline-flex';
  el('on-shift-list').innerHTML = 'Emma Thompson';
  resetVenuePageState();
  console.log(JSON.stringify({ staff: el('kpi-staff-count').textContent, labour: el('kpi-labour-cost').textContent,
    sub: el('kpi-labour-sub').textContent, alerts: el('alerts-panel-top').innerHTML,
    badge: el('alerts-count-badge').style.display, onShift: el('on-shift-list').innerHTML }));
}
""")
    assert data["staff"] == "—" and data["labour"] == "—" and data["sub"] == ""
    assert data["alerts"] == "" and data["badge"] == "none" and data["onShift"] == ""


# Every loader a venue switch re-runs. Each is started for 'wem'; the manager
# switches to 'apo' while its request is out, and nothing may land on screen.
STALE_LOADERS = [
    "updateDashboardKPIs", "populateAlerts", "loadSetupWizard", "loadBriefing", "loadSnapshot",
    "loadChecklistsBoard", "loadChecklistHistory", "loadTimesheets", "loadLeaveRequests",
    "loadCoverRequests", "loadTeamAvailability", "loadMenuPage", "loadInventory", "loadUsage",
    "loadAnnouncements", "loadManagerFeed", "loadSopLibrary", "loadSystemHealth", "loadActivity",
    "loadVenueSettings", "loadAIInsights", "checkDeputyStatus", "checkMYOBStatus", "checkHumanForceStatus",
]

STALE_STUBS = RESP + r"""
global.state = { currentVenue: 'wem', isDemo: false, baseUrl: '', token: 't', authToken: 't' };
global._getVenueId = function () { return state.currentVenue; };
global.SETUP_PAGE = {};
global.actFilter = { since: '7d', category: '' };
global._jwtClaims = function () { return {}; };
global.formatCurrency = function (v) { return '$' + v; };
global.fixPlural = function (s) { return s; };
global._ago = function () { return ''; };
global.eventCopy = function () { return ''; };
global._healthRows = function () { return 'rows'; };
global._aiInsightCard = function () { return mkEl('card'); };
global._setKpi = function (id, v) { el(id).textContent = v; };
global._ICON_WARN = global._ICON_URGENT = global._ICON_INFO = '';
global.ivXeroConnected = null; global.ivMyobConnected = null;
STUBBED.forEach(function (n) { global[n] = function () { log.push(n); }; });
// A payload that every loader would happily paint.
const ROWS = [{ id: 'w1', name: 'Wembley row', status: 'pending', employee_name: 'Emma', start_date: '2026-10-01',
  end_date: '2026-10-02', ack_stats: { outstanding_names: ['Emma'] }, reaction_counts: {}, comments: [],
  category: 'audit', item_count: 1, done_items: 0, schedule: 'daily', template_id: 't1', days: {}, constrained_days: [] }];
const PAYLOAD = { complete: false, steps: [], steps_done: 1, steps_total: 4, percent: 25, date: '2026-10-10',
  today: { staff_rostered: 9, labour_cost: 900, coverage: null }, attention: ['Wembley thing'], kitchen: { stock_value: 1 },
  approvals: { pending_leave: 3 }, team: {}, all_clear: false, net_sales: 5000, days: [1], labour_pct: 30,
  prime_cost_band: 'healthy', prime_cost_pct: 50, items: ROWS, timesheets: ROWS, requests: ROWS, covers: ROWS,
  recipes: ROWS, ingredients: ROWS, announcements: ROWS, posts: ROWS, documents: ROWS, staff: ROWS, checklists: ROWS,
  runs: ROWS, events: ROWS, insights: [{ severity: 'high' }], attention_items: [], stocktakes: ROWS, orders: ROWS,
  invoices: ROWS, pushes: [], connected: true, configured: true, name: 'The Wembley Hotel', state: 'wa',
  by_category: { audit: 1, security: 0, error: 0 }, total_stock_value: 1, low_count: 1, total_waste_value: 1,
  total_revenue_inc_gst: 1, total_cogs: 1, gross_profit: 1, food_cost_pct: 1, total_worked_minutes: 60 };
global.fetchAPI = async function (url) {
  calls.push(url);
  // the manager switched while this was out (the account-level label lookup is venue-free)
  if (url.indexOf('/api/me/access') !== 0) state.currentVenue = 'apo';
  await tick(5);
  return PAYLOAD;
};
global.fetch = async function (url) {
  calls.push(url);
  state.currentVenue = 'apo';
  await tick(5);
  return { ok: true, json: async function () { return PAYLOAD; } };
};
function snapshot() {
  const out = {};
  Object.keys(_els).forEach(function (id) {
    const e = _els[id];
    out[id] = [e.innerHTML, String(e.textContent), e.value, e.style.display || '', e.style.width || '',
               e.children.length, e.disabled];
  });
  return out;
}
"""


# What the loaders call to paint (or to load more); recorded, never run.
STALE_STUBBED = [
    "renderAlerts", "renderTimesheets", "renderLeaveRequests", "renderCoverRequests", "renderMenuRows",
    "renderIngredientRows", "renderInventory", "renderStocktake", "renderOrders", "renderInvoices",
    "renderAnnouncements", "annAudiencePills", "loadChecklistHistory", "loadSystemHealth", "loadPlatformSecurity",
    "initTimesheetDates", "updateDeputyStatus", "updateMYOBStatus", "updateHumanForceStatus",
    "_obCanManage", "aeLoad",
]


@needs_node
def test_a_late_response_for_the_previous_venue_never_paints_the_new_one():
    src = _dash()
    out = {}
    for name in STALE_LOADERS:
        stubbed = [n for n in STALE_STUBBED if n != name]
        data = _run("global.STUBBED = %s;\n" % json.dumps(stubbed) + STALE_STUBS + _fn(src, name) + r"""
async function main() {
  const pending = """ + name + r"""();
  const before = snapshot();
  log.length = 0; toasts.length = 0;
  await pending;
  await tick(30);
  const after = snapshot();
  const changed = Object.keys(after).filter(function (id) {
    return JSON.stringify(after[id]) !== JSON.stringify(before[id] || ['', '', '', '', '', 0, false]);
  });
  console.log(JSON.stringify({ changed: changed, log: log, toasts: toasts, calls: calls,
    rows: [tsRows, lvRows, cvRows, msRecipes, ivRows, anRows, mfPosts, sopDocs].map(function (r) { return (r || []).length; }) }));
}
global.tsRows = []; global.lvRows = []; global.cvRows = []; global.msRecipes = []; global.msIngredients = [];
global.ivRows = []; global.ivOpenStocktakes = []; global.ivOrders = []; global.ivInvoices = [];
global.ivXeroPushes = {}; global.ivMyobPushes = {}; global.anRows = []; global.mfPosts = []; global.sopDocs = [];
""")
        assert data["calls"], name
        out[name] = data
    bad = {n: {k: d[k] for k in ("changed", "log", "toasts", "rows")} for n, d in out.items()
           if d["changed"] or d["log"] or d["toasts"] or any(d["rows"])}
    assert not bad, bad


SWITCH_RACE_STUBS = RESP + r"""
global.state = { currentVenue: 'wem', currentPage: 'timesheets', isDemo: false, ws: null };
global.wsReconnectTimeout = null;
global.resetVenuePageState = function () {};
global.connectWebSocket = function () {};
global.DELAY = { apo: 60, bay: 200, wem: 10 };
global.loadVenueData = async function () {
  const v = state.currentVenue;
  log.push('load:' + v);
  await tick(DELAY[v] || 10);
};
global.showPage = function (p) { log.push('showPage:' + p); state.currentPage = p; };
function dim(page) { return el('page-' + page).style.pointerEvents; }
"""


@needs_node
def test_only_the_latest_of_quick_venue_switches_lifts_the_page_and_reopens_it():
    js = _fn(_dash(), "onVenueSelected")
    data = _run(SWITCH_RACE_STUBS + js + r"""
async function main() {
  const first = onVenueSelected('apo');      // 60ms
  await tick(5);
  const second = onVenueSelected('bay');     // 200ms, still loading when the first settles
  await first;
  const midway = dim('timesheets');
  await second;
  const done = dim('timesheets');
  console.log(JSON.stringify({ midway: midway, done: done, log: log }));
}
""")
    assert data["midway"] == "none"
    assert data["done"] == ""
    assert [l for l in data["log"] if l.startswith("showPage")] == ["showPage:timesheets"]


@needs_node
def test_a_page_opened_during_the_venue_load_is_not_snatched_back():
    js = _fn(_dash(), "onVenueSelected")
    data = _run(SWITCH_RACE_STUBS + js + r"""
async function main() {
  const switching = onVenueSelected('apo');
  await tick(5);
  showPage('roster');                        // the sidebar sits outside the dimmed page
  await switching;
  const single = { log: log.slice(), page: state.currentPage, timesheets: dim('timesheets') };

  // Two overlapping switches made from different pages.
  log.length = 0;
  state.currentPage = 'timesheets';
  const first = onVenueSelected('apo');
  await tick(5);
  showPage('roster');
  const second = onVenueSelected('bay');     // dims the roster page now
  await first;
  const timesheetsAfterFirst = dim('timesheets');
  const rosterWhileSecond = dim('roster');
  await second;
  console.log(JSON.stringify({ single: single, log: log, timesheets: timesheetsAfterFirst,
    rosterMid: rosterWhileSecond, roster: dim('roster') }));
}
""")
    single = data["single"]
    assert single["page"] == "roster"
    assert "showPage:timesheets" not in single["log"]
    assert single["timesheets"] == ""
    assert "showPage:timesheets" not in data["log"]
    assert data["timesheets"] == ""          # the first switch's page is never left dimmed
    assert data["rosterMid"] == "none" and data["roster"] == ""


# --------------------------------------------------------------------------
# Optimiser panel (finding 44)
# --------------------------------------------------------------------------

@needs_node
def test_optimiser_on_a_real_venue_opens_the_real_roster_generator():
    js = _fn(_dash(), "startOptimisation")
    data = _run(r"""
global.state = { currentVenue: 'wem', isDemo: false };
global.openGenerateRoster = function () { log.push('openGenerateRoster'); };
global.fetchAPI = async function (url) { calls.push(url); return { job_id: 'j1' }; };
global.pollOptimisationStatus = function () { log.push('poll'); };
global.localIso = function () { return '2026-10-10'; };
global.currentOptimisationJob = null;
""" + js + r"""
async function main() {
  await startOptimisation();
  console.log(JSON.stringify({ log: log, calls: calls }));
}
""")
    assert data["log"] == ["openGenerateRoster"]
    assert data["calls"] == []


def test_optimiser_panel_shows_nothing_invented():
    html = _html("dashboard.html")
    m = re.search(r'<div class="grid-1" id="optimiser-card"([^>]*)>', html)
    assert m and "display:none" in m.group(1).replace(" ", "")
    src = _dash()
    assert "'0.94'" not in src
    assert "8% cheaper" not in src
    assert "Roster accepted and published" not in src
    assert 'id="btn-optimiser-accept"' not in html and 'id="btn-optimiser-compare"' not in html
    assert 'id="optimiser-quality"' not in html
    assert "optimiser-card" in _fn(src, "loadDemoVenueData")
