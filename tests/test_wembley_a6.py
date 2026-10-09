"""Browser-side fixes for the Wembley pilot: staff lists that load every
person, venue switching that never leaves the old venue on screen, honest
revenue charts, a sign-up path for staff with a join code, sessions that
survive the one-hour access token, home-screen manifests, import reports and
truthful "who was messaged" toasts.

The JS under test is pulled out of the real pages and run under node with a
small fake DOM, so these exercise the shipped code rather than grepping it.
"""
import json
import os
import re
import shutil
import subprocess

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
STATIC = os.path.join(HERE, "..", "static")

NODE = shutil.which("node")
needs_node = pytest.mark.skipif(NODE is None, reason="node is required to run the page JS")


def _html(name: str) -> str:
    with open(os.path.join(STATIC, name), encoding="utf-8") as fh:
        return fh.read()


def _scripts(name: str):
    return re.findall(r"<script[^>]*>([\s\S]*?)</script>", _html(name))


def _main(name: str) -> str:
    """The page's largest inline script (the app code)."""
    return max(_scripts(name), key=len)


def _fn(src: str, name: str) -> str:
    """A top-level function declaration, up to its closing brace in column 0."""
    m = re.search(r"^(?:async )?function " + re.escape(name) + r"\(", src, re.M)
    assert m, "function %s not found" % name
    end = src.index("\n}\n", m.start()) + 2
    return src[m.start():end]


def _fns(src: str, *names: str) -> str:
    return "\n".join(_fn(src, n) for n in names)


def _top(src: str, name: str) -> str:
    """A one-line top-level const/let/var declaration."""
    m = re.search(r"^(?:const|let|var) " + re.escape(name) + r"\b.*$", src, re.M)
    assert m, "declaration %s not found" % name
    return m.group(0)


PRELUDE = r"""
function mkEl(id) {
  const s = new Set();
  const el = {
    id: id, tagName: '', className: '', innerHTML: '', textContent: '', value: '', checked: false,
    disabled: false, style: {}, children: [], parentElement: null, _l: {}, _attrs: {},
    addEventListener(t, f) { (this._l[t] = this._l[t] || []).push(f); },
    fire(t, ev) { return Promise.all((this._l[t] || []).map(f => f(ev || { preventDefault() {}, target: this }))); },
    appendChild(c) { this.children.push(c); c.parentElement = this; return c; },
    insertBefore(c) { return this.appendChild(c); },
    remove() { const p = this.parentElement; if (p) p.children = p.children.filter(x => x !== this); this._removed = true; },
    querySelector(sel) {
      if (sel.charAt(0) !== '.') return null;
      const cls = sel.slice(1);
      return this.children.find(ch => (' ' + ch.className + ' ').indexOf(' ' + cls + ' ') >= 0) || null;
    },
    querySelectorAll() { return []; },
    getAttribute(k) { return k in this._attrs ? this._attrs[k] : null; },
    setAttribute(k, v) { this._attrs[k] = String(v); },
    removeAttribute(k) { delete this._attrs[k]; },
    closest() { return null; },
    focus() {}, scrollIntoView() {}, click() {},
    getContext() { return { clearRect() {} }; },
  };
  el.classList = {
    add: (...c) => c.forEach(x => s.add(x)), remove: (...c) => c.forEach(x => s.delete(x)),
    contains: c => s.has(c),
    toggle: (c, f) => { const on = f === undefined ? !s.has(c) : !!f; if (on) s.add(c); else s.delete(c); return on; },
  };
  return el;
}
const _els = {};
global.el = function (id) { return _els[id] || (_els[id] = mkEl(id)); };
global.document = {
  getElementById: id => el(id),
  querySelector: () => null,
  querySelectorAll: () => [],
  createElement: tag => { const e = mkEl(''); e.tagName = String(tag).toUpperCase(); return e; },
  addEventListener() {},
  body: mkEl('body'), head: mkEl('head'),
};
function mkStorage(seed) {
  const m = Object.assign({}, seed || {});
  return { getItem: k => (k in m ? m[k] : null), setItem: (k, v) => { m[k] = String(v); },
           removeItem: k => { delete m[k]; }, _m: m };
}
global.localStorage = mkStorage();
global.sessionStorage = mkStorage();
global.window = global;
global.location = { href: '', search: '', pathname: '/', origin: 'http://x', reload() { this.reloaded = true; } };
global.opened = [];
global.open = function (u, t) { opened.push(u); return null; };
global.confirms = [];
global.confirmAnswer = true;
global.confirm = function (m) { confirms.push(m); return confirmAnswer; };
global.toasts = [];
global.showToast = function (m, k) { toasts.push({ m: String(m), k: k }); };
global.showToastNotification = function (t, m, k) { toasts.push({ m: String(t) + ': ' + String(m), k: k }); };
global.esc = function (s) { return String(s == null ? '' : s).replace(/[&<>"']/g, function (c) {
  return { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]; }); };
global.calls = [];
global.log = [];
"""


def _run(prog: str, env=None) -> dict:
    full = PRELUDE + "\n" + prog
    full += "\n;(async () => {\n" + "  try { await main(); } catch (e) { console.log(JSON.stringify({error: String(e && e.stack || e)})); }\n})();"
    run_env = dict(os.environ)
    if env:
        run_env.update(env)
    out = subprocess.run([NODE, "-e", full], capture_output=True, text=True, timeout=60, env=run_env)
    assert out.returncode == 0, out.stderr
    lines = [l for l in out.stdout.strip().splitlines() if l.startswith("{")]
    assert lines, out.stdout + out.stderr
    data = json.loads(lines[-1])
    assert "error" not in data, data.get("error")
    return data


DASH = None


def _dash() -> str:
    global DASH
    if DASH is None:
        DASH = _main("dashboard.html")
    return DASH


# --------------------------------------------------------------------------
# Staff lists: every person at the selected venue (findings 14/19/22/23/33)
# --------------------------------------------------------------------------

FAKE_EMPLOYEES_SERVER = r"""
// GET /employees as the API behaves: default limit 50 across every venue the
// caller holds; venue_id filters; limit capped at MAX_LIMIT (422 above it).
global.MAX_LIMIT = 500;
global.CLAMP = 0;          // >0: hand back at most this many per page, whatever was asked
global.ALL = [];
for (let i = 0; i < 120; i++) ALL.push({ id: 'w-' + i, venue_id: 'wem', name: 'Wem ' + String(i).padStart(3, '0') });
for (let i = 0; i < 30; i++) ALL.push({ id: 'a-' + i, venue_id: 'apo', name: 'Apo ' + i });
global.ROSTERS = [];
for (let i = 0; i < 260; i++) ROSTERS.push({ id: 'r-' + i, venue_id: i % 2 ? 'wem' : 'apo', week_start: '2026-01-05', shifts: [] });
global.fetchAPI = async function (url) {
  calls.push(url);
  const u = new URL('http://x' + url);
  const p = u.searchParams;
  const list = u.pathname === '/employees' ? ALL : (u.pathname === '/rosters' ? ROSTERS : null);
  if (!list) return { items: [] };
  const limit = p.has('limit') ? +p.get('limit') : 50;
  const cap = u.pathname === '/employees' ? MAX_LIMIT : 200;
  if (limit > cap) throw new Error('422 Unprocessable Entity');
  const offset = +(p.get('offset') || 0);
  const rows = p.get('venue_id') ? list.filter(e => e.venue_id === p.get('venue_id')) : list;
  const n = CLAMP ? Math.min(CLAMP, limit) : limit;
  return { items: rows.slice(offset, offset + n), total: rows.length, limit: n, offset: offset };
};
"""

LOAD_VENUE_STUBS = r"""
global.state = { currentVenue: 'wem', currentPage: 'staff', isDemo: false, employees: [], rosters: [] };
global.annAudience = ['old'];
['annAudiencePills', 'showLoading', 'clearState', 'showEmptyVenueStates', 'updateDashboardKPIs',
 'initRevenueLabourChart', 'initLabourPctTrendChart', 'initHourlyBreakdownChart', 'populateAlerts',
 'populateStaffList', 'renderStaffTable', 'checkDeputyStatus', 'checkMYOBStatus', 'checkHumanForceStatus',
 'loadSetupWizard', 'loadBriefing', 'loadSnapshot', 'loadRealRevenueCharts', 'fetchAndUpdateLiveRevenue'].forEach(function (n) {
  global[n] = function () { log.push(n); };
});
global.connectionManager = { hide() {}, show() {} };
"""


def _paging_js():
    src = _dash()
    return _fns(src, "fetchAllPages", "fetchVenueEmployees", "fetchVenueRosters", "loadVenueData")


@needs_node
def test_staff_list_loads_every_page_for_the_selected_venue():
    data = _run(FAKE_EMPLOYEES_SERVER + LOAD_VENUE_STUBS + _paging_js() + r"""
async function main() {
  global.CLAMP = 50;   // force several pages
  await loadVenueData();
  console.log(JSON.stringify({
    n: state.employees.length, venues: Array.from(new Set(state.employees.map(e => e.venue_id))),
    total: state.employeesTotal, rosters: state.rosters.length, calls: calls }));
}
""")
    assert data["n"] == 120
    assert data["venues"] == ["wem"]
    assert data["total"] == 120
    assert data["rosters"] == 130
    emp_calls = [c for c in data["calls"] if c.startswith("/employees")]
    assert emp_calls and all("venue_id=wem" in c for c in emp_calls)
    assert all("venue_id=wem" in c for c in data["calls"] if c.startswith("/rosters"))


@needs_node
def test_staff_list_asks_for_500_and_falls_back_to_200_pages():
    data = _run(FAKE_EMPLOYEES_SERVER + LOAD_VENUE_STUBS + _paging_js() + r"""
async function main() {
  const big = await fetchVenueEmployees('wem');
  const firstBig = calls[0];
  calls.length = 0;
  global.MAX_LIMIT = 200;   // an older server that caps a page at 200
  for (let i = 120; i < 450; i++) ALL.push({ id: 'w-' + i, venue_id: 'wem', name: 'W' + i });
  const small = await fetchVenueEmployees('wem');
  console.log(JSON.stringify({ firstBig: firstBig, big: big.items.length, small: small.items.length,
                               total: small.total, calls: calls }));
}
""")
    assert "limit=500" in data["firstBig"]
    assert data["big"] == 120
    assert data["small"] == 450 and data["total"] == 450
    assert any("limit=200" in c for c in data["calls"])


@needs_node
def test_staff_table_says_when_the_list_came_back_short():
    src = _dash()
    js = _fns(src, "_weekHoursByEmployee", "_fmtAward", "renderStaffTable")
    data = _run(r"""
global._staffTableWired = true;
global._jwtRole = () => 'manager';
global.state = { isDemo: false, currentVenue: 'wem', rosters: [],
  employees: [{ id: 'a', venue_id: 'wem', name: 'Ann' }, { id: 'b', venue_id: 'wem', name: 'Bob' }],
  employeesTotal: 5 };
""" + js + r"""
async function main() {
  renderStaffTable();
  const shortNote = el('staff-count-note').textContent;
  state.employeesTotal = 2;
  renderStaffTable();
  console.log(JSON.stringify({ shortNote: shortNote, fullNote: el('staff-count-note').textContent,
                               body: el('staff-body').innerHTML }));
}
""")
    assert "Showing 2 of 5" in data["shortNote"]
    assert data["fullNote"] == ""
    assert "Ann" in data["body"] and "Bob" in data["body"]
    assert 'id="staff-count-note"' in _html("dashboard.html")


@needs_node
def test_staff_on_shift_kpi_is_the_rostered_count_not_the_headcount():
    js = _fns(_dash(), "_setKpi", "updateDashboardKPIs")
    data = _run(r"""
global.state = { currentVenue: 'wem', employees: Array.from({ length: 75 }, (_, i) => ({ id: 'w' + i })) };
global._getVenueId = () => 'wem';
global.__brief = { today: { staff_rostered: 4 } };
global.fetchAPI = async function (url) {
  if (url.indexOf('/api/briefing') === 0) return __brief;
  return { net_sales: 0, days: [] };
};
""" + js + r"""
async function main() {
  const pending = updateDashboardKPIs();
  const early = el('kpi-staff-count').textContent;
  await pending;
  const rostered = el('kpi-staff-count').textContent;
  global.__brief = null;
  await updateDashboardKPIs();
  console.log(JSON.stringify({ early: early, rostered: rostered, noBrief: el('kpi-staff-count').textContent }));
}
""")
    assert data["early"] == "—"
    assert data["rostered"] == 4
    assert data["noBrief"] == "—"


@needs_node
def test_live_revenue_poll_sends_the_session_and_never_shows_another_venues_takings():
    js = _fns(_dash(), "fetchAndUpdateLiveRevenue")
    data = _run(r"""
global.state = { currentVenue: 'apo', isDemo: false };
global.connectionManager = { hide() { log.push('hide'); }, show() { log.push('show'); } };
global.updateVarianceDisplay = function () {};
global.updateHourlySparkline = function () {};
global.fetch = async function () { throw new Error('raw fetch carries no session token'); };
global.fetchAPI = async function (url) {
  calls.push(url);
  if (url.indexOf('/revenue/') >= 0) return { venue_id: 'apo', current_hour: null, today_total: null };
  return { message: 'No revenue data yet' };
};
""" + js + r"""
async function main() {
  el('live-hour-revenue').textContent = '812.40';      // left over from the previous venue
  el('live-today-count').textContent = '57';
  await fetchAndUpdateLiveRevenue();
  console.log(JSON.stringify({ calls: calls, toasts: toasts, log: log,
    hour: el('live-hour-revenue').textContent, count: el('live-today-count').textContent }));
}
""")
    assert data["calls"][0] == "/api/pos/live/revenue/apo"
    assert data["toasts"] == []
    assert data["log"] == ["hide"]
    assert data["hour"] == "0.00" and str(data["count"]) == "0"


# --------------------------------------------------------------------------
# Venue switching (findings 21/43/107/108)
# --------------------------------------------------------------------------

SWITCH_STUBS = r"""
global.state = { currentVenue: 'wem', currentPage: 'timesheets', isDemo: false, currentRosterId: 'wem-roster',
  employees: [{ id: 'w1' }], rosters: [{ id: 'wem-roster' }], venues: [{ id: 'wem', name: 'The Wembley Hotel' },
  { id: 'apo', name: 'Apothecary at Murdoch Square' }], ws: null };
global.wsReconnectTimeout = null;
global.tsRows = [{ id: 'ts-wem' }]; global.lvRows = [{ id: 'lv' }]; global.ivRows = [{ id: 'iv' }];
global.ivOpenStocktake = { id: 'st' }; global.ivOpenStocktakes = [{ id: 'st' }]; global.ivOrders = [1];
global.siLines = [1]; global.ivInvoices = [1]; global.ivXeroPushes = { a: 1 }; global.ivMyobPushes = { a: 1 };
global.msRecipes = [1]; global.msIngredients = [1]; global.rbEditingId = 'rec-1'; global.rbLines = [1];
global.piUnmapped = [1]; global.anRows = [1]; global.mfPosts = [1]; global.sopDocs = [1]; global.cvRows = [1];
global.ckRun = { id: 'run' }; global.etsIndex = 3; global.rosterSection = 'bar'; global.ivSection = 'bar';
global.annAudience = ["x"];
global.connectWebSocket = function () { log.push('ws'); };
global.loadVenueData = async function () { log.push('loadVenueData:' + state.currentVenue); };
global.showPage = function (p) { log.push('showPage:' + p); };
global.clearAIChat = function () { log.push('clearAIChat'); };
"""


def _switch_js():
    return _fns(_dash(), "resetVenuePageState", "onVenueSelected")


@needs_node
def test_venue_switch_reruns_the_active_page_and_drops_old_venue_state():
    data = _run(SWITCH_STUBS + _switch_js() + r"""
async function main() {
  await onVenueSelected('apo');
  console.log(JSON.stringify({ log: log, saved: localStorage.getItem('rosteriq_current_venue'),
    rosterId: state.currentRosterId, ts: tsRows, lv: lvRows, iv: ivRows, st: ivOpenStocktake,
    rb: rbEditingId, ck: ckRun, rs: rosterSection, is: ivSection, emps: state.employees,
    ros: state.rosters, venue: state.currentVenue }));
}
""")
    assert data["venue"] == "apo"
    assert data["saved"] == "apo"
    assert data["rosterId"] is None
    assert data["ts"] == [] and data["lv"] == [] and data["iv"] == []
    assert data["st"] is None and data["rb"] is None and data["ck"] is None
    assert data["rs"] == "" and data["is"] == ""
    assert data["emps"] == [] and data["ros"] == []
    log = data["log"]
    assert "clearAIChat" in log
    assert "loadVenueData:apo" in log
    assert log.index("loadVenueData:apo") < log.index("showPage:timesheets")


@needs_node
def test_venue_switch_on_roster_page_reloads_that_page():
    data = _run(SWITCH_STUBS + _switch_js() + r"""
async function main() {
  state.currentPage = 'roster';
  await onVenueSelected('apo');
  state.currentPage = 'dashboard';
  log.length = 0;
  await onVenueSelected('wem');
  console.log(JSON.stringify({ log: log }));
}
""")
    # dashboard: loadVenueData already reloads the home cards
    assert "showPage:dashboard" not in data["log"]
    assert "loadVenueData:wem" in data["log"]


@needs_node
def test_venue_switch_survives_blocked_storage():
    data = _run(SWITCH_STUBS + _switch_js() + r"""
async function main() {
  global.localStorage = { getItem() { throw new Error('blocked'); }, setItem() { throw new Error('blocked'); },
                          removeItem() { throw new Error('blocked'); } };
  await onVenueSelected('apo');
  console.log(JSON.stringify({ log: log, venue: state.currentVenue }));
}
""")
    assert data["venue"] == "apo"
    assert "showPage:timesheets" in data["log"]


@needs_node
def test_dashboard_wires_the_selector_to_the_switch_handler():
    src = _dash()
    m = re.search(r"getElementById\('venue-select'\)\.addEventListener\('change',[\s\S]*?\n\}\);", src)
    assert m and "onVenueSelected(" in m.group(0)


ROSTER_STUBS = r"""
global.state = { currentVenue: 'apo', isDemo: false, currentRosterId: null, rosters: [{ id: 'apo-old-import' }],
  venues: [{ id: 'apo', name: 'Apothecary at Murdoch Square' }], employees: [] };
global.fetchAPI = async function (url, opts) { calls.push({ url: url, body: opts && opts.body }); return global.__resp; };
global.__resp = { success: true };
"""


def _publish_js():
    return _fns(_dash(), "displayedRosterId", "rosterDeliverySummary", "publishRoster", "sendRosterToStaff")


@needs_node
def test_publish_with_nothing_on_screen_asks_for_a_week_instead_of_guessing():
    data = _run(ROSTER_STUBS + _publish_js() + r"""
async function main() {
  const id = displayedRosterId();
  await publishRoster();
  console.log(JSON.stringify({ id: id, calls: calls, toasts: toasts }));
}
""")
    assert data["id"] is None
    assert data["calls"] == []
    assert any("Load a week first" in t["m"] for t in data["toasts"])


@needs_node
def test_publish_confirm_names_the_venue_and_week():
    data = _run(ROSTER_STUBS + _publish_js() + r"""
async function main() {
  state.currentRosterId = 'apo-2026-10-12';
  state.currentRosterWeek = '2026-10-12';
  confirmAnswer = false;
  await publishRoster();
  console.log(JSON.stringify({ calls: calls, confirms: confirms }));
}
""")
    assert data["calls"] == []
    assert len(data["confirms"]) == 1
    assert "Apothecary at Murdoch Square" in data["confirms"][0]
    assert "12 Oct" in data["confirms"][0]


@needs_node
def test_roster_week_load_ignores_a_response_for_the_previous_venue():
    src = _dash()
    js = _fns(src, "localIso", "getWeekMonday", "loadRosterForWeek")
    data = _run(r"""
global.state = { currentVenue: 'wem', currentRosterId: null, employees: [] };
global.rosterWeekOffset = 0;
global.renderRosterTable = function (r) { log.push('render:' + r.id); };
global.fetchAPI = async function (url) {
  calls.push(url);
  state.currentVenue = 'apo';      // the manager switched venue while this was in flight
  return { items: [{ id: 'wem-week', venue_id: 'wem', created_at: '2026-10-01' }] };
};
""" + js + r"""
async function main() {
  await loadRosterForWeek();
  console.log(JSON.stringify({ id: state.currentRosterId, log: log, calls: calls }));
}
""")
    assert "venue_id=wem" in data["calls"][0]
    assert data["id"] is None
    assert data["log"] == []


# --------------------------------------------------------------------------
# AI chat stays with its venue (finding 108)
# --------------------------------------------------------------------------

AI_STUBS = r"""
global.state = { currentVenue: 'apo', venues: [{ id: 'wem', name: 'The Wembley Hotel' }, { id: 'apo', name: 'Apothecary' }] };
global.aiState = { conversationId: 'conv-wem', history: [{ role: 'user', content: 'x' }] };
global._getVenueId = function () { return state.currentVenue; };
global.navigateTo = function () {};
global.addAIChatMessage = function (role, content, extras) { log.push({ role: role, content: content, extras: extras || null }); };
global.fetchAPI = async function (url, opts) { calls.push({ url: url, body: opts && opts.body ? JSON.parse(opts.body) : null }); return global.__resp; };
global.__resp = { message: 'done' };
"""


@needs_node
def test_ai_action_proposed_for_another_venue_is_refused():
    js = _fns(_dash(), "executeAIAction")
    data = _run(AI_STUBS + js + r"""
async function main() {
  await executeAIAction({ action_type: 'generate_roster', params: { budget: 8000 } }, 'wem');
  const refused = calls.length;
  await executeAIAction({ action_type: 'generate_roster', params: {} }, 'apo');
  console.log(JSON.stringify({ refused: refused, calls: calls, log: log }));
}
""")
    assert data["refused"] == 0
    assert len(data["calls"]) == 1 and data["calls"][0]["body"]["venue_id"] == "apo"
    assert "The Wembley Hotel" in data["log"][0]["content"]


@needs_node
def test_ai_reply_that_lands_after_a_venue_switch_is_dropped():
    js = _fns(_dash(), "sendAIMessage")
    data = _run(AI_STUBS + js + r"""
async function main() {
  el('ai-chat-input').value = 'Draft next week under $8,000';
  global.fetchAPI = async function (url, opts) {
    calls.push(url);
    state.currentVenue = 'wem';    // switched while the agent was thinking
    return { conversation_id: 'conv-new', response: 'Here is the Apothecary roster', actions: [] };
  };
  await sendAIMessage();
  console.log(JSON.stringify({ log: log, conv: aiState.conversationId }));
}
""")
    assistant = [m for m in data["log"] if m["role"] == "assistant"]
    assert assistant == []
    assert data["conv"] != "conv-new"


@needs_node
def test_ai_action_buttons_carry_the_venue_they_were_proposed_for():
    src = _dash()
    body = _fn(src, "addAIChatMessage")
    assert re.search(r"executeAIAction\(action,\s*[^)]+\)", body)
    send = _fn(src, "sendAIMessage")
    assert "venue_id: venueId" in send


# --------------------------------------------------------------------------
# Honest charts on a real venue (finding 18)
# --------------------------------------------------------------------------

CHART_STUBS = r"""
global.charts = {};
global.hourlySparklineChart = null;
global.made = [];
global.Chart = function (ctx, config) { made.push({ canvas: ctx && ctx.__id, config: config }); this.destroy = function () {}; };
global.dotFill = function (c) { return c; };
global.colors = { primary: '#F59E0B', success: '#10B981' };
global.whenChartReady = function (fn) { fn(); };
global.demoData = { hourly_breakdown: [{ hour: 18, revenue: 500, forecast: 450 }, { hour: 19, revenue: 700, forecast: 650 }] };
['hourly-sparkline-chart', 'revenue-labour-chart', 'labour-pct-trend-chart'].forEach(function (id) {
  const c = el(id);
  c.getContext = function () { return { __id: id, clearRect() {} }; };
  el('wrap-' + id).appendChild(c);
});
global.state = { currentVenue: 'wem', isDemo: false, venues: [{ id: 'wem', name: 'Wembley', max_labour_pct: 28 }] };
"""


def _chart_js():
    src = _dash()
    return (_top(src, "NO_SALES_MSG") + "\n" +
            _fns(src, "createChart", "chartEmptyState", "chartClearEmptyState", "initHourlyBreakdownChart",
                 "initRevenueLabourChart", "initLabourPctTrendChart", "loadRealRevenueCharts",
                 "drawRealRevenueCharts"))


def _snap(sales):
    days = []
    for i in range(7):
        d = "2026-10-%02d" % (3 + i)
        days.append({"date": d, "revenue_inc_gst": 1100.0 * sales, "revenue_net": 1000.0 * sales,
                     "labour": 300.0, "cogs": 0.0, "prime_pct": None})
    return {"venue_id": "wem", "net_sales": 7000.0 * sales, "labour": 2100.0, "days": days,
            "labour_pct": (30.0 if sales else None)}


@needs_node
def test_real_venue_without_sales_draws_no_invented_numbers():
    data = _run(CHART_STUBS + _chart_js() + r"""
async function main() {
  global.fetchAPI = async function (url) { calls.push(url); return """ + json.dumps(_snap(0)) + r"""; };
  initHourlyBreakdownChart();
  initRevenueLabourChart();
  initLabourPctTrendChart();
  const legacy = made.length;
  await loadRealRevenueCharts();
  const notes = ['hourly-sparkline-chart', 'revenue-labour-chart', 'labour-pct-trend-chart'].map(function (id) {
    const n = el('wrap-' + id).querySelector('.chart-empty');
    return { text: n ? n.textContent : null, hidden: el(id).style.display };
  });
  console.log(JSON.stringify({ legacy: legacy, made: made.length, notes: notes, calls: calls }));
}
""")
    assert data["legacy"] == 0, "real-venue chart functions drew hard-coded data"
    assert data["made"] == 0
    assert any("/api/snapshot?venue_id=wem" in c for c in data["calls"])
    for n in data["notes"]:
        assert n["text"] and "No sales recorded yet" in n["text"]
        assert n["hidden"] == "none"


@needs_node
def test_real_venue_with_sales_charts_its_own_snapshot():
    data = _run(CHART_STUBS + _chart_js() + r"""
async function main() {
  global.fetchAPI = async function (url) { return """ + json.dumps(_snap(1)) + r"""; };
  el('wrap-revenue-labour-chart').appendChild(Object.assign(mkEl(''), { className: 'chart-empty' }));
  await loadRealRevenueCharts();
  const byCanvas = {};
  made.forEach(function (m) { byCanvas[m.canvas] = m.config.data.datasets.map(d => d.data); });
  console.log(JSON.stringify({ byCanvas: byCanvas,
    staleNote: !!el('wrap-revenue-labour-chart').querySelector('.chart-empty'),
    hourlyNote: (el('wrap-hourly-sparkline-chart').querySelector('.chart-empty') || {}).textContent || '' }));
}
""")
    rl = data["byCanvas"]["revenue-labour-chart"]
    assert rl[0] == [1000.0] * 7 and rl[1] == [300.0] * 7
    pct = data["byCanvas"]["labour-pct-trend-chart"]
    assert [round(v, 3) for v in pct[0]] == [0.3] * 7
    assert [round(v, 3) for v in pct[1]] == [0.28] * 7
    assert data["staleNote"] is False
    # daily takings exist, but nothing hourly: the hourly card says so instead of inventing bars
    assert "hourly-sparkline-chart" not in data["byCanvas"]
    assert data["hourlyNote"]


@needs_node
def test_demo_keeps_its_showcase_charts():
    data = _run(CHART_STUBS + _chart_js() + r"""
async function main() {
  state.isDemo = true;
  initHourlyBreakdownChart(true);
  initRevenueLabourChart();
  initLabourPctTrendChart();
  console.log(JSON.stringify({ canvases: made.map(m => m.canvas) }));
}
""")
    assert sorted(data["canvases"]) == ["hourly-sparkline-chart", "labour-pct-trend-chart", "revenue-labour-chart"]


# --------------------------------------------------------------------------
# Sessions outlive the one-hour access token (finding 92)
# --------------------------------------------------------------------------

FETCH_STUB = r"""
global.REFRESH_OK = true;
global.fetches = [];
function resp(status, body) {
  return { ok: status >= 200 && status < 300, status: status, statusText: String(status),
           headers: { get: function () { return 'application/json'; } },
           json: async function () { return body; }, text: async function () { return JSON.stringify(body); } };
}
global.fetch = async function (url, opts) {
  opts = opts || {};
  const auth = (opts.headers || {})['Authorization'] || '';
  fetches.push({ url: url, auth: auth, body: opts.body || null });
  await new Promise(r => setTimeout(r, 5));
  if (url.indexOf('/api/auth/refresh') >= 0) {
    if (!REFRESH_OK) return resp(401, { detail: 'Invalid or expired refresh token' });
    return resp(200, { access_token: 'new-access', refresh_token: 'new-refresh' });
  }
  if (auth === 'Bearer new-access') return resp(200, { ok: true, url: url });
  return resp(401, { detail: 'Invalid or expired token' });
};
"""


def _dash_fetch_js():
    src = _dash()
    return _fns(src, "_refreshAccessToken", "_sessionExpired", "fetchAPI")


@needs_node
def test_dashboard_refreshes_an_expired_session_once_and_retries():
    data = _run(FETCH_STUB + r"""
global.localStorage = mkStorage({ jwt_token: 'old-access', jwt_refresh_token: 'old-refresh' });
global.state = { baseUrl: '', authToken: 'old-access', isDemo: false };
global._lastErrorReference = null;
global._refreshInFlight = null;
global._sessionEnding = false;
""" + _dash_fetch_js() + r"""
async function main() {
  const [a, b] = await Promise.all([fetchAPI('/employees?venue_id=wem'), fetchAPI('/rosters?venue_id=wem')]);
  console.log(JSON.stringify({ a: a, b: b, fetches: fetches, stored: localStorage._m, href: location.href }));
}
""")
    assert data["a"]["ok"] and data["b"]["ok"]
    refreshes = [f for f in data["fetches"] if "/api/auth/refresh" in f["url"]]
    assert len(refreshes) == 1
    assert json.loads(refreshes[0]["body"]) == {"refresh_token": "old-refresh"}
    assert data["stored"]["jwt_token"] == "new-access"
    assert data["stored"]["jwt_refresh_token"] == "new-refresh"
    assert data["href"] == ""


@needs_node
def test_dashboard_goes_to_login_only_when_the_refresh_fails():
    data = _run(FETCH_STUB + r"""
global.REFRESH_OK = false;
global.localStorage = mkStorage({ jwt_token: 'old-access', jwt_refresh_token: 'old-refresh' });
global.state = { baseUrl: '', authToken: 'old-access', isDemo: false };
global._lastErrorReference = null;
global._refreshInFlight = null;
global._sessionEnding = false;
""" + _dash_fetch_js() + r"""
async function main() {
  let err = null;
  try { await fetchAPI('/employees'); } catch (e) { err = e.message; }
  console.log(JSON.stringify({ err: err, href: location.href, fetches: fetches.length }));
}
""")
    assert data["err"]
    assert "login" in data["href"]
    assert data["fetches"] == 2


@needs_node
def test_demo_dashboard_never_tries_to_refresh():
    data = _run(FETCH_STUB + r"""
global.localStorage = mkStorage({ jwt_token: 'demo' });
global.state = { baseUrl: '', authToken: 'demo', isDemo: true };
global._lastErrorReference = null;
global._refreshInFlight = null;
global._sessionEnding = false;
""" + _dash_fetch_js() + r"""
async function main() {
  let err = null;
  try { await fetchAPI('/employees'); } catch (e) { err = e.message; }
  console.log(JSON.stringify({ err: err, href: location.href, urls: fetches.map(f => f.url) }));
}
""")
    assert data["err"]
    assert data["href"] == ""
    assert not any("/api/auth/refresh" in u for u in data["urls"])


def _my_api_js(demo=False):
    src = _main("my.html")
    return (
        "global.demoStaff = %s;\n" % ("true" if demo else "false") +
        "global.token = 'old-access';\n"
        "global.demoRetried = false;\nglobal.reauthing = false;\nglobal.refreshing = null;\n" +
        _fns(src, "refreshSession", "api", "apiErrorMessage")
    )


@needs_node
def test_staff_app_refreshes_instead_of_bouncing_to_login():
    data = _run(FETCH_STUB + r"""
global.localStorage = mkStorage({ jwt_token: 'old-access', jwt_refresh_token: 'old-refresh' });
""" + _my_api_js() + r"""
async function main() {
  const [a, b] = await Promise.all([api('/api/me/shifts'), api('/api/me/leave')]);
  console.log(JSON.stringify({ a: a, b: b, href: location.href, stored: localStorage._m,
    refreshes: fetches.filter(f => f.url.indexOf('/api/auth/refresh') >= 0).length }));
}
""")
    assert data["a"]["ok"] and data["b"]["ok"]
    assert data["href"] == ""
    assert data["refreshes"] == 1
    assert data["stored"]["jwt_token"] == "new-access"


@needs_node
def test_staff_app_goes_to_login_when_refresh_fails():
    data = _run(FETCH_STUB + r"""
global.REFRESH_OK = false;
global.localStorage = mkStorage({ jwt_token: 'old-access', jwt_refresh_token: 'old-refresh' });
""" + _my_api_js() + r"""
async function main() {
  let err = null;
  try { await api('/api/me/shifts'); } catch (e) { err = e.message; }
  console.log(JSON.stringify({ err: err, href: location.href }));
}
""")
    assert data["err"] == "unauthenticated"
    assert data["href"] == "/login"


@needs_node
def test_demo_staff_phone_still_remints_its_own_session():
    data = _run(FETCH_STUB + r"""
global.sessionStorage = mkStorage({ demo_staff_token: 'old-access' });
""" + _my_api_js(demo=True) + r"""
async function main() {
  let err = null;
  try { await api('/api/me/shifts'); } catch (e) { err = e.message; }
  console.log(JSON.stringify({ err: err, reloaded: !!location.reloaded, href: location.href,
    refreshes: fetches.filter(f => f.url.indexOf('/api/auth/refresh') >= 0).length,
    retried: sessionStorage.getItem('demo_staff_retried') }));
}
""")
    assert data["reloaded"] is True
    assert data["href"] == ""
    assert data["refreshes"] == 0
    assert data["retried"] == "1"


# --------------------------------------------------------------------------
# A staff sign-up path (findings 6/95/139)
# --------------------------------------------------------------------------

def test_login_page_leads_with_both_paths():
    html = _html("login.html")
    owner = re.search(r'<a[^>]+href="(/register|/static/register\.html)"[^>]*>[\s\S]*?New venue\?[\s\S]*?Create your account', html)
    staff = re.search(r'<a[^>]+href="/register\?join=1"[^>]*>[\s\S]*?Joining your team\?[\s\S]*?Use your join code', html)
    assert owner and staff
    form = html.index('id="loginForm"')
    assert owner.start() < form and staff.start() < form


REGISTER_FETCH = r"""
global.LINK_OK = true;
global.fetches = [];
global.fetch = async function (url, opts) {
  opts = opts || {};
  fetches.push({ url: url, auth: (opts.headers || {})['Authorization'] || '', body: opts.body ? JSON.parse(opts.body) : null });
  if (url === '/api/auth/register') {
    return { ok: true, status: 201, json: async () => ({ user: { id: 'u1' }, tokens: { access_token: 'acc-1', refresh_token: 'ref-1' } }) };
  }
  if (url === '/api/me/link') {
    if (!LINK_OK) return { ok: false, status: 400, json: async () => ({ detail: 'That join code isn’t valid' }) };
    return { ok: true, status: 200, json: async () => ({ linked: true, venue_name: 'The Wembley Hotel' }) };
  }
  return { ok: false, status: 404, json: async () => ({}) };
};
global.setTimeout = function (f) { f(); return 0; };
"""


def _register_js():
    return _main("register.html")


def _fill_register(join: bool) -> str:
    return r"""
  el('name').value = 'Harper Lee';
  el('email').value = 'harper@example.com';
  el('password').value = 'Passw0rd!234';
  el('confirmPassword').value = 'Passw0rd!234';
""" + (r"  el('joinCode').value = 'ABCD-EFGH';" if join else r"  el('venueName').value = 'The Wembley Hotel';")


@needs_node
def test_register_join_mode_signs_up_links_and_opens_the_staff_app():
    data = _run(REGISTER_FETCH + r"""
global.location.search = '?join=1';
""" + _register_js() + r"""
async function main() {
""" + _fill_register(True) + r"""
  await el('registerForm').fire('submit');
  await new Promise(r => setImmediate(r));
  console.log(JSON.stringify({ fetches: fetches, href: location.href, stored: localStorage._m }));
}
""")
    urls = [f["url"] for f in data["fetches"]]
    assert urls == ["/api/auth/register", "/api/me/link"]
    assert "venue" not in json.dumps(data["fetches"][0]["body"]).lower()
    link = data["fetches"][1]
    assert link["auth"] == "Bearer acc-1"
    assert link["body"] == {"code": "ABCD-EFGH"}
    assert data["href"] == "/my"
    assert "pending_venue_name" not in data["stored"]
    assert data["stored"]["jwt_token"] == "acc-1"
    assert data["stored"]["jwt_refresh_token"] == "ref-1"


@needs_node
def test_register_join_mode_retries_only_the_code_after_a_bad_code():
    data = _run(REGISTER_FETCH + r"""
global.location.search = '?join=1';
global.LINK_OK = false;
""" + _register_js() + r"""
async function main() {
""" + _fill_register(True) + r"""
  await el('registerForm').fire('submit');
  await new Promise(r => setImmediate(r));
  const afterFirst = { href: location.href, err: el('errorMessage').textContent };
  global.LINK_OK = true;
  el('joinCode').value = 'WXYZ-1234';
  await el('registerForm').fire('submit');
  await new Promise(r => setImmediate(r));
  console.log(JSON.stringify({ afterFirst: afterFirst, urls: fetches.map(f => f.url),
    lastBody: fetches[fetches.length - 1].body, href: location.href }));
}
""")
    assert data["afterFirst"]["href"] == ""
    assert "join code" in data["afterFirst"]["err"].lower()
    assert data["urls"] == ["/api/auth/register", "/api/me/link", "/api/me/link"]
    assert data["lastBody"] == {"code": "WXYZ-1234"}
    assert data["href"] == "/my"


@needs_node
def test_register_owner_mode_still_goes_to_the_venue_wizard():
    data = _run(REGISTER_FETCH + _register_js() + r"""
async function main() {
""" + _fill_register(False) + r"""
  await el('registerForm').fire('submit');
  await new Promise(r => setImmediate(r));
  console.log(JSON.stringify({ urls: fetches.map(f => f.url), href: location.href, stored: localStorage._m }));
}
""")
    assert data["urls"] == ["/api/auth/register"]
    assert data["href"] == "/static/dashboard.html"
    assert data["stored"]["pending_venue_name"] == "The Wembley Hotel"


def _load_venues_js():
    return _fn(_dash(), "loadVenues")


NO_VENUE_STUBS = r"""
global.state = { isDemo: false, venues: [] };
global.fetchAPI = async function () { return []; };
global.connectionManager = { hide() {}, show() {} };
global.showOnboardingWizard = function () { log.push('wizard'); };
global.showNoVenueChoice = function () { log.push('choice'); };
global.loadDemoVenue = function () {};
global.connectWebSocket = function () {};
global.loadVenueData = function () {};
"""


@needs_node
def test_signed_in_user_without_a_venue_gets_a_choice_not_the_owner_wizard():
    data = _run(NO_VENUE_STUBS + _load_venues_js() + r"""
async function main() {
  await loadVenues();
  const plain = log.slice();
  log.length = 0;
  localStorage.setItem('pending_venue_name', 'The Wembley Hotel');
  await loadVenues();
  const owner = log.slice();
  log.length = 0;
  location.search = '?setup=venue';
  await loadVenues();
  console.log(JSON.stringify({ plain: plain, owner: owner, setup: log,
    prefill: el('ob-venue-name').value }));
}
""")
    assert data["plain"] == ["choice"]
    assert data["owner"] == ["wizard"]
    assert data["prefill"] == "The Wembley Hotel"
    assert data["setup"] == ["wizard"]


def test_no_venue_choice_card_offers_both_paths():
    html = _html("dashboard.html")
    m = re.search(r'<div[^>]+id="no-venue-choice"[\s\S]*?<!-- /no-venue-choice -->', html)
    assert m
    card = m.group(0)
    assert "Setting up a venue" in card and "Joining your team" in card
    assert 'href="/my"' in card
    assert "noVenueSetUp()" in card


def test_staff_app_no_link_screen_keeps_join_code_and_offers_venue_setup():
    html = _html("my.html")
    nolink = html[html.index('id="nolink"'):html.index('id="view-home"')]
    assert 'id="join-code"' in nolink and "submitJoinCode()" in nolink
    assert "/static/dashboard.html?setup=venue" in nolink


def test_join_code_card_points_staff_at_the_join_sign_up():
    html = _html("dashboard.html")
    card = html[html.index('id="modal-join-code"'):html.index('id="modal-generate-roster"')]
    assert "/register?join=1" in card
    assert "sign up at <b>/login</b>" not in card


# --------------------------------------------------------------------------
# Home-screen manifests (finding 64) and the Time Clock venue (finding 106)
# --------------------------------------------------------------------------

def test_staff_app_and_kiosk_have_their_own_manifests():
    my = _html("my.html")
    tc = _html("timeclock.html")
    assert '<link rel="manifest" href="/static/manifest-my.json">' in my
    assert '<link rel="manifest" href="/static/manifest-timeclock.json">' in tc
    with open(os.path.join(STATIC, "manifest-my.json"), encoding="utf-8") as fh:
        m = json.load(fh)
    assert m["start_url"] == "/my" and m["scope"] == "/my" and m["name"] == "RosterIQ Staff"
    with open(os.path.join(STATIC, "manifest-timeclock.json"), encoding="utf-8") as fh:
        t = json.load(fh)
    assert t["start_url"] == "/timeclock"
    with open(os.path.join(STATIC, "manifest.json"), encoding="utf-8") as fh:
        d = json.load(fh)
    assert d["start_url"] != "/dashboard"


def test_manifests_are_served_without_a_login():
    from fastapi.testclient import TestClient
    from rosteriq.api import app
    c = TestClient(app)
    for path, start in (("/static/manifest-my.json", "/my"), ("/static/manifest-timeclock.json", "/timeclock")):
        r = c.get(path)
        assert r.status_code == 200, path
        assert r.json()["start_url"] == start


@needs_node
def test_time_clock_link_opens_the_selected_venue():
    src = _dash()
    html = _html("dashboard.html")
    assert "window.open('/timeclock','_blank')" not in html
    assert 'onclick="openTimeClock()"' in html
    data = _run(r"""
global.state = { currentVenue: 'apothecary-murdoch-square' };
""" + _fn(src, "openTimeClock") + r"""
async function main() {
  openTimeClock();
  console.log(JSON.stringify({ opened: opened }));
}
""")
    assert data["opened"] == ["/timeclock?venue_id=apothecary-murdoch-square"]


@needs_node
def test_kiosk_honours_venue_id_and_falls_back_to_its_last_venue():
    src = _main("timeclock.html")
    js = _fn(src, "resolveVenue")
    data = _run(r"""
global.api = async function () { return { items: [{ id: 'wem', name: 'The Wembley Hotel' }, { id: 'apo', name: 'Apothecary' }] }; };
global.venueName = '';
""" + js + r"""
async function main() {
  global.venueId = 'apo';
  await resolveVenue();
  const asked = venueId;
  localStorage.setItem('clock_venue_id', 'apo');
  global.venueId = 'somewhere-else';
  await resolveVenue();
  console.log(JSON.stringify({ asked: asked, fallback: venueId, name: venueName }));
}
""")
    assert data["asked"] == "apo"
    assert data["fallback"] == "apo"
    assert data["name"] == "Apothecary"


# --------------------------------------------------------------------------
# Import reports (finding 138)
# --------------------------------------------------------------------------

RICH_STAFF_IMPORT = {
    "status": "imported", "created_count": 2, "updated_count": 1, "skipped_count": 1,
    "rows": [
        {"line": 2, "name": "Isla Kaur", "email": "isla@example.com", "status": "created", "reason": "",
         "flags": ["rate_defaulted", "no_mobile"]},
        {"line": 3, "name": "Archie Taylor", "email": "archie@example.com", "status": "created", "reason": "", "flags": []},
        {"line": 4, "name": "Hudson Tran", "email": "hudson@example.com", "status": "updated", "reason": "", "flags": []},
        {"line": 5, "name": "Xavier <b>Kelly</b>", "email": "x@example.com", "status": "skipped",
         "reason": "email already exists", "flags": []},
    ],
    "flags_summary": [{"flag": "rate_defaulted", "count": 12, "message": "12 rates defaulted to the import default — check these"}],
    "unmapped_columns": ["Payroll ID", "Pronouns"],
    "column_map": {"name": "Full Name", "email": "Email"},
    "location": "The Wembley Hotel",
}


def _staff_import_js():
    src = _dash()
    return _fns(src, "importFlagText", "importFlagsSummary", "staffImportCounts", "_importExtras",
                "renderStaffImportReport", "runStaffImport")


@needs_node
def test_staff_import_shows_each_row_flags_and_unmapped_columns():
    data = _run(r"""
global.state = { currentVenue: 'wem' };
global.loadVenueData = function () { log.push('reload'); };
global.fetchAPI = async function (url, opts) { calls.push({ url: url, body: JSON.parse(opts.body) }); return """ + json.dumps(RICH_STAFF_IMPORT) + r"""; };
""" + _staff_import_js() + r"""
async function main() {
  el('staff-import-text').value = 'Full Name,Email\nIsla Kaur,isla@example.com';
  el('staff-import-update').checked = true;
  await runStaffImport();
  console.log(JSON.stringify({ body: calls[0].body, report: el('staff-import-report').innerHTML,
    result: el('staff-import-result').textContent, log: log }));
}
""")
    assert data["body"]["update_existing"] is True
    rep = data["report"]
    for s in ("Isla Kaur", "Hudson Tran", "email already exists", "Payroll ID", "Pronouns",
              "12 rates defaulted to the import default", "The Wembley Hotel"):
        assert s in rep, s
    assert "rate defaulted" in rep and "no mobile" in rep
    assert "Xavier &lt;b&gt;Kelly&lt;/b&gt;" in rep
    assert "<b>Kelly</b>" not in rep
    assert "1 updated" in data["result"]
    assert data["log"] == ["reload"]


@needs_node
def test_staff_import_report_works_with_the_older_response_shape():
    legacy = {"status": "imported", "created_count": 1, "skipped_count": 1,
              "created": [{"name": "Isla Kaur", "email": "isla@example.com", "rate": "31.50"}],
              "skipped": [{"row": 5, "name": "Xavier Kelly", "reason": "email already exists"}]}
    data = _run(r"""
global.state = { currentVenue: 'wem' };
global.loadVenueData = function () {};
global.fetchAPI = async function (url, opts) { calls.push(JSON.parse(opts.body)); return """ + json.dumps(legacy) + r"""; };
""" + _staff_import_js() + r"""
async function main() {
  el('staff-import-text').value = 'x';
  await runStaffImport();
  console.log(JSON.stringify({ body: calls[0], report: el('staff-import-report').innerHTML }));
}
""")
    assert data["body"]["update_existing"] is False
    assert "Xavier Kelly" in data["report"] and "email already exists" in data["report"]
    assert "Isla Kaur" in data["report"]


@needs_node
def test_roster_import_lists_skipped_lines_and_duplicates():
    resp = {"status": "imported", "shifts_imported": 40, "weeks": 2, "skipped_count": 2,
            "rows": [{"line": 7, "name": "Meca Rossi", "status": "skipped", "reason": "no staff member matches 'Meca R.'"},
                     {"line": 9, "name": "", "status": "skipped", "reason": "end time before start"}],
            "duplicates_skipped": 3, "unmapped_columns": ["Cost centre"], "location": "Front Bar"}
    src = _dash()
    js = _fns(src, "_importExtras", "renderRosterImportReport", "runRosterImport")
    data = _run(r"""
global.state = { currentVenue: 'wem' };
global.loadVenueData = function () {};
global.refreshRealRoster = function () {};
global.fetchAPI = async function () { return """ + json.dumps(resp) + r"""; };
""" + js + r"""
async function main() {
  el('roster-import-text').value = 'x';
  await runRosterImport();
  console.log(JSON.stringify({ report: el('roster-import-report').innerHTML }));
}
""")
    rep = data["report"]
    for s in ("Meca Rossi", "no staff member matches", "end time before start", "3 duplicate", "Cost centre",
              "Front Bar"):
        assert s in rep, s


@needs_node
def test_wizard_staff_import_shows_the_same_report():
    src = _dash()
    js = _fns(src, "importFlagText", "importFlagsSummary", "staffImportCounts", "_importExtras",
              "renderStaffImportReport", "obImportStaff")
    data = _run(r"""
global.obState = { venueId: 'wem' };
global.fetchAPI = async function (url, opts) { calls.push(JSON.parse(opts.body)); return """ + json.dumps(RICH_STAFF_IMPORT) + r"""; };
""" + js + r"""
async function main() {
  el('ob-staff-csv').value = 'x';
  el('ob-staff-update').checked = true;
  await obImportStaff();
  console.log(JSON.stringify({ body: calls[0], report: el('ob-staff-report').innerHTML }));
}
""")
    assert data["body"]["update_existing"] is True
    assert "email already exists" in data["report"] and "Payroll ID" in data["report"]
    html = _html("dashboard.html")
    for el_id in ("staff-import-update", "staff-import-report", "roster-import-report",
                  "ob-staff-update", "ob-staff-report"):
        assert 'id="%s"' % el_id in html, el_id


# --------------------------------------------------------------------------
# Who was actually messaged (finding 62)
# --------------------------------------------------------------------------

@needs_node
def test_publish_toast_states_who_was_reached():
    data = _run(ROSTER_STUBS + _publish_js() + r"""
async function main() {
  state.currentRosterId = 'r1';
  const out = [];
  for (const r of [
    { success: true, delivered: { email: 0, sms: 0, push: 0, in_app: 0 }, channels_available: ['in_app'] },
    { success: true, delivered: { email: 3, sms: 2, push: 0, in_app: 10 }, channels_available: ['email', 'sms', 'in_app'] },
    { success: true, notifications_sent: 0, message: 'Roster published successfully. 0 staff notified.' },
  ]) {
    toasts.length = 0;
    global.__resp = r;
    await publishRoster();
    out.push(toasts[toasts.length - 1]);
  }
  console.log(JSON.stringify({ out: out }));
}
""")
    nobody, some, legacy = data["out"]
    assert "Nobody was messaged" in nobody["m"] and "/my" in nobody["m"]
    assert "aren't switched on" in nobody["m"] or "aren’t switched on" in nobody["m"]
    assert "3 emailed" in some["m"] and "2 texted" in some["m"] and "10" in some["m"]
    assert some["k"] == "success"
    assert "Nobody was messaged" in legacy["m"]


@needs_node
def test_send_to_staff_never_counts_a_websocket_as_a_message():
    data = _run(ROSTER_STUBS + _publish_js() + r"""
async function main() {
  el('roster-week-date').textContent = '12 Oct - 18 Oct, 2026';
  global.__resp = { event_type: 'roster_published', venue_id: 'apo', total_targets: 71,
    sent: { email: 0, sms: 0, push: 0, websocket: 71 }, failed: { email: 71 }, skipped: {} };
  await sendRosterToStaff();
  const legacy = toasts[toasts.length - 1];
  global.__resp = { total_targets: 71, delivered: { email: 5, sms: 0, push: 0, in_app: 20 }, channels_available: ['email', 'in_app'] };
  await sendRosterToStaff();
  console.log(JSON.stringify({ legacy: legacy, rich: toasts[toasts.length - 1] }));
}
""")
    assert "71" not in data["legacy"]["m"]
    assert "Nobody was messaged" in data["legacy"]["m"]
    assert data["legacy"]["k"] != "success"
    assert "5 emailed" in data["rich"]["m"]


# --------------------------------------------------------------------------
# Wizard week start in Perth before 8am (finding 143)
# --------------------------------------------------------------------------

@needs_node
def test_wizard_first_roster_week_is_a_local_monday_before_8am_in_perth():
    src = _dash()
    js = _fns(src, "localIso", "obNextMonday")
    data = _run(r"""
const RealDate = Date;
global.Date = class extends RealDate {
  constructor(...a) { if (a.length) super(...a); else super(2026, 9, 14, 7, 30, 0); }   // Wed 14 Oct, 07:30 local
  static now() { return new RealDate(2026, 9, 14, 7, 30, 0).getTime(); }
};
""" + js + r"""
async function main() {
  const iso = obNextMonday();
  const p = iso.split('-').map(Number);
  console.log(JSON.stringify({ iso: iso, dow: new RealDate(p[0], p[1] - 1, p[2]).getDay() }));
}
""", env={"TZ": "Australia/Perth"})
    assert data["iso"] == "2026-10-19"
    assert data["dow"] == 1
