"""
Wembley pilot: venue setup.

* Areas with a minimum per weekday, a "needs this skill" flag and per-weekday
  trading hours are set in Settings → Areas & minimums and in a setup-wizard
  step after the staff import. They save through POST /venues; min_staff
  follows as each area's busiest-day minimum.
* Re-running the wizard on a venue never wipes its areas, hours or minimums
  and never renames it unless Rename was pressed (?rename=true).
* "+ Add another venue" starts the wizard blank on a new id (?create=true
  refuses an existing one) and lands on the new venue.
* The cold-start forecast and the briefing's coverage only count trading
  hours: recorded, else inferred from published or imported rosters, else
  10:00-midnight (never 06:00).
* Coverage gaps and the conflict detector hold each hour to the minimums of
  the trading day it belongs to.
"""

import json
import uuid
from collections import defaultdict
from datetime import date, datetime, time, timedelta
from decimal import Decimal

from fastapi.testclient import TestClient

from rosteriq.api import app
from rosteriq.database import MemoryStore, get_db
from rosteriq.ensemble import EnsembleForecaster
from rosteriq.models import DemandForecast, Employee, Roster, Shift, ShiftStatus, VenueConfig
from rosteriq.roster_optimiser import compute_coverage_gaps
from rosteriq.services.clock import venue_today
from rosteriq.services.conflict_detector import ConflictType, detect_conflicts
from rosteriq.services.trading_hours import venue_trading_hours
from rosteriq.tests.test_wembley_a6 import _dash, _fn, _fns, _html, _run, _top, needs_node

PW = "Passw0rd!234"
DAYS = ("mon", "tue", "wed", "thu", "fri", "sat", "sun")
MON = date(2026, 10, 12)
FRI, SAT, SUN = MON + timedelta(days=4), MON + timedelta(days=5), MON + timedelta(days=6)

WEMBLEY_AREAS = [
    {"name": "Sports Bar", "min_by_day": {"mon": 1, "tue": 1, "wed": 2, "thu": 2, "fri": 3, "sat": 3, "sun": 2}},
    {"name": "Duty Manager", "min_by_day": {d: 1 for d in DAYS}},
]
WEMBLEY_HOURS = {"mon": {"open": "16:00", "close": "22:00"},
                 "fri": {"open": "11:00", "close": "01:00"},
                 "sat": {"open": "11:00", "close": "01:00"}}


def _venue(**kw):
    base = dict(id="wv", name="The Wembley Hotel", tanda_org_id="", state="wa",
                timezone="Australia/Perth", max_labour_pct=30, created_at=datetime(2026, 10, 1))
    base.update(kw)
    return VenueConfig(**base)


def _shift(sid, emp, day, start, end, role="sports bar"):
    return Shift(id=sid, employee_id=emp, date=day, start_time=time(*start), end_time=time(*end),
                 status=ShiftStatus.scheduled, role=role)


def _roster(rid, vid, monday, shifts):
    return Roster(id=rid, venue_id=vid, week_start=monday, week_end=monday + timedelta(days=6),
                  shifts=shifts, created_at=datetime(2026, 10, 1))


def _fc(vid, day, hour, covers=5.0, version="cold_start_v1"):
    return DemandForecast(id=f"{vid}-{day}-{hour}-{version}", venue_id=vid, date=day, hour=hour,
                          predicted_covers=covers, confidence=0.5, model_version=version)


def _emp(vid, eid, skills, **kw):
    fields = dict(employment_type="casual", award_level="level_2", state="wa",
                  hourly_base_rate=Decimal("31.50"), skills=skills,
                  created_at=datetime(2026, 10, 1), updated_at=datetime(2026, 10, 1))
    fields.update(kw)
    return Employee(id=eid, venue_id=vid, name=eid, **fields)


def _hours_by_day(forecasts):
    out = defaultdict(list)
    for f in forecasts:
        out[f.date].append(f.hour)
    return {d: sorted(h) for d, h in out.items()}


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------

def _login(c, email):
    c.post("/api/auth/register", json={"email": email, "password": PW, "name": "U"})
    tok = c.post("/api/auth/login", json={"email": email, "password": PW}).json()["access_token"]
    return {"Authorization": f"Bearer {tok}"}


def _set(email, role, venue_ids, grants=None):
    db = get_db()
    rec = db.get_user_by_email(email)
    rec["role"] = role
    rec["venue_ids"] = list(venue_ids)
    rec["section_grants"] = grants or {}
    db.save_user(rec)


def _body(vid, name="The Wembley Hotel", **kw):
    body = {"id": vid, "name": name, "state": "wa", "max_labour_pct": 30, "tanda_org_id": "",
            "timezone": "Australia/Perth", "created_at": "2026-10-01T00:00:00"}
    body.update(kw)
    return body


def _manager_with_venue(c, **kw):
    email = f"b1m{uuid.uuid4().hex[:8]}@x.com"
    h = _login(c, email)
    _set(email, "staff", [])
    vid = f"wembley-{uuid.uuid4().hex[:6]}"
    r = c.post("/venues", json=_body(vid, **kw), headers=h)
    assert r.status_code == 200, r.text
    return h, vid, email


def _user(c, vid, role="staff", grants=None):
    email = f"b1u{uuid.uuid4().hex[:8]}@x.com"
    h = _login(c, email)
    _set(email, role, [vid], grants)
    return h


# ---------------------------------------------------------------------------
# Cold-start forecast and trading hours (finding 54)
# ---------------------------------------------------------------------------

def test_cold_start_forecast_without_hours_trades_from_ten_not_six():
    week = EnsembleForecaster(_venue()).predict_week(MON)
    hours = _hours_by_day(week)
    assert hours[MON] == list(range(10, 24))
    assert not [f for f in week if f.hour < 10]


def test_cold_start_forecast_follows_recorded_hours_past_midnight():
    hours = _hours_by_day(EnsembleForecaster(_venue(trading_hours=WEMBLEY_HOURS)).predict_week(MON))
    assert hours[MON] == list(range(16, 22))                 # Monday 4pm-10pm only
    assert hours[MON + timedelta(days=1)] == list(range(10, 24))  # Tuesday not recorded: default
    assert hours[FRI] == list(range(11, 24))
    assert hours[SAT] == [0] + list(range(11, 24))           # 00:00 is Friday night's last hour
    assert hours[SUN][0] == 0                                 # and Saturday night's


def test_hours_are_inferred_from_published_or_imported_rosters_only():
    db = MemoryStore()
    v = _venue(trading_hours={"thu": {"open": "12:00", "close": "22:00"}})
    db.save_venue(v)
    prev = MON - timedelta(days=7)
    # An imported Deputy week: Monday 11:00-15:00 and 17:00-23:30.
    db.save_roster(_roster(f"imported-wv-{prev.isoformat()}", "wv", prev, [
        _shift("i1", "a", prev, (11, 0), (15, 0)), _shift("i2", "b", prev, (17, 0), (23, 30))]))
    # A published week two weeks back: Saturday 11:00 to 01:30.
    two = MON - timedelta(days=14)
    db.save_roster(_roster("pub", "wv", two, [_shift("p1", "a", two + timedelta(days=5), (11, 0), (1, 30))]))
    db.update_roster_state("pub", "published", "test", "t")
    # An unpublished generated draft with a 06:00 Wednesday start never counts.
    db.save_roster(_roster("draft", "wv", MON, [_shift("d1", "a", MON + timedelta(days=2), (6, 0), (17, 0))]))

    eff = venue_trading_hours(db, v, MON)
    assert eff["mon"] == {"open": "11:00", "close": "23:30", "source": "roster history"}
    assert eff["sat"] == {"open": "11:00", "close": "01:30", "source": "roster history"}
    assert eff["wed"] == {"open": "10:00", "close": "00:00", "source": "default"}
    assert eff["thu"] == {"open": "12:00", "close": "22:00", "source": "recorded"}

    hours = _hours_by_day(EnsembleForecaster(v, trading_hours=eff).predict_week(MON))
    assert hours[MON] == list(range(11, 24))
    assert hours[MON + timedelta(days=2)] == list(range(10, 24))
    assert hours[SUN][:3] == [0, 1, 10]                      # Sat's 01:30 close spills into Sunday


def test_generated_week_forecast_follows_hours_and_drops_stale_early_rows():
    c = TestClient(app)
    h, vid, _ = _manager_with_venue(c)
    r = c.post("/forecasts/generate", json={"venue_id": vid, "week_start": MON.isoformat()}, headers=h)
    assert r.status_code == 200, r.text
    stored = get_db().get_forecasts(vid, MON, SUN)
    assert stored and not [f for f in stored if f.hour < 10 and f.predicted_covers > 0]

    # Monday's hours recorded later: the cold-start week is rebuilt to match.
    r = c.post("/venues", json=_body(vid, trading_hours={"mon": {"open": "16:00", "close": "22:00"}}), headers=h)
    assert r.status_code == 200, r.text
    c.post("/forecasts/generate", json={"venue_id": vid, "week_start": MON.isoformat()}, headers=h)
    monday = {f.hour: f.predicted_covers for f in get_db().get_forecasts(vid, MON, MON)}
    assert all(monday[hr] > 0 for hr in range(16, 22))
    assert all(monday.get(hr, 0) == 0 for hr in list(range(0, 16)) + [22, 23])


def test_briefing_never_reports_a_shortfall_before_the_venue_opens():
    c = TestClient(app)
    h, vid, _ = _manager_with_venue(c)
    db = get_db()
    today = venue_today(vid, db)
    monday = today - timedelta(days=today.weekday())
    # The forecast the old cold start stored: 40 covers an hour from 06:00.
    db.add_forecasts([_fc(vid, today, hr, covers=40.0) for hr in range(6, 24)])
    shifts = [_shift(f"{vid}-s{i}", f"{vid}-e{i}", today, (10, 0), (0, 0), role="bar") for i in range(4)]
    db.save_roster(_roster(f"{vid}-wk", vid, monday, shifts))
    db.update_roster_state(f"{vid}-wk", "published", "test", "t")

    b = c.get(f"/api/briefing?venue_id={vid}", headers=h).json()
    assert not [a for a in b["attention"] if "short at" in a], b["attention"]
    today_cov = b["today"]["coverage"]["today"]
    assert today_cov["status"] == "covered" and today_cov["peak_hour"] >= "10:00"


# ---------------------------------------------------------------------------
# Coverage gaps and the conflict detector (finding 75)
# ---------------------------------------------------------------------------

def test_coverage_gaps_use_each_days_minimums_inside_trading_hours():
    v = _venue(areas=WEMBLEY_AREAS[:1], trading_hours=WEMBLEY_HOURS)
    fcs = ([_fc("wv", MON, hr) for hr in range(6, 24)] + [_fc("wv", SAT, hr) for hr in range(11, 24)]
           + [_fc("wv", SUN, 0)])
    roster = _roster("r", "wv", MON, [_shift("m", "a", MON, (16, 0), (22, 0)),
                                       _shift("s", "a", SAT, (11, 0), (1, 0))])
    cov = compute_coverage_gaps(roster, fcs, venue_config=v,
                                trading_hours=venue_trading_hours(None, v, MON))
    days = {d["date"]: d for d in cov["days"]}
    assert days[MON.isoformat()]["status"] == "covered"     # one sports bar on a Monday is enough
    assert days[MON.isoformat()]["peak_hour"] >= "16:00"    # 06:00 is outside trading hours
    assert days[SAT.isoformat()]["required"] == 3 and days[SAT.isoformat()]["gap"] == 2
    # Sunday 00:00 is Saturday night: held to Saturday's three, not Sunday's two.
    assert days[SUN.isoformat()]["required"] == 3


def test_conflict_detector_checks_minimums_by_weekday_within_trading_hours():
    v = _venue(areas=WEMBLEY_AREAS[:1], trading_hours=WEMBLEY_HOURS)
    roster = _roster("r", "wv", MON, [_shift("m", "a", MON, (16, 0), (22, 0)),
                                       _shift("s", "a", SAT, (11, 0), (1, 0))])
    emp = _emp("wv", "a", ["sports bar"])
    conflicts = detect_conflicts(roster, v, [emp], [_fc("wv", MON, 10, covers=30.0)])
    short = [cf.message for cf in conflicts if cf.conflict_type == ConflictType.UNDERSTAFFED_HOUR]
    assert not [m for m in short if "Mon" in m], short
    assert "Sports bar: 1 of 3 on 11:00-01:00 Sat" in short, short


# ---------------------------------------------------------------------------
# POST /venues: areas, minimums, no accidental wipe or rename (72, 74, 31)
# ---------------------------------------------------------------------------

def test_areas_save_with_min_staff_at_each_areas_busiest_day():
    c = TestClient(app)
    h, vid, _ = _manager_with_venue(c, areas=WEMBLEY_AREAS, trading_hours=WEMBLEY_HOURS)
    v = c.get(f"/venues/{vid}", headers=h).json()
    assert v["min_staff"] == {"sports bar": 3, "duty manager": 1}
    flags = {a["name"]: a["requires_skill"] for a in v["areas"]}
    assert flags == {"sports bar": False, "duty manager": True}
    assert v["trading_hours"]["fri"] == {"open": "11:00", "close": "01:00"}


def test_rerunning_the_wizard_never_wipes_or_renames_the_venue():
    c = TestClient(app)
    h, vid, _ = _manager_with_venue(c, areas=WEMBLEY_AREAS, trading_hours=WEMBLEY_HOURS)
    # Exactly what the old wizard posted when Ben typed Apothecary's name in.
    r = c.post("/venues", json=_body(vid, name="Apothecary at Murdoch Square", min_staff={},
                                     max_labour_pct=28), headers=h)
    assert r.status_code == 200, r.text
    assert {"name", "areas", "trading_hours"} <= set(r.json()["kept"])
    v = c.get(f"/venues/{vid}", headers=h).json()
    assert v["name"] == "The Wembley Hotel"
    assert [a["name"] for a in v["areas"]] == ["sports bar", "duty manager"]
    assert v["trading_hours"] == WEMBLEY_HOURS
    assert v["min_staff"] == {"sports bar": 3, "duty manager": 1}
    assert v["max_labour_pct"] == 28
    names = {x["id"]: x["name"] for x in c.get("/venues", headers=h).json()["items"]}
    assert names[vid] == "The Wembley Hotel"


def test_rename_needs_the_explicit_flag():
    c = TestClient(app)
    h, vid, _ = _manager_with_venue(c)
    r = c.post(f"/venues?rename=true", json=_body(vid, name="The Wembley"), headers=h)
    assert r.status_code == 200 and r.json()["name"] == "The Wembley"
    assert c.get(f"/venues/{vid}", headers=h).json()["name"] == "The Wembley"


def test_emptying_areas_and_hours_takes_an_explicit_clear():
    c = TestClient(app)
    h, vid, _ = _manager_with_venue(c, areas=WEMBLEY_AREAS, trading_hours=WEMBLEY_HOURS)
    r = c.post("/venues?clear=areas,trading_hours", json=_body(vid, areas=[], trading_hours={}, min_staff={}),
               headers=h)
    assert r.status_code == 200, r.text
    v = c.get(f"/venues/{vid}", headers=h).json()
    assert v["areas"] == [] and v["trading_hours"] == {} and v["min_staff"] == {}


def test_add_another_venue_creates_alongside_and_never_overwrites():
    c = TestClient(app)
    h, vid, _ = _manager_with_venue(c, areas=WEMBLEY_AREAS)
    r = c.post(f"/venues?create=true", json=_body(vid, name="Apothecary at Murdoch Square"), headers=h)
    assert r.status_code == 409
    apo = f"apothecary-{uuid.uuid4().hex[:6]}"
    r = c.post("/venues?create=true", json=_body(apo, name="Apothecary at Murdoch Square"), headers=h)
    assert r.status_code == 200, r.text
    names = {x["id"]: x["name"] for x in c.get("/venues", headers=h).json()["items"]}
    assert names[vid] == "The Wembley Hotel" and names[apo] == "Apothecary at Murdoch Square"
    assert c.get(f"/venues/{vid}", headers=h).json()["min_staff"] == {"sports bar": 3, "duty manager": 1}


# ---------------------------------------------------------------------------
# GET /api/venue-setup: what the card and wizard step read
# ---------------------------------------------------------------------------

def test_venue_setup_offers_imported_skills_and_roles_to_managers_only():
    c = TestClient(app)
    h, vid, _ = _manager_with_venue(c)
    other_h, other_vid, _ = _manager_with_venue(c)
    db = get_db()
    db.save_employee(_emp(vid, f"{vid}-a", ["Front Bar", "Bottle Shop"]))
    db.save_employee(_emp(vid, f"{vid}-b", ["front bar"]))
    db.save_employee(_emp(other_vid, f"{other_vid}-x", ["Cellar"]))
    monday = venue_today(vid, db) - timedelta(days=venue_today(vid, db).weekday())
    db.save_roster(_roster(f"imported-{vid}-{monday.isoformat()}", vid, monday,
                           [_shift(f"{vid}-s", f"{vid}-a", monday, (11, 0), (15, 0), role="Restaurant")]))

    r = c.get(f"/api/venue-setup?venue_id={vid}", headers=h)
    assert r.status_code == 200, r.text
    d = r.json()
    cands = {x["name"]: x for x in d["candidates"]}
    assert cands["front bar"]["staff"] == 2 and cands["front bar"]["label"] == "Front Bar"
    assert "bottle shop" in cands and cands["restaurant"]["shifts"] == 1
    assert "cellar" not in cands
    assert d["effective_hours"]["mon"] == {"open": "11:00", "close": "15:00", "source": "roster history"}
    assert d["effective_hours"]["tue"] == {"open": "10:00", "close": "00:00", "source": "default"}

    assert c.get(f"/api/venue-setup?venue_id={vid}", headers=_user(c, vid)).status_code == 403
    dept = _user(c, vid, role="manager", grants={vid: ["bar"]})
    assert c.get(f"/api/venue-setup?venue_id={vid}", headers=dept).status_code == 403
    assert c.get(f"/api/venue-setup?venue_id={vid}", headers=other_h).status_code == 403


# ---------------------------------------------------------------------------
# The dashboard: wizard re-run, add venue, Areas & minimums card
# ---------------------------------------------------------------------------

STORED = {
    "id": "the-wembley-hotel", "name": "The Wembley Hotel", "tanda_org_id": "", "state": "wa",
    "timezone": "Australia/Perth", "max_labour_pct": 30, "created_at": "2026-10-01T00:00:00",
    "areas": [{"name": "sports bar", "label": "Sports Bar", "requires_skill": False,
               "min_by_day": {"mon": 1, "tue": 1, "wed": 2, "thu": 2, "fri": 3, "sat": 3, "sun": 2}}],
    "trading_hours": {"fri": {"open": "11:00", "close": "01:00"}},
    "min_staff": {"sports bar": 3},
}

WIZARD_STUBS = r"""
global.STORED = %s;
global.state = { isDemo: false, currentVenue: 'the-wembley-hotel',
                 venues: [{ id: 'the-wembley-hotel', name: 'The Wembley Hotel', state: 'wa', max_labour_pct: 30 }] };
global.refreshSessionRole = async function () {};
global.CREATE_CONFLICTS = 0;
global.fetchAPI = async function (url, opts) {
  const method = (opts && opts.method) || 'GET';
  calls.push({ url: url, method: method, body: opts && opts.body ? JSON.parse(opts.body) : null });
  if (method === 'GET' && url.indexOf('/venues/') === 0) return JSON.parse(JSON.stringify(STORED));
  if (method === 'POST' && url.indexOf('create=true') >= 0 && CREATE_CONFLICTS > 0) {
    CREATE_CONFLICTS -= 1;
    throw new Error('A venue with this id already exists');
  }
  return { id: 'ok' };
};
""" % json.dumps(STORED)


def _wizard_js():
    d = _dash()
    return (_top(d, "aeState") + "\n" + _fns(
        d, "showOnboardingWizard", "_obVenueHeading", "obStartRename", "_obResetSteps", "obGoToStep",
        "_obNewVenueId", "obSaveVenue", "getTimezoneForState", "obFinish")
        + "\nglobal.obState = { currentStep: 1, totalSteps: 5, venueId: null, rosterId: null,"
        + " mode: 'new', renaming: false, created: false, adding: false };\n")


@needs_node
def test_wizard_rerun_edits_the_venue_and_only_renames_on_request():
    data = _run(WIZARD_STUBS + _wizard_js() + r"""
async function main() {
  showOnboardingWizard();
  const opened = { title: el('ob-venue-title').textContent, readOnly: el('ob-venue-name').readOnly,
                   rename: el('ob-rename-btn').style.display, name: el('ob-venue-name').value };
  el('ob-venue-name').value = 'Apothecary at Murdoch Square';
  el('ob-max-labour').value = '28';
  await obSaveVenue();
  const plain = calls.filter(c => c.method === 'POST');
  calls.length = 0;
  obStartRename();
  const renaming = { readOnly: el('ob-venue-name').readOnly, rename: el('ob-rename-btn').style.display };
  el('ob-venue-name').value = 'The Wembley';
  await obSaveVenue();
  console.log(JSON.stringify({ opened: opened, plain: plain, renaming: renaming,
    renamed: calls.filter(c => c.method === 'POST'), venueId: obState.venueId }));
}
""")
    assert data["opened"] == {"title": "Editing The Wembley Hotel", "readOnly": True, "rename": "",
                              "name": "The Wembley Hotel"}
    assert len(data["plain"]) == 1 and data["plain"][0]["url"] == "/venues"
    body = data["plain"][0]["body"]
    assert body["id"] == "the-wembley-hotel" and body["name"] == "The Wembley Hotel"
    assert body["areas"] == STORED["areas"] and body["trading_hours"] == STORED["trading_hours"]
    assert body["min_staff"] == {"sports bar": 3} and body["max_labour_pct"] == 28
    assert data["renaming"] == {"readOnly": False, "rename": "none"}
    assert data["renamed"][0]["url"] == "/venues?rename=true"
    assert data["renamed"][0]["body"]["name"] == "The Wembley"
    assert data["venueId"] == "the-wembley-hotel"


@needs_node
def test_add_another_venue_starts_blank_creates_a_new_id_and_lands_on_it():
    data = _run(WIZARD_STUBS + _wizard_js() + r"""
global.hideOnboardingWizard = function () {};
global.resetVenuePageState = function () { log.push('reset'); };
global.loadVenues = async function () { log.push('loadVenues'); };
global.showPage = function (p) { log.push('page:' + p); };
global.loadVenueData = function () { log.push('data'); };
async function main() {
  el('ob-venue-name').value = 'The Wembley Hotel';
  showOnboardingWizard({ addVenue: true });
  const opened = { title: el('ob-venue-title').textContent, readOnly: el('ob-venue-name').readOnly,
                   name: el('ob-venue-name').value };
  el('ob-venue-name').value = 'Apothecary at Murdoch Square';
  CREATE_CONFLICTS = 1;
  const ok = await obSaveVenue();
  await obFinish();
  const posts = calls.filter(c => c.method === 'POST');
  console.log(JSON.stringify({ opened: opened, ok: ok, posts: posts, venueId: obState.venueId,
    current: state.currentVenue, selected: el('venue-select').value, log: log,
    saved: localStorage.getItem('rosteriq_current_venue') }));
}
""")
    assert data["opened"] == {"title": "Add another venue", "readOnly": False, "name": ""}
    assert data["ok"] is True
    urls = [p["url"] for p in data["posts"]]
    assert urls == ["/venues?create=true", "/venues?create=true"]
    first, second = data["posts"][0]["body"], data["posts"][1]["body"]
    assert first["id"] == "apothecary-at-murdoch-square" and first["name"] == "Apothecary at Murdoch Square"
    assert second["id"].startswith("apothecary-at-murdoch-square-") and second["id"] != "the-wembley-hotel"
    assert "areas" not in first and "min_staff" not in first
    assert data["venueId"] == second["id"] == data["current"] == data["selected"] == data["saved"]
    assert "reset" in data["log"] and "loadVenues" in data["log"]


@needs_node
def test_a_double_press_on_next_creates_the_new_venue_once():
    data = _run(WIZARD_STUBS + _wizard_js() + _fn(_dash(), "obNextStep") + r"""
async function main() {
  showOnboardingWizard({ addVenue: true });
  el('ob-venue-name').value = 'Apothecary at Murdoch Square';
  await Promise.all([obNextStep(), obNextStep()]);
  console.log(JSON.stringify({ posts: calls.filter(c => c.method === 'POST').map(c => c.url),
    step: obState.currentStep, next: el('ob-btn-next').disabled }));
}
""")
    assert data["posts"] == ["/venues?create=true"]
    assert data["step"] == 2 and data["next"] is False


def _areas_js():
    d = _dash()
    return "\n".join([_top(d, "AE_DAYS"), _top(d, "AE_DAY_LABELS"), _top(d, "aeState"),
                      _fns(d, "aeAttr", "aeNorm", "aeTime", "aeNeedsSkill", "aeRender", "_aeHint",
                           "_aeEdit", "_aeAdd", "aeSave")])


AREA_STUBS = WIZARD_STUBS + r"""
global.rosterSectionPills = function () { log.push('pills'); };
function blankHours() {
  const h = {};
  ['mon', 'tue', 'wed', 'thu', 'fri', 'sat', 'sun'].forEach(d => { h[d] = { open: '', close: '' }; });
  return h;
}
"""


@needs_node
def test_areas_card_saves_minimums_hours_and_min_staff_through_post_venues():
    data = _run(AREA_STUBS + _areas_js() + r"""
async function main() {
  const hours = blankHours();
  hours.mon = { open: '10:00', close: '22:00' };
  hours.fri = { open: '11:00', close: '01:00' };
  aeState['settings-areas'] = { venueId: 'the-wembley-hotel', loaded: true, dirty: true, save: true,
    areas: [{ name: 'sports bar', label: 'Sports Bar', requires_skill: false,
              min_by_day: { mon: 1, tue: 1, wed: 2, thu: 2, fri: 3, sat: 3, sun: 2 } }],
    hours: hours, effective: {}, candidates: [] };
  _aeAdd('settings-areas', 'Duty Manager', 'Duty Manager');
  const added = aeState['settings-areas'].areas[1];
  const ok = await aeSave('settings-areas');
  const saved = calls.filter(c => c.method === 'POST');
  calls.length = 0;

  aeState['settings-areas'].hours.tue = { open: '10:00', close: '' };
  const half = await aeSave('settings-areas');
  const halfPosts = calls.filter(c => c.method === 'POST').length;
  calls.length = 0;

  aeState['settings-areas'].areas = [];
  aeState['settings-areas'].hours = blankHours();
  await aeSave('settings-areas');
  console.log(JSON.stringify({ added: added, ok: ok, saved: saved, half: half, halfPosts: halfPosts,
    cleared: calls.filter(c => c.method === 'POST'), toasts: toasts }));
}
""")
    assert data["added"]["requires_skill"] is True                     # on by default for duty manager
    assert data["added"]["min_by_day"]["mon"] == 1
    assert data["ok"] is True and data["saved"][0]["url"] == "/venues"
    body = data["saved"][0]["body"]
    assert body["name"] == "The Wembley Hotel"
    assert body["min_staff"] == {"sports bar": 3, "duty manager": 1}
    assert body["trading_hours"] == {"mon": {"open": "10:00", "close": "22:00"},
                                     "fri": {"open": "11:00", "close": "01:00"}}
    assert [a["name"] for a in body["areas"]] == ["sports bar", "duty manager"]
    assert data["half"] is False and data["halfPosts"] == 0
    assert data["cleared"][0]["url"] == "/venues?clear=areas,min_staff,trading_hours"


@needs_node
def test_areas_card_offers_imported_candidates_escaped():
    data = _run(AREA_STUBS + _areas_js() + r"""
async function main() {
  aeState['ob-areas'] = { venueId: 'v', loaded: true, dirty: false, save: false, areas: [],
    hours: blankHours(), effective: { mon: { open: '11:00', close: '23:30', source: 'roster history' } },
    candidates: [{ name: 'front bar', label: 'Front Bar', staff: 12, shifts: 40 },
                 { name: 'x', label: '<img src=x onerror=alert(1)>', staff: 1, shifts: 0 }] };
  aeRender('ob-areas');
  console.log(JSON.stringify({ html: el('ob-areas').innerHTML }));
}
""")
    html = data["html"]
    assert "Front Bar (12 staff, 40 shifts)" in html
    assert "<img" not in html and "&lt;img" in html
    assert "11:00–23:30 from your roster history" in html
    assert "Save areas" not in html                                      # the wizard's Next saves


@needs_node
def test_venue_selector_offers_add_another_venue_to_managers():
    d = _dash()
    prog = r"""
global.state = { isDemo: false, venues: [] };
global.fetchAPI = async function () { return [{ id: 'the-wembley-hotel', name: 'The Wembley Hotel' }]; };
global.connectionManager = { hide() {}, show() {} };
global.connectWebSocket = function () {};
global.loadVenueData = function () {};
global.CAN = true;
global._obCanManage = function () { return CAN; };
""" + _fn(d, "loadVenues") + r"""
async function main() {
  await loadVenues();
  const mgr = el('venue-select').innerHTML;
  CAN = false;
  await loadVenues();
  console.log(JSON.stringify({ mgr: mgr, staff: el('venue-select').innerHTML }));
}
"""
    data = _run(prog)
    assert 'value="__add_venue__">+ Add another venue' in data["mgr"]
    assert "__add_venue__" not in data["staff"]
    i = d.index("getElementById('venue-select').addEventListener('change'")
    handler = d[i:i + 400]
    assert "'__add_venue__'" in handler and "showOnboardingWizard({ addVenue: true })" in handler


def test_settings_and_wizard_carry_the_areas_editor():
    html = _html("dashboard.html")
    start = html.index('id="page-settings"')
    settings = html[start:html.index('class="page"', start + 1)] if 'class="page"' in html[start + 1:] \
        else html[start:html.index('id="onboarding-wizard"')]
    assert 'id="settings-areas"' in settings and "Areas &amp; minimums" in settings
    assert 'onclick="showOnboardingWizard({ addVenue: true })"' in settings
    steps = html[html.index('class="onboarding-progress"'):html.index('class="onboarding-body"')]
    labels = [s for s in ("Team", "Areas", "Kitchen", "First roster") if s in steps]
    assert labels == ["Team", "Areas", "Kitchen", "First roster"]
    assert steps.index("Team") < steps.index("Areas") < steps.index("Kitchen")
    page3 = html[html.index('data-ob-page="3"'):html.index('data-ob-page="4"')]
    assert 'id="ob-areas"' in page3
    assert "min_staff: {}" not in _fn(_dash(), "obSaveVenue")
