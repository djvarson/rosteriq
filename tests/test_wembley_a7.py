"""
Wembley pilot: the AI and server pieces a manager leans on in week one.

* AFL games come from the published fixture (Squiggle), in venue local time;
  the off-season and an unreachable source are said plainly, never filled in.
* The AI prompt carries no venue's people or numbers; the cut tool reads the
  venue's own recorded labour target and minimums.
* Employment type reaches the AI as 'casual', so "casuals first" ordering works.
* The demo-only demand endpoints don't serve made-up signals for real venues.
* Generate Optimal Roster runs (or says why it can't) instead of a 500.
* Publish / Send to Staff report what actually reached people.
* Email sender, forgot-password honesty, and a pub Wi-Fi's shared IP.
"""

import asyncio
import json
import os
import uuid
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import httpx
import pytest
from fastapi.testclient import TestClient

from rosteriq.database import get_db
from rosteriq.models import (
    VenueConfig, Employee, EmploymentType, AwardLevel, State,
    Roster, Shift, ShiftStatus,
)
from rosteriq.middleware.rate_limiter import RateLimiterMiddleware

# conftest neutralises the limiter's dispatch per test; keep the real one.
_REAL_LIMITER_DISPATCH = RateLimiterMiddleware.dispatch

FIXTURE = json.loads((Path(__file__).parent / "fixtures" / "squiggle_games_2026.json").read_text())
NOW = datetime(2026, 1, 1)
PW = "Passw0rd!234"


def _venue(db, vid, state=State.wa, tz="Australia/Perth", max_labour_pct=30.0, min_staff=None):
    db.save_venue(VenueConfig(
        id=vid, name=f"Venue {vid}", tanda_org_id="", state=state, timezone=tz,
        min_staff=min_staff or {}, max_labour_pct=max_labour_pct, created_at=NOW,
    ))


def _emp(vid, eid, etype=EmploymentType.casual, email=None, skills=("bar",)):
    return Employee(
        id=eid, venue_id=vid, name=f"Staff {eid}", employment_type=etype,
        award_level=AwardLevel.level_2, state=State.wa,
        hourly_base_rate=Decimal("30.00"), email=email, skills=list(skills),
        created_at=NOW, updated_at=NOW,
    )


def _roster(db, vid, emp_ids, day=None, rid=None):
    day = day or date.today()
    ws = day - timedelta(days=day.weekday())
    shifts = [Shift(id=f"{vid}-s{i}", employee_id=e, date=day, start_time=time(10, 0),
                    end_time=time(16, 0), break_minutes=30, status=ShiftStatus.scheduled,
                    role="bar", cost=Decimal("180.00"))
              for i, e in enumerate(emp_ids)]
    roster = Roster(id=rid or f"{vid}-r", venue_id=vid, week_start=ws,
                    week_end=ws + timedelta(days=6), shifts=shifts, created_at=NOW)
    db.save_roster(roster)
    return roster


# ---------------------------------------------------------------------------
# [16] AFL fixtures from Squiggle
# ---------------------------------------------------------------------------

class _Squiggle:
    """A recorded Squiggle: the trimmed 2026 response, nothing for 2027."""

    def __init__(self, fail=False):
        self.fail = fail
        self.requests = []

    def handler(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        if self.fail:
            raise httpx.ConnectError("no route to host", request=request)
        q = request.url.query.decode() if isinstance(request.url.query, bytes) else str(request.url.query)
        if "year=2026" in q:
            return httpx.Response(200, json=FIXTURE)
        return httpx.Response(200, json={"games": []})


@pytest.fixture
def squiggle(monkeypatch):
    from rosteriq.services import afl_fixtures, clock
    fake = _Squiggle()
    monkeypatch.setattr(afl_fixtures, "_http_client", lambda: httpx.AsyncClient(
        transport=httpx.MockTransport(fake.handler),
        headers={"User-Agent": afl_fixtures._user_agent()}))
    monkeypatch.setattr(clock, "venue_timezone", lambda venue_id, db=None: "Australia/Perth")
    monkeypatch.setenv("SQUIGGLE_CONTACT", "ops@example.org")
    afl_fixtures._last_failure.clear()
    yield fake
    afl_fixtures._last_failure.clear()


def _games(venue_id, now, days=10, **kw):
    from rosteriq.services.afl_fixtures import upcoming_games
    return asyncio.run(upcoming_games(venue_id, days_ahead=days, db=get_db(), now=now, **kw))


def test_afl_in_season_lists_real_games_in_venue_local_time(squiggle):
    db = get_db()
    _venue(db, "afl-wa")
    # Fri 24 Jul 2026, 10:00 in Perth
    out = _games("afl-wa", datetime(2026, 7, 24, 2, 0, tzinfo=timezone.utc))

    assert out["status"] == "ok"
    got = [(g["date"], g["kickoff"], g["matchup"], g["signal"]) for g in out["games"]]
    assert got == [
        ("2026-07-24", "18:10", "Fremantle v West Coast", "home_game"),
        ("2026-07-31", "18:10", "Fremantle v Western Bulldogs", "home_game"),
        ("2026-08-02", "11:10", "Richmond v West Coast", "watch_at_venue"),
    ]
    assert all(g["ground"] in ("Optus Stadium", "M.C.G.") for g in out["games"])
    blob = json.dumps(out)
    assert "Opponent" not in blob and "demand_multiplier" not in blob
    req = squiggle.requests[0]
    assert str(req.url).startswith("https://api.squiggle.com.au/?q=games;year=2026")
    ua = req.headers["user-agent"]
    assert "RosterIQ" in ua and "ops@example.org" in ua


def test_afl_off_season_says_so_and_invents_nothing(squiggle):
    db = get_db()
    _venue(db, "afl-oct")
    # Fri 9 Oct 2026: the Grand Final was 26 Sep, the 2027 fixture isn't out
    out = _games("afl-oct", datetime(2026, 10, 9, 1, 0, tzinfo=timezone.utc), days=14)

    assert out["status"] == "off_season"
    assert out["games"] == [] and out["count"] == 0
    assert "AFL off-season — no games until the 2027 fixture is published" in out["note"]


def test_afl_source_down_is_unavailable_not_made_up(monkeypatch, squiggle):
    squiggle.fail = True
    db = get_db()
    _venue(db, "afl-down")
    out = _games("afl-down", datetime(2026, 7, 24, 2, 0, tzinfo=timezone.utc))

    assert out["status"] == "unavailable"
    assert out["games"] == []
    assert "fixture source unavailable" in out["note"]


def test_afl_fixture_is_fetched_once_a_day_and_cached_in_kv_store(squiggle):
    db = get_db()
    _venue(db, "afl-cache")
    when = datetime(2026, 7, 24, 2, 0, tzinfo=timezone.utc)
    _games("afl-cache", when)
    _games("afl-cache", when)
    assert len([r for r in squiggle.requests if "year=2026" in str(r.url)]) == 1
    cached = db.load_from_key("afl:squiggle:games:2026")
    assert cached["fetched_on"] == datetime.now(timezone.utc).date().isoformat()
    assert len(cached["games"]) == len(FIXTURE["games"])


def test_afl_victorian_venue_does_not_get_perth_teams(squiggle):
    db = get_db()
    _venue(db, "afl-vic", state=State.vic, tz="Australia/Melbourne")
    out = _games("afl-vic", datetime(2026, 7, 29, 2, 0, tzinfo=timezone.utc), days=7)
    assert "Fremantle" not in out["local_teams"]
    assert ("Collingwood v Geelong", "home_game") in [(g["matchup"], g["signal"]) for g in out["games"]]


def test_ai_afl_tool_only_reports_published_games(squiggle):
    from rosteriq.ai_agent import AgentContext
    db = get_db()
    _venue(db, "afl-ai")
    data = json.loads(asyncio.run(AgentContext("afl-ai").execute_tool("get_afl_fixtures", {"days_ahead": 14})))
    assert "error" not in data, data
    published = {f"{g['hteam']} v {g['ateam']}" for g in FIXTURE["games"]}
    assert all(g["matchup"] in published for g in data["games"])
    assert data["status"] in ("ok", "off_season")
    assert "Opponent" not in json.dumps(data)


# ---------------------------------------------------------------------------
# [123] No venue's people or numbers in the AI prompt; targets from settings
# ---------------------------------------------------------------------------

def test_system_prompt_carries_no_venue_specific_people_or_numbers():
    from rosteriq.ai_agent import SYSTEM_PROMPT
    for text in ("Ben", "13% labour", "10% labour", "28% labour", "2.5x", "3.0x",
                 "~8 managers", "Total minimum: ~6"):
        assert text not in SYSTEM_PROMPT, text


def test_cut_tool_uses_the_venues_recorded_target_and_minimum():
    from rosteriq.ai_agent import AgentContext
    db = get_db()
    _venue(db, "cut-v", max_labour_pct=22.0, min_staff={"bar": 2})
    db.save_employees([_emp("cut-v", f"cut-e{i}") for i in range(4)])
    _roster(db, "cut-v", [f"cut-e{i}" for i in range(4)])

    bar = json.loads(asyncio.run(AgentContext("cut-v").execute_tool("get_cut_recommendation", {"area": "bar"})))
    assert bar["labour_target_pct"] == 22.0
    assert bar["skeleton_minimum"] == 2

    kitchen = json.loads(asyncio.run(AgentContext("cut-v").execute_tool("get_cut_recommendation", {"area": "kitchen"})))
    assert kitchen["labour_target_pct"] == 22.0
    assert any("No minimum staffing is recorded for kitchen" in n for n in kitchen["settings_notes"])


# ---------------------------------------------------------------------------
# [149] Employment type as its value, so casuals sort first
# ---------------------------------------------------------------------------

def test_ai_sees_casual_not_enum_name_and_sorts_casuals_first():
    from rosteriq.ai_agent import AgentContext
    db = get_db()
    _venue(db, "sos-v")
    db.save_employees([
        _emp("sos-v", "sos-ft", EmploymentType.full_time),
        _emp("sos-v", "sos-pt", EmploymentType.part_time),
        _emp("sos-v", "sos-cas", EmploymentType.casual),
    ])
    ctx = AgentContext("sos-v")
    emps = json.loads(asyncio.run(ctx.execute_tool("get_employees", {})))["employees"]
    assert {e["employment_type"] for e in emps} == {"full_time", "part_time", "casual"}

    sos = json.loads(asyncio.run(ctx.execute_tool("find_available_staff", {})))
    assert [e["employment_type"] for e in sos["available"]] == ["casual", "part_time", "full_time"]


# ---------------------------------------------------------------------------
# API helpers
# ---------------------------------------------------------------------------

def _register(c, email, name="U"):
    r = c.post("/api/auth/register", json={"email": email, "password": PW, "name": name})
    assert r.status_code == 201, r.text
    return r.json()


def _login(c, email):
    r = c.post("/api/auth/login", json={"email": email, "password": PW})
    assert r.status_code == 200, r.text
    return {"Authorization": f"Bearer {r.json()['access_token']}"}


def _manager_with_venue(c, vid, **venue):
    """Platform owner registered first (out of the way), then a manager who
    creates the venue."""
    _register(c, f"boot_{uuid.uuid4().hex[:6]}@x.com")
    email = f"mgr_{uuid.uuid4().hex[:6]}@x.com"
    _register(c, email)
    h = _login(c, email)
    body = {"id": vid, "name": f"Venue {vid}", "state": "wa", "timezone": "Australia/Perth",
            "max_labour_pct": 30, "tanda_org_id": "", "created_at": "2026-07-01T00:00:00"}
    body.update(venue)
    r = c.post("/venues", json=body, headers=h)
    assert r.status_code in (200, 201), r.text
    return _login(c, email)


def _staff_rows(vid, n, prefix="e"):
    return [{"id": f"{vid}-{prefix}{i}", "venue_id": vid, "name": f"Casual {i}",
             "employment_type": "casual", "award_level": "level_2", "state": "wa",
             "hourly_base_rate": "31.50", "email": f"{vid}-{prefix}{i}@x.com",
             "skills": ["bar"], "created_at": "2026-07-01T00:00:00",
             "updated_at": "2026-07-01T00:00:00"} for i in range(n)]


# ---------------------------------------------------------------------------
# [105] Made-up demand signals are demo-only; /overview doesn't crash
# ---------------------------------------------------------------------------

DEMAND_PATHS = ("overview", "signals", "demand-outlook", "week-ahead", "signals/history")


def test_real_venue_demand_endpoints_say_not_connected_instead_of_inventing():
    c = TestClient(app_())
    h = _manager_with_venue(c, "wem-demand")
    for path in DEMAND_PATHS:
        r = c.get(f"/dashboard/wem-demand/{path}", headers=h)
        assert r.status_code == 200, (path, r.text)
        body = r.json()
        assert body["status"] == "not_connected" and body["connected"] is False, (path, body)
        blob = json.dumps(body).lower()
        for fake in ("bureau of meteorology", "eventbrite", "twitter", "staff scheduled today"):
            assert fake not in blob, (path, fake)


def test_demo_venue_overview_with_a_shift_today_does_not_crash():
    from rosteriq.services.demo import DEMO_VENUE_ID
    c = TestClient(app_())
    email = f"owner_{uuid.uuid4().hex[:6]}@x.com"
    _register(c, email)               # first user: platform owner
    h = _login(c, email)
    db = get_db()
    _venue(db, DEMO_VENUE_ID)
    db.save_employees([_emp(DEMO_VENUE_ID, "demo-ov-1")])
    _roster(db, DEMO_VENUE_ID, ["demo-ov-1"])

    r = c.get(f"/dashboard/{DEMO_VENUE_ID}/overview", headers=h)
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["roster_summary_today"]["labour_cost"] > 0
    assert body["venue"]["timezone"] == "Australia/Perth"


# ---------------------------------------------------------------------------
# [37] Generate Optimal Roster
# ---------------------------------------------------------------------------

def _forecast_rows(vid, monday):
    rows = []
    for d in range(7):
        day = monday + timedelta(days=d)
        for hr in range(11, 23):
            rows.append({"id": f"{vid}-fc-{day.isoformat()}-{hr}", "venue_id": vid,
                         "date": day.isoformat(), "hour": hr, "predicted_covers": 40,
                         "confidence": 0.7, "signals_used": [], "model_version": "t"})
    return rows


def test_optimal_roster_runs_from_any_day_of_the_week():
    c = TestClient(app_())
    vid = "wem-opt"
    h = _manager_with_venue(c, vid, min_staff={"bar": 1})
    assert c.post("/employees/bulk", json=_staff_rows(vid, 6), headers=h).status_code == 200
    from rosteriq.services.clock import venue_today
    today = venue_today(vid, get_db())
    monday = today + timedelta(days=7 - today.weekday())   # a week that hasn't started
    assert c.post("/forecasts", json=_forecast_rows(vid, monday), headers=h).status_code == 200

    # Any day of the week (here its Friday) picks that week's Monday
    r = c.post("/api/v1/rosters/optimise", json={
        "venue_id": vid, "week_start": (monday + timedelta(days=4)).isoformat(),
        "strategy": "balanced", "solver_preference": "greedy", "timeout_seconds": 5}, headers=h)
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["week_start"] == monday.isoformat()
    status = c.get(f"/api/v1/rosters/optimise/status/{body['job_id']}", headers=h).json()
    assert status["status"] == "completed", status
    assert status["roster_id"]


def test_optimal_roster_without_a_forecast_is_a_clear_4xx():
    c = TestClient(app_())
    vid = "wem-opt-nofc"
    h = _manager_with_venue(c, vid)
    assert c.post("/employees/bulk", json=_staff_rows(vid, 3), headers=h).status_code == 200
    r = c.post("/api/v1/rosters/optimise", json={"venue_id": vid, "week_start": "2026-10-12"},
               headers=h)
    assert r.status_code == 422, r.text
    assert "forecast" in r.json()["error"]["message"].lower()


# ---------------------------------------------------------------------------
# [62] Publish / Send to Staff report what actually reached people
# ---------------------------------------------------------------------------

class _Outbox:
    def __init__(self):
        self.sent = []

    async def send(self, to_email, subject, html_content):
        self.sent.append((to_email, subject, html_content))
        return True


@pytest.fixture
def email_on(monkeypatch):
    """Outbound email switched on, captured instead of sent."""
    from rosteriq.services.notifications import NotificationService
    from rosteriq.services.notification_hub import get_notification_hub
    svc = NotificationService()
    svc.smtp_user, svc.smtp_pass = "mailer@x.com", "app-password"
    box = _Outbox()
    monkeypatch.setattr(svc, "send_email", box.send)
    monkeypatch.setattr(get_notification_hub(), "_email_service", svc)
    return box


@pytest.fixture
def email_off(monkeypatch):
    from rosteriq.services.notifications import NotificationService
    from rosteriq.services.notification_hub import get_notification_hub
    for k in ("SMTP_USER", "SMTP_PASS"):
        monkeypatch.delenv(k, raising=False)
    monkeypatch.setattr(get_notification_hub(), "_email_service", NotificationService())


def _published_roster(c, h, vid, rostered, extra_unrostered=1):
    rows = _staff_rows(vid, rostered + extra_unrostered, prefix=f"p{uuid.uuid4().hex[:4]}-")
    assert c.post("/employees/bulk", json=rows, headers=h).status_code == 200
    ids = [r["id"] for r in rows]
    roster = _roster(get_db(), vid, ids[:rostered], rid=f"{vid}-r-{uuid.uuid4().hex[:4]}")
    r = c.post(f"/api/v1/rosters/{roster.id}/publish", json={"skip_approval": True}, headers=h)
    assert r.status_code == 200, r.text
    return r.json(), rows


def test_publish_with_email_off_says_nobody_was_messaged(email_off):
    c = TestClient(app_())
    h = _manager_with_venue(c, "wem-pub-off")
    body, _ = _published_roster(c, h, "wem-pub-off", rostered=2)
    assert body["success"] is True
    assert body["notifications_sent"] == 0
    assert body["delivered"] == {"email": 0, "sms": 0, "push": 0, "in_app": 0}
    assert body["channels_available"] == []
    assert "Nobody was messaged" in body["message"] and "/my" in body["message"]


def test_publish_emails_only_rostered_staff_in_plain_english(email_on):
    c = TestClient(app_())
    vid = "wem-pub-on"
    h = _manager_with_venue(c, vid)
    body, rows = _published_roster(c, h, vid, rostered=2, extra_unrostered=1)

    assert body["notifications_sent"] == 2
    assert body["delivered"]["email"] == 2
    assert "email" in body["channels_available"]
    to = sorted(m[0] for m in email_on.sent)
    assert to == sorted(r["email"] for r in rows[:2])          # not the unrostered one
    _, subject, page = email_on.sent[0]
    assert f"Venue {vid}" in subject
    assert "/my" in page and "has been published" in page
    for internal in ("roster_id", "conflicts_warning", "employee_id", "week_end"):
        assert internal not in page, internal


def test_send_to_staff_targets_linked_or_rostered_staff_and_reports_delivery(email_off):
    c = TestClient(app_())
    vid = "wem-send"
    h = _manager_with_venue(c, vid)
    rows = _staff_rows(vid, 3, prefix=f"s{uuid.uuid4().hex[:4]}-")
    assert c.post("/employees/bulk", json=rows, headers=h).status_code == 200
    rostered, linked, neither = rows
    _roster(get_db(), vid, [rostered["id"]], rid=f"{vid}-r")
    db = get_db()
    db.update_roster_state(f"{vid}-r", "published", "test", "t")   # a draft is refused
    user = _register(c, linked["email"])["user"]
    rec = db.get_user_by_id(user["id"])
    rec["venue_ids"] = [vid]
    db.save_user(rec)

    r = c.post("/api/v1/notifications/dispatch", json={
        "event_type": "ROSTER_PUBLISHED", "venue_id": vid,
        "payload": {"week": "13 Oct", "message": "Your roster is up."}}, headers=h)
    assert r.status_code == 202, r.text
    body = r.json()
    assert body["total_targets"] == 2                      # not the unlinked, unrostered one
    assert body["reached"] == 0
    assert body["delivered"] == {"email": 0, "sms": 0, "push": 0, "in_app": 0}
    assert body["sent"]["ws"] == 0                         # nobody had the app open
    assert body["channels_available"] == []
    assert body["message"].startswith("Nobody was messaged")


def test_in_app_counts_only_a_live_session_of_a_linked_login(email_off, monkeypatch):
    from rosteriq.services.notification_hub import NotificationHub, NotificationEventType
    from rosteriq.services.notifications import NotificationService
    db = get_db()
    _venue(db, "wem-inapp")
    db.save_employees([_emp("wem-inapp", "inapp-1", email="casey@x.com"),
                       _emp("wem-inapp", "inapp-2", email="jo@x.com")])
    db.save_user({"id": "u-casey", "email": "casey@x.com", "name": "Casey", "role": "staff",
                  "venue_ids": ["wem-inapp"], "password_hash": "", "api_key_hash": "",
                  "is_active": True, "created_at": NOW, "last_login": None})

    class _Mgr:
        user_connections = {"u-casey": {object()}}

    hub = NotificationHub()
    hub._email_service = NotificationService()
    sent_to = []

    async def _fake_send(uid, title, body, action_url=None):
        sent_to.append(uid)

    monkeypatch.setattr(hub._ws_dispatcher, "_get_manager", lambda: _Mgr())
    monkeypatch.setattr(hub._ws_dispatcher, "send_notification", _fake_send)
    summary = asyncio.run(hub.dispatch(NotificationEventType.ROSTER_PUBLISHED, "wem-inapp",
                                       {"week_start": "2026-10-12"}, ["inapp-1", "inapp-2"]))
    assert sent_to == ["u-casey"]
    assert summary["delivered"]["in_app"] == 1 and summary["reached"] == 1
    assert summary["message"] == "1 staff notified (1 in the app)."


# ---------------------------------------------------------------------------
# [115] Sender address
# ---------------------------------------------------------------------------

def test_sender_reads_smtp_from_then_from_email_then_the_login(monkeypatch):
    from rosteriq.services.notifications import NotificationService
    monkeypatch.setenv("SMTP_USER", "mailer@wembley.example.com")
    monkeypatch.setenv("SMTP_FROM", "rosters@wembley.example.com")
    monkeypatch.setenv("FROM_EMAIL", "old@wembley.example.com")
    assert NotificationService().from_email == "rosters@wembley.example.com"
    monkeypatch.delenv("SMTP_FROM")
    assert NotificationService().from_email == "old@wembley.example.com"
    monkeypatch.delenv("FROM_EMAIL")
    assert NotificationService().from_email == "mailer@wembley.example.com"


# ---------------------------------------------------------------------------
# [56] Forgot password tells the truth when email is off
# ---------------------------------------------------------------------------

def test_forgot_password_without_email_says_so_and_records_it(monkeypatch):
    from rosteriq.services import notifications
    for k in ("SMTP_USER", "SMTP_PASS"):
        monkeypatch.delenv(k, raising=False)
    monkeypatch.setattr(notifications, "_notification_service", None)
    c = TestClient(app_())
    email = f"ben_{uuid.uuid4().hex[:6]}@x.com"
    _register(c, email)
    db = get_db()

    r = c.post("/api/auth/forgot-password", json={"email": email})
    assert r.status_code == 200
    body = r.json()
    assert body["email_configured"] is False
    assert "Email reset isn't switched on" in body["message"]
    assert "ask your manager" in body["message"]
    # Same answer for an address with no account: nothing to enumerate
    other = c.post("/api/auth/forgot-password", json={"email": "nobody@x.com"}).json()
    assert other["message"] == body["message"]
    # No token minted (it would also revoke a manager-issued link)
    assert not db._password_reset_tokens
    rows = db.list_events(category="security", action_prefix="auth.password_reset_requested")
    assert rows and rows[0]["details"]["outcome"] == "unavailable"
    assert rows[0]["details"]["reason"] == "email_not_configured"


# ---------------------------------------------------------------------------
# [51]/[57] One pub, one Wi-Fi IP
# ---------------------------------------------------------------------------

class _TightLimiter(RateLimiterMiddleware):
    dispatch = _REAL_LIMITER_DISPATCH
    TIER_LIMITS = {"unauthenticated": (3, 0.001), "authenticated": (3, 0.001),
                   "api_key": (3, 0.001)}


def _limited_app():
    from fastapi import FastAPI
    from fastapi.responses import HTMLResponse
    a = FastAPI()
    a.add_middleware(_TightLimiter)

    @a.get("/login")
    async def login_page():
        return HTMLResponse("<h1>login</h1>")

    @a.get("/static/app.js")
    async def asset():
        return HTMLResponse("x")

    @a.get("/api/me/shifts")
    async def shifts():
        return {"ok": True}

    @a.post("/api/clock/in")
    async def clock_in():
        return {"ok": True}

    return a


def _token(user_id):
    from rosteriq.services.auth import AuthService
    tok, _ = AuthService.create_access_token(user_id, f"{user_id}@x.com", "staff")
    return {"Authorization": f"Bearer {tok}"}


def test_pages_and_static_files_are_never_rate_limited():
    c = TestClient(_limited_app())
    for _ in range(10):
        assert c.get("/login").status_code == 200
        assert c.get("/static/app.js").status_code == 200


def test_signed_in_staff_on_one_ip_get_their_own_buckets():
    c = TestClient(_limited_app())          # every request: the same client IP
    for _ in range(3):
        assert c.get("/api/me/shifts", headers=_token("casual-a")).status_code == 200
    assert c.get("/api/me/shifts", headers=_token("casual-a")).status_code == 429
    assert c.get("/api/me/shifts", headers=_token("casual-b")).status_code == 200


def test_kiosk_punches_are_not_starved_by_the_shared_bucket():
    c = TestClient(_limited_app())
    kiosk = _token("kiosk-manager")
    for _ in range(3):
        c.get("/api/me/shifts", headers=kiosk)
    assert c.get("/api/me/shifts", headers=kiosk).status_code == 429
    assert c.post("/api/clock/in", headers=kiosk).status_code == 200


def test_demo_login_is_still_bucketed_per_visitor():
    from rosteriq.services.demo import DEMO_USER_ID
    shared = _limited_app()
    a = TestClient(shared, client=("198.51.100.10", 1))
    b = TestClient(shared, client=("198.51.100.11", 1))
    demo = _token(DEMO_USER_ID)
    for _ in range(3):
        assert a.get("/api/me/shifts", headers=demo).status_code == 200
    assert a.get("/api/me/shifts", headers=demo).status_code == 429
    assert b.get("/api/me/shifts", headers=demo).status_code == 200


def test_unverifiable_tokens_share_the_ip_bucket():
    c = TestClient(_limited_app())
    codes = [c.get("/api/me/shifts", headers={"Authorization": f"Bearer junk-{i}"}).status_code
             for i in range(5)]
    assert codes.count(429) == 2


def test_other_peoples_typos_on_the_pub_wifi_dont_lock_ben_out():
    ip = f"10.77.{uuid.uuid4().int % 250}.9"
    c = TestClient(app_(), client=(ip, 4321))
    ben = f"ben_{uuid.uuid4().hex[:6]}@x.com"
    _register(c, ben)
    for i in range(5):
        r = c.post("/api/auth/login", json={"email": f"casual{i}_{uuid.uuid4().hex[:4]}@x.com",
                                            "password": "wrong-password"})
        assert r.status_code == 401, r.text
    assert c.post("/api/auth/login", json={"email": ben, "password": PW}).status_code == 200

    # Ben's own five typos lock Ben (from here) — not the next person
    for _ in range(5):
        c.post("/api/auth/login", json={"email": ben, "password": "wrong-password"})
    assert c.post("/api/auth/login", json={"email": ben, "password": PW}).status_code == 429
    meca = f"meca_{uuid.uuid4().hex[:6]}@x.com"
    _register(c, meca)
    assert c.post("/api/auth/login", json={"email": meca, "password": PW}).status_code == 200


def test_ip_wide_failure_ceiling_still_applies(memory_store):
    from rosteriq.services.auth import AuthService, LOGIN_MAX_FAILURES_PER_IP
    auth = AuthService(db=memory_store)
    ip = "203.0.113.7"
    for i in range(LOGIN_MAX_FAILURES_PER_IP - 1):
        auth.record_login_attempt(f"guess{i}@x.com", ip, success=False)
    assert auth.check_login_rate_limit(ip, email="fresh@x.com") is True
    auth.record_login_attempt("one-more@x.com", ip, success=False)
    assert auth.check_login_rate_limit(ip, email="fresh@x.com") is False
    assert auth.check_login_rate_limit("198.51.100.1", email="fresh@x.com") is True


def app_():
    from rosteriq.api import app
    return app
