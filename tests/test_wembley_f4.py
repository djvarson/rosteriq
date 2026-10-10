"""
Wembley pilot: what the AI, notifications and the optimiser tell a manager.

* AFL: a quiet midweek or the pre-finals bye is not the off-season; club
  nicknames (Freo, Eagles, Pies) find the club; an unknown name is said so.
* Notifications: "already notified" only after something reached them, per
  roster/week; Send to Staff never tells people a draft was published.
* Generate Optimal Roster never replaces a week that has started; Discard
  really discards. Its shifts get the venue's areas, the award's meal break
  and a week-costed total.
* The manual digest carries no made-up weather or events for a real venue.
* The AI compliance check reads real shift hours and award_rules thresholds,
  worded as advice.
"""

import asyncio
import json
import uuid
from collections import Counter
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

FIXTURE = json.loads((Path(__file__).parent / "fixtures" / "squiggle_games_2026.json").read_text())
NOW = datetime(2026, 1, 1)
PW = "Passw0rd!234"


def app_():
    from rosteriq.api import app
    return app


def _venue(db, vid, state=State.wa, tz="Australia/Perth", min_staff=None, name=None):
    db.save_venue(VenueConfig(
        id=vid, name=name or f"Venue {vid}", tanda_org_id="", state=state, timezone=tz,
        min_staff=min_staff or {}, max_labour_pct=30.0, created_at=NOW,
    ))


def _emp(vid, eid, etype=EmploymentType.casual, email=None, skills=("bar",)):
    return Employee(
        id=eid, venue_id=vid, name=f"Staff {eid}", employment_type=etype,
        award_level=AwardLevel.level_2, state=State.wa,
        hourly_base_rate=Decimal("30.00"), email=email, skills=list(skills),
        created_at=NOW, updated_at=NOW,
    )


def _venue_today(vid):
    from rosteriq.services.clock import venue_today
    return venue_today(vid, get_db())


def _next_monday(vid):
    today = _venue_today(vid)
    return today + timedelta(days=7 - today.weekday())


def _roster(db, vid, emp_ids, day, rid, start=time(10, 0), end=time(16, 0), brk=30):
    ws = day - timedelta(days=day.weekday())
    shifts = [Shift(id=f"{rid}-s{i}", employee_id=e, date=day, start_time=start,
                    end_time=end, break_minutes=brk, status=ShiftStatus.scheduled,
                    role="bar", cost=Decimal("180.00"))
              for i, e in enumerate(emp_ids)]
    roster = Roster(id=rid, venue_id=vid, week_start=ws, week_end=ws + timedelta(days=6),
                    shifts=shifts, created_at=datetime.now())
    db.save_roster(roster)
    return roster


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
    creates the venue. Returns (headers, manager email)."""
    _register(c, f"boot_{uuid.uuid4().hex[:6]}@x.com")
    email = f"mgr_{uuid.uuid4().hex[:6]}@x.com"
    _register(c, email)
    h = _login(c, email)
    body = {"id": vid, "name": f"Venue {vid}", "state": "wa", "timezone": "Australia/Perth",
            "max_labour_pct": 30, "tanda_org_id": "", "created_at": "2026-07-01T00:00:00"}
    body.update(venue)
    r = c.post("/venues", json=body, headers=h)
    assert r.status_code in (200, 201), r.text
    return _login(c, email), email


def _staff_rows(vid, n, prefix="e", emails=True, skills=("bar",)):
    return [{"id": f"{vid}-{prefix}{i}", "venue_id": vid, "name": f"Casual {i}",
             "employment_type": "casual", "award_level": "level_2", "state": "wa",
             "hourly_base_rate": "31.50", "email": f"{vid}-{prefix}{i}@x.com" if emails else None,
             "skills": list(skills), "created_at": "2026-07-01T00:00:00",
             "updated_at": "2026-07-01T00:00:00"} for i in range(n)]


# ---------------------------------------------------------------------------
# [39] [40] AFL fixtures
# ---------------------------------------------------------------------------

class _Squiggle:
    def handler(self, request: httpx.Request) -> httpx.Response:
        if "year=2026" in str(request.url):
            return httpx.Response(200, json=FIXTURE)
        return httpx.Response(200, json={"games": []})


@pytest.fixture
def squiggle(monkeypatch):
    from rosteriq.services import afl_fixtures, clock
    fake = _Squiggle()
    monkeypatch.setattr(afl_fixtures, "_http_client", lambda: httpx.AsyncClient(
        transport=httpx.MockTransport(fake.handler)))
    monkeypatch.setattr(clock, "venue_timezone", lambda venue_id, db=None: "Australia/Perth")
    afl_fixtures._last_failure.clear()
    yield fake
    afl_fixtures._last_failure.clear()


def _games(venue_id, now, days=10, **kw):
    from rosteriq.services.afl_fixtures import upcoming_games
    return asyncio.run(upcoming_games(venue_id, days_ahead=days, db=get_db(), now=now, **kw))


def _perth_10am(d: date) -> datetime:
    return datetime(d.year, d.month, d.day, 2, 0, tzinfo=timezone.utc)


def test_afl_quiet_midweek_in_season_is_not_off_season(squiggle):
    _venue(get_db(), "afl-mid")
    # Tue 4 Aug: games were played in July, more on Sat 8 Aug; none Tue-Wed.
    out = _games("afl-mid", _perth_10am(date(2026, 8, 4)), days=2)
    assert out["status"] == "ok", out
    assert "off-season" not in out["note"]
    assert out["note"].startswith("No AFL games between Tue 4 Aug and Wed 5 Aug")
    assert "next local game is Sat 8 Aug (Melbourne v Fremantle, 11:15)" in out["note"]
    assert out["next_local_game"]["date"] == "2026-08-08"


def test_afl_pre_finals_bye_is_not_off_season(squiggle):
    _venue(get_db(), "afl-bye")
    out = _games("afl-bye", _perth_10am(date(2026, 8, 24)), days=7)
    assert out["status"] == "ok", out
    assert "next local game is Fri 11 Sep (Fremantle v Geelong, 18:10)" in out["note"]


def test_afl_before_the_first_game_is_still_the_off_season(squiggle):
    _venue(get_db(), "afl-pre")
    out = _games("afl-pre", _perth_10am(date(2026, 7, 1)), days=7)
    assert out["status"] == "off_season"
    assert "season starts Fri 24 Jul" in out["note"]


@pytest.mark.parametrize("nickname", ["Dockers", "Freo", "Eagles", "West Coast Eagles",
                                      "Fremantle Dockers", "fremantle"])
def test_afl_club_nicknames_find_the_derby(squiggle, nickname):
    _venue(get_db(), "afl-nick")
    out = _games("afl-nick", _perth_10am(date(2026, 7, 24)), days=7, team_filter=nickname)
    assert out["status"] == "ok"
    assert [g["matchup"] for g in out["games"]] == ["Fremantle v West Coast"], (nickname, out)


def test_afl_unrecognised_team_says_so_instead_of_no_games(squiggle):
    _venue(get_db(), "afl-unk")
    out = _games("afl-unk", _perth_10am(date(2026, 7, 24)), days=7, team_filter="Wombats")
    assert out["status"] == "team_not_recognised"
    assert out["games"] == []
    assert "isn't an AFL club" in out["note"] and "No " not in out["note"]


def test_resolve_teams_prefers_the_longer_club_name():
    from rosteriq.services.afl_fixtures import resolve_teams
    assert resolve_teams("North Melbourne") == {"North Melbourne"}
    assert resolve_teams("Roos") == {"North Melbourne"}
    assert resolve_teams("Port Adelaide Power") == {"Port Adelaide"}
    assert resolve_teams("Western Sydney Giants") == {"Greater Western Sydney"}
    assert resolve_teams("Sydney Swans") == {"Sydney"}
    assert resolve_teams("Pies") == {"Collingwood"}
    assert resolve_teams("St. Kilda") == {"St Kilda"}
    assert resolve_teams("") == set()


# ---------------------------------------------------------------------------
# [41] [42] Notifications
# ---------------------------------------------------------------------------

class _Outbox:
    def __init__(self):
        self.sent = []

    async def send(self, to_email, subject, html_content):
        if not to_email:
            return False
        self.sent.append((to_email, subject, html_content))
        return True


@pytest.fixture
def email_on(monkeypatch):
    from rosteriq.services.notifications import NotificationService
    from rosteriq.services.notification_hub import get_notification_hub
    svc = NotificationService()
    svc.smtp_user, svc.smtp_pass = "mailer@x.com", "app-password"
    box = _Outbox()
    monkeypatch.setattr(svc, "send_email", box.send)
    hub = get_notification_hub()
    monkeypatch.setattr(hub, "_email_service", svc)
    hub._dedup_cache.clear()
    yield box
    hub._dedup_cache.clear()


def _publish(c, h, rid):
    r = c.post(f"/api/v1/rosters/{rid}/publish", json={"skip_approval": True}, headers=h)
    assert r.status_code == 200, r.text
    return r.json()


def test_staff_never_reached_are_not_reported_already_notified(email_on):
    c = TestClient(app_())
    vid = "wem-dedup"
    h, _ = _manager_with_venue(c, vid)
    rows = _staff_rows(vid, 2)
    rows[0]["email"] = None
    assert c.post("/employees/bulk", json=rows, headers=h).status_code == 200
    db = get_db()
    tomorrow = _venue_today(vid) + timedelta(days=1)
    roster = _roster(db, vid, [r["id"] for r in rows], tomorrow, f"{vid}-r")
    body = _publish(c, h, roster.id)
    assert body["notifications_sent"] == 1

    # The manager adds Casual 0's email, then presses Send to Staff.
    e0 = db.get_employee(rows[0]["id"])
    db.save_employees([e0.model_copy(update={"email": f"{vid}-fixed@x.com"})])
    r = c.post("/api/v1/notifications/dispatch", json={
        "event_type": "ROSTER_PUBLISHED", "venue_id": vid,
        "payload": {"roster_id": roster.id}}, headers=h)
    assert r.status_code == 202, r.text
    out = r.json()
    assert out["reached"] == 1 and out["skipped"]["duplicate"] == 1
    assert f"{vid}-fixed@x.com" in [m[0] for m in email_on.sent]
    assert out["message"].startswith("1 staff notified (1 by email).")
    assert "already had it" in out["message"]

    # Pressed again straight away: everyone really was just sent it.
    again = c.post("/api/v1/notifications/dispatch", json={
        "event_type": "ROSTER_PUBLISHED", "venue_id": vid,
        "payload": {"roster_id": roster.id}}, headers=h).json()
    assert again["reached"] == 0 and again["skipped"]["duplicate"] == 2
    assert again["message"].startswith("Nobody was messaged again")


def test_publishing_next_week_right_after_this_week_still_notifies(email_on):
    c = TestClient(app_())
    vid = "wem-2wk"
    h, _ = _manager_with_venue(c, vid)
    rows = _staff_rows(vid, 2)
    assert c.post("/employees/bulk", json=rows, headers=h).status_code == 200
    db = get_db()
    ids = [r["id"] for r in rows]
    this_week = _roster(db, vid, ids, _venue_today(vid) + timedelta(days=1), f"{vid}-this")
    next_week = _roster(db, vid, ids, _next_monday(vid) + timedelta(days=1), f"{vid}-next")
    assert _publish(c, h, this_week.id)["notifications_sent"] == 2
    second = _publish(c, h, next_week.id)
    assert second["notifications_sent"] == 2, second["message"]
    assert len(email_on.sent) == 4


def test_send_to_staff_refuses_a_draft_and_emails_nobody(email_on):
    c = TestClient(app_())
    vid = "wem-draft"
    h, _ = _manager_with_venue(c, vid, name="The Wembley Hotel")
    rows = _staff_rows(vid, 3)
    assert c.post("/employees/bulk", json=rows, headers=h).status_code == 200
    db = get_db()
    draft = _roster(db, vid, [r["id"] for r in rows], _next_monday(vid), f"gen-{vid}")

    from rosteriq.services.notification_hub import get_notification_hub
    assert get_notification_hub().reachable_staff_ids(vid) == []

    for payload in ({"roster_id": draft.id}, {"week": "next week"}):
        r = c.post("/api/v1/notifications/dispatch", json={
            "event_type": "ROSTER_PUBLISHED", "venue_id": vid, "payload": payload}, headers=h)
        assert r.status_code == 409, (payload, r.text)
        assert "publish" in r.json()["error"]["message"].lower()
    assert email_on.sent == []


def test_send_to_staff_for_an_imported_week_says_it_is_up_not_published(email_on):
    c = TestClient(app_())
    vid = "wem-imp"
    h, _ = _manager_with_venue(c, vid)
    rows = _staff_rows(vid, 2)
    assert c.post("/employees/bulk", json=rows, headers=h).status_code == 200
    db = get_db()
    monday = _next_monday(vid)
    imported = _roster(db, vid, [r["id"] for r in rows], monday,
                       f"imported-{vid}-{monday.isoformat()}")
    r = c.post("/api/v1/notifications/dispatch", json={
        "event_type": "ROSTER_PUBLISHED", "venue_id": vid,
        "payload": {"roster_id": imported.id}}, headers=h)
    assert r.status_code == 202, r.text
    assert r.json()["reached"] == 2
    pages = [m[2] for m in email_on.sent]
    assert all("is up." in p and "has been published" not in p for p in pages)


def test_send_to_staff_with_another_venues_roster_is_not_found(email_on):
    c = TestClient(app_())
    h, _ = _manager_with_venue(c, "wem-mine")
    db = get_db()
    _venue(db, "wem-theirs")
    db.save_employees([_emp("wem-theirs", "theirs-1", email="t@x.com")])
    theirs = _roster(db, "wem-theirs", ["theirs-1"], _next_monday("wem-theirs"), "theirs-r")
    db.update_roster_state(theirs.id, "published", "test", "t")
    r = c.post("/api/v1/notifications/dispatch", json={
        "event_type": "ROSTER_PUBLISHED", "venue_id": "wem-mine",
        "payload": {"roster_id": theirs.id}}, headers=h)
    assert r.status_code == 404
    assert email_on.sent == []


# ---------------------------------------------------------------------------
# [43] Generate Optimal Roster: future weeks only; Discard discards
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


def test_optimiser_refuses_a_week_that_has_started():
    c = TestClient(app_())
    vid = "wem-opt-now"
    h, _ = _manager_with_venue(c, vid, min_staff={"bar": 1})
    assert c.post("/employees/bulk", json=_staff_rows(vid, 4), headers=h).status_code == 200
    today = _venue_today(vid)
    this_monday = today - timedelta(days=today.weekday())
    assert c.post("/forecasts", json=_forecast_rows(vid, this_monday), headers=h).status_code == 200
    db = get_db()
    monday = this_monday
    imported = _roster(db, vid, [f"{vid}-e0"], today, f"imported-{vid}-{monday.isoformat()}")
    before = {r.id for r in db.get_rosters_by_date_range(vid, monday, monday + timedelta(days=6))}

    r = c.post("/api/v1/rosters/optimise", json={
        "venue_id": vid, "week_start": today.isoformat(), "solver_preference": "greedy"},
        headers=h)
    assert r.status_code == 422, r.text
    msg = r.json()["error"]["message"]
    assert "already started" in msg
    assert _next_monday(vid).strftime("%a %d %b") in msg
    after = {r.id for r in db.get_rosters_by_date_range(vid, monday, monday + timedelta(days=6))}
    assert after == before
    from rosteriq.services.current_roster import current_roster
    assert current_roster(db, vid, today).id == imported.id


def test_discard_supersedes_the_optimised_draft():
    from rosteriq.services.current_roster import current_roster, roster_state
    c = TestClient(app_())
    vid = "wem-opt-disc"
    h, _ = _manager_with_venue(c, vid, min_staff={"bar": 1})
    assert c.post("/employees/bulk", json=_staff_rows(vid, 4), headers=h).status_code == 200
    monday = _next_monday(vid)
    assert c.post("/forecasts", json=_forecast_rows(vid, monday), headers=h).status_code == 200
    db = get_db()
    imported = _roster(db, vid, [f"{vid}-e0"], monday, f"imported-{vid}-{monday.isoformat()}")

    r = c.post("/api/v1/rosters/optimise", json={
        "venue_id": vid, "week_start": monday.isoformat(), "solver_preference": "greedy"},
        headers=h)
    assert r.status_code == 200, r.text
    rid = r.json()["roster_id"]
    assert rid and current_roster(db, vid, monday).id == rid

    d = c.post(f"/api/v1/rosters/optimise/discard/{rid}", headers=h)
    assert d.status_code == 200, d.text
    assert roster_state(db, rid) == "superseded"
    assert current_roster(db, vid, monday).id == imported.id

    # A published roster or the imported week can't be discarded from here.
    db.update_roster_state(rid, "published", "test", "t")
    assert c.post(f"/api/v1/rosters/optimise/discard/{rid}", headers=h).status_code == 409
    assert c.post(f"/api/v1/rosters/optimise/discard/{imported.id}", headers=h).status_code == 409


def test_discard_of_another_venues_roster_is_not_found():
    c = TestClient(app_())
    h, _ = _manager_with_venue(c, "wem-disc-mine")
    db = get_db()
    _venue(db, "wem-disc-theirs")
    theirs = _roster(db, "wem-disc-theirs", [], _next_monday("wem-disc-theirs"), "disc-theirs")
    r = c.post(f"/api/v1/rosters/optimise/discard/{theirs.id}", headers=h)
    assert r.status_code == 404
    from rosteriq.services.current_roster import roster_state
    assert roster_state(db, theirs.id) == "draft"


# ---------------------------------------------------------------------------
# [18] [25] Optimiser shifts: areas, the award's meal break, a week-costed total
# ---------------------------------------------------------------------------

def _area_staff(vid, n):
    return [_emp(vid, f"{vid}-a{i}", etype=EmploymentType.full_time,
                 skills=("front bar",) if i % 2 else ("kitchen",)) for i in range(n)]


def _week_forecasts(vid, monday, hours=range(11, 23), covers=40):
    from rosteriq.models import DemandForecast
    return [DemandForecast(id=f"{vid}-{d}-{h}", venue_id=vid, date=monday + timedelta(days=d),
                           hour=h, predicted_covers=covers, confidence=0.7, signals_used=[],
                           model_version="t")
            for d in range(7) for h in hours]


def test_optimiser_shifts_are_placed_in_the_venues_areas():
    from rosteriq.services.roster_optimiser_v2 import HybridOptimiser
    db = get_db()
    vid = "wem-v2-areas"
    _venue(db, vid, min_staff={"front bar": 1, "kitchen": 1})
    venue = db.get_venue(vid)
    staff = _area_staff(vid, 8)
    monday = date(2026, 10, 12)
    opt = HybridOptimiser(venue, staff, _week_forecasts(vid, monday), monday,
                          monday + timedelta(days=6))
    roster = opt._greedy_solve()
    assert roster.shifts
    roles = Counter(s.role for s in roster.shifts)
    assert "general" not in roles, roles
    by_id = {e.id: e for e in staff}
    for s in roster.shifts:
        assert s.role in by_id[s.employee_id].skills
    for d in range(7):
        day = monday + timedelta(days=d)
        assert {s.role for s in roster.shifts if s.date == day} == {"front bar", "kitchen"}


def test_assign_areas_spreads_generalists_across_areas_by_hour():
    from rosteriq.services.roster_optimiser_v2 import assign_areas
    venue = VenueConfig(id="v", name="V", tanda_org_id="", state=State.wa,
                        min_staff={"front bar": 1, "kitchen": 1}, max_labour_pct=30.0,
                        created_at=NOW)
    staff = [_emp("v", f"g{i}", skills=()) for i in range(4)]
    day = date(2026, 10, 12)
    times = [(6, 14), (10, 18), (14, 22), (17, 23)]
    shifts = [Shift(id=f"s{i}", employee_id=f"g{i}", date=day, start_time=time(a, 0),
                    end_time=time(b, 0), status=ShiftStatus.scheduled, role="general")
              for i, (a, b) in enumerate(times)]
    assign_areas(shifts, staff, venue)
    by_hour = {area: set() for area in ("front bar", "kitchen")}
    for s in shifts:
        by_hour[s.role] |= {h for _, h in s.clock_hours()}
    assert by_hour["front bar"] >= set(range(6, 22))
    assert by_hour["kitchen"] >= set(range(10, 23))


def test_optimiser_templates_use_the_award_meal_break():
    from rosteriq.award_rules import get_minimum_break_minutes
    from rosteriq.services import roster_optimiser_v2 as v2
    from rosteriq.services import auto_scheduler
    for templates in (v2.SHIFT_TEMPLATES, auto_scheduler.SHIFT_TEMPLATES):
        for name, (start, end, brk) in templates.items():
            assert brk == get_minimum_break_minutes(float(end - start)), name
    f = _emp("v", "ft", etype=EmploymentType.full_time)
    shift = v2.shift_template_to_shift("full_day", f, date(2026, 10, 12), State.wa)
    assert shift.break_minutes == get_minimum_break_minutes(9.0)


def test_optimiser_roster_total_is_costed_as_a_week():
    from rosteriq.cost_calculator import calculate_roster_cost
    from rosteriq.services.roster_optimiser_v2 import HybridOptimiser
    db = get_db()
    vid = "wem-v2-cost"
    _venue(db, vid)
    venue = db.get_venue(vid)
    # Three full-timers for a seven-day week of mornings: well past 38h each.
    staff = [_emp(vid, f"{vid}-f{i}", etype=EmploymentType.full_time) for i in range(3)]
    monday = date(2026, 10, 12)
    roster = HybridOptimiser(venue, staff, _week_forecasts(vid, monday, hours=range(6, 14)),
                             monday, monday + timedelta(days=6))._greedy_solve()
    expected = calculate_roster_cost(roster, {e.id: e for e in staff}, State.wa)
    assert roster.total_cost == expected
    assert expected != sum(s.cost for s in roster.shifts)


def test_auto_scheduler_places_areas_breaks_and_week_cost():
    from rosteriq.award_rules import get_minimum_break_minutes
    from rosteriq.cost_calculator import calculate_roster_cost
    from rosteriq.services.auto_scheduler import AutoScheduler
    db = get_db()
    vid = "wem-auto"
    _venue(db, vid, min_staff={"front bar": 1, "kitchen": 1})
    staff = _area_staff(vid, 6)
    db.save_employees(staff)
    monday = date(2026, 10, 12)
    db.add_forecasts(_week_forecasts(vid, monday))
    result = AutoScheduler(db).generate_week(vid, monday)
    shifts = result.roster.shifts
    assert shifts
    assert "general" not in {s.role for s in shifts}
    assert all(s.break_minutes == get_minimum_break_minutes(s.duration_hours) for s in shifts)
    expected = calculate_roster_cost(result.roster, {e.id: e for e in staff}, State.wa)
    assert result.roster.total_cost == expected == result.total_cost


# ---------------------------------------------------------------------------
# [45] Manual daily digest
# ---------------------------------------------------------------------------

@pytest.fixture
def digest_outbox(monkeypatch):
    import rosteriq.api as api
    from rosteriq.services.notifications import NotificationService
    svc = NotificationService()
    svc.smtp_user, svc.smtp_pass = "mailer@x.com", "pw"
    box = _Outbox()
    monkeypatch.setattr(svc, "send_email", box.send)
    monkeypatch.setattr(api, "get_notification_service", lambda: svc)
    return box


def test_digest_for_a_real_venue_invents_no_weather_or_events(digest_outbox, squiggle):
    c = TestClient(app_())
    h, email = _manager_with_venue(c, "wem-digest")
    r = c.post(f"/api/notifications/digest/wem-digest?manager_email={email}", headers=h)
    assert r.status_code == 200, r.text
    assert r.json()["status"] == "sent"
    (_, _, page), = digest_outbox.sent
    for invented in ("Sunny", "24°C", "Local market day", "AFL Round 10", "Weather:"):
        assert invented not in page, invented


def test_digest_lists_todays_real_afl_game(digest_outbox, squiggle, monkeypatch):
    from rosteriq.services import afl_fixtures
    real = afl_fixtures.upcoming_games

    async def derby_day(venue_id, days_ahead=14, team_filter="", db=None, now=None):
        return await real(venue_id, days_ahead=days_ahead, team_filter=team_filter, db=db,
                          now=_perth_10am(date(2026, 7, 24)))

    monkeypatch.setattr(afl_fixtures, "upcoming_games", derby_day)
    c = TestClient(app_())
    h, email = _manager_with_venue(c, "wem-digest-afl")
    r = c.post(f"/api/notifications/digest/wem-digest-afl?manager_email={email}", headers=h)
    assert r.status_code == 200, r.text
    (_, _, page), = digest_outbox.sent
    assert "AFL: Fremantle v West Coast, 18:10 at Optus Stadium" in page
    assert "Sunny" not in page and "AFL Round 10" not in page


# ---------------------------------------------------------------------------
# [46] AI compliance check
# ---------------------------------------------------------------------------

def _long_day_roster(vid, brk=0):
    db = get_db()
    _venue(db, vid)
    db.save_employees([_emp(vid, f"{vid}-c1")])
    today = _venue_today(vid)
    return _roster(db, vid, [f"{vid}-c1"], today, f"{vid}-r", start=time(9, 0),
                   end=time(21, 0), brk=brk)


def test_ai_compliance_check_flags_short_breaks_and_long_shifts_as_advice():
    from rosteriq.ai_agent import AgentContext
    from rosteriq.award_rules import get_minimum_break_minutes, MAX_SHIFT_LENGTH_HOURS
    _long_day_roster("ai-comp")
    data = json.loads(asyncio.run(AgentContext("ai-comp").execute_tool(
        "get_compliance_issues", {"date_range_days": 7})))
    kinds = Counter(i["type"] for i in data["issues"])
    assert kinds["break_violation"] == 1 and kinds["long_shift"] == 1, data
    brk = next(i for i in data["issues"] if i["type"] == "break_violation")
    assert "Staff ai-comp-c1" in brk["description"]
    assert f"expect {get_minimum_break_minutes(12.0)}min" in brk["description"]
    longest = next(i for i in data["issues"] if i["type"] == "long_shift")
    assert f"{MAX_SHIFT_LENGTH_HOURS:g}h" in longest["description"]
    blob = json.dumps(data)
    assert "Fair Work requires" not in blob and "cannot legally" not in blob
    assert "not legal advice" in data["note"]


def test_ai_compliance_check_passes_a_shift_with_its_break():
    from rosteriq.ai_agent import AgentContext
    _long_day_roster("ai-comp-ok", brk=60)
    data = json.loads(asyncio.run(AgentContext("ai-comp-ok").execute_tool(
        "get_compliance_issues", {"date_range_days": 7})))
    assert [i["type"] for i in data["issues"]] == ["long_shift"]


def test_ai_hours_come_from_the_shift_not_a_missing_field():
    from rosteriq.ai_agent import AgentContext, generate_insights
    _long_day_roster("ai-hours")
    ctx = AgentContext("ai-hours")
    shifts = json.loads(asyncio.run(ctx.execute_tool("get_shifts", {})))
    assert shifts["total_hours"] == 12.0
    labour = json.loads(asyncio.run(ctx.execute_tool("get_labour_summary", {})))
    assert labour["total_hours"] == 12.0
    insights = asyncio.run(generate_insights("ai-hours", max_insights=20))
    titles = [i["title"] for i in insights]
    assert any(t.startswith("This week: $") and "(12h)" in t for t in titles), titles
    card = next(i for i in insights if i["type"] == "compliance")
    assert "Fair Work requires" not in card["description"]
    assert "not legal advice" in card["description"]
