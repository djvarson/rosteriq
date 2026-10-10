"""
Wembley pilot, wave B8: the night-time tools a duty manager uses on the floor.

* SOS — who can I call? (services/sos.py, GET /api/sos, the AI's
  find_available_staff): approved leave and inactive staff are out, "already
  on" only when the shift overlaps, on-at-Apothecary, hours this week and the
  RECORDED visa cap, casuals first, salaried only when asked, unavailable
  only when asked and flagged, a ready-to-send text.
* Cut / hold (services/cut_advice.py, GET /api/ops/cut-advice, the AI's
  get_cut_recommendation): venue clock, only people on now, cost-calculator
  rates, area minimums, function / AFL holds, casual candidates finishing
  latest first, by name.
* Every SOS search and cut call shown is kept in the event log.
* On-Shift page: headcount by area, the whole on-shift list, no live-revenue
  toast; Roster page: a Day view of heads per half hour against minimums.
"""

import asyncio
import json
import os
import re
import shutil
import subprocess
import sys
import types
import uuid
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from zoneinfo import ZoneInfo

import pytest
from fastapi.testclient import TestClient

from rosteriq.database import get_db
from rosteriq.models import (
    AwardLevel, Employee, EmploymentType, Roster, Shift, ShiftStatus, State, VenueConfig,
)
from rosteriq.services import cut_advice, sos

VID = "b8-wembley"
APOTH = "b8-apothecary"
NOW = datetime(2026, 7, 1)
FRI = date(2026, 10, 9)
THU = FRI - timedelta(days=1)
MON = FRI - timedelta(days=4)
DAYS = ("mon", "tue", "wed", "thu", "fri", "sat", "sun")
PW = "Passw0rd!234"
assert FRI.weekday() == 4 and MON.weekday() == 0


def _venue(db, vid=VID, name="The Wembley Hotel", close="00:00", hours=True, areas=None):
    db.save_venue(VenueConfig(
        id=vid, name=name, tanda_org_id="", state=State.wa, timezone="Australia/Perth",
        areas=areas if areas is not None else [
            {"name": "front bar", "min_by_day": {d: 1 for d in DAYS}},
            {"name": "kitchen", "min_by_day": {d: 1 for d in DAYS}},
            {"name": "bottle shop", "min_by_day": {d: 1 for d in DAYS}},
            {"name": "duty manager", "min_by_day": {d: 1 for d in DAYS}},
        ],
        trading_hours={d: {"open": "11:00", "close": close} for d in DAYS} if hours else {},
        max_labour_pct=30.0, created_at=NOW,
    ))


def _emp(eid, name=None, etype=EmploymentType.casual, skills=("front bar",), vid=VID,
         rate="32.60", email=None, availability=None, visa_cap=None, phone="0400 111 222"):
    return Employee(
        id=eid, venue_id=vid, name=name or eid.title(), employment_type=etype,
        award_level=AwardLevel.level_2, state=State.wa, hourly_base_rate=Decimal(rate),
        phone=phone, email=email or f"{eid}@x.example", skills=list(skills),
        availability=availability or {}, visa_work_limit_fortnight=visa_cap,
        created_at=NOW, updated_at=NOW,
    )


def _shift(eid, day, start, end, role="front bar", brk=0, sid=None):
    return Shift(id=sid or f"s-{uuid.uuid4().hex[:8]}", employee_id=eid, date=day,
                 start_time=time.fromisoformat(start), end_time=time.fromisoformat(end),
                 break_minutes=brk, status=ShiftStatus.scheduled, role=role)


def _roster(db, shifts, monday=MON, vid=VID):
    db.save_roster(Roster(id=f"{vid}-r-{monday.isoformat()}", venue_id=vid, week_start=monday,
                          week_end=monday + timedelta(days=6), shifts=shifts, created_at=NOW))


class _With:
    """An Employee carrying a wave-B field (active, pay_basis) that its own
    branch adds to the model; every other attribute is the employee's."""

    def __init__(self, emp, **extra):
        object.__setattr__(self, "_emp", emp)
        self.__dict__.update(extra)

    def __getattr__(self, name):
        return getattr(self._emp, name)


def _override(monkeypatch, db, **by_id):
    real = db.get_employees

    def get_employees(venue_id=None):
        return [_With(e, **by_id[e.id]) if e.id in by_id else e for e in (real(venue_id) or [])]

    monkeypatch.setattr(db, "get_employees", get_employees)


def _leave(db, eid, start, end, status="approved"):
    db.save_leave_request({"id": f"lv-{uuid.uuid4().hex[:6]}", "venue_id": VID, "employee_id": eid,
                           "start_date": start, "end_date": end, "status": status,
                           "created_at": NOW})


def _ids(rows):
    return [r["id"] for r in rows]


# ---------------------------------------------------------------------------
# [39] SOS list: leave, inactive, overlap, other venue, visa, ordering
# ---------------------------------------------------------------------------

def test_sos_leaves_out_approved_leave_inactive_and_salaried(monkeypatch):
    db = get_db()
    _venue(db)
    db.save_employees([
        _emp("willow", "Willow Singh"), _emp("pending"), _emp("cas"), _emp("gone"),
        _emp("dana", "Dana VenueMgr", EmploymentType.full_time),
    ])
    _leave(db, "willow", THU, FRI + timedelta(days=2))
    _leave(db, "pending", FRI, FRI, status="pending")
    _override(monkeypatch, db, gone={"active": False}, dana={"pay_basis": "salaried"})

    r = sos.find_cover(db, VID, day=FRI, start="19:00", end="23:00", now=datetime(2026, 10, 9, 18, 40))
    assert sorted(_ids(r["available"])) == ["cas", "pending"]
    assert r["on_leave"] == [{"id": "willow", "name": "Willow Singh",
                              "from": THU.isoformat(), "to": (FRI + timedelta(days=2)).isoformat()}]
    every = json.dumps(r)
    assert '"gone"' not in every and "Dana VenueMgr" not in every
    assert r["salaried_left_out"] == 1 and any("salaried" in n for n in r["notes"])

    asked = sos.find_cover(db, VID, day=FRI, start="19:00", end="23:00", include_salaried=True,
                           now=datetime(2026, 10, 9, 18, 40))
    assert _ids(asked["available"])[-1] == "dana"


def test_sos_already_on_only_when_the_shift_overlaps_the_window():
    db = get_db()
    _venue(db)
    db.save_employees([_emp("early"), _emp("late"), _emp("tomorrow")])
    _roster(db, [_shift("early", FRI, "11:00", "17:00"), _shift("late", FRI, "18:00", "23:00"),
                 _shift("tomorrow", FRI + timedelta(days=1), "18:00", "23:00")])

    r = sos.find_cover(db, VID, day=FRI, start="19:00", end="23:00")
    by_id = {e["id"]: e for e in r["available"]}
    assert set(by_id) == {"early", "tomorrow"}
    assert by_id["early"]["shift_today"] == "11:00-17:00"
    assert "Rostered 11:00-17:00 that day" in by_id["early"]["flags"]
    assert [(e["id"], e["shift_here"]) for e in r["already_on"]] == [("late", "18:00-23:00")]
    assert r["already_on"][0]["flags"][0] == "Already on 18:00-23:00"


def test_sos_casuals_first_then_part_time_then_full_time():
    db = get_db()
    _venue(db)
    db.save_employees([_emp("ft", etype=EmploymentType.full_time),
                       _emp("pt", etype=EmploymentType.part_time), _emp("cas")])
    r = sos.find_cover(db, VID, day=FRI, start="19:00", end="23:00")
    assert [e["employment_type"] for e in r["available"]] == ["casual", "part_time", "full_time"]


def test_sos_shows_hours_this_week_and_the_recorded_visa_cap():
    db = get_db()
    _venue(db)
    db.save_employees([_emp("ruby", "Ruby Kaur", visa_cap=48), _emp("max", "Max Tran", visa_cap=48),
                       _emp("free")])
    prev = MON - timedelta(days=7)
    _roster(db, [_shift(e, prev + timedelta(days=d), "10:00", "18:00") for e in ("ruby", "max") for d in range(4)],
            monday=prev)
    _roster(db, [_shift("ruby", MON, "10:00", "16:30", brk=30), _shift("ruby", MON + timedelta(days=1), "10:00", "16:00"),
                 _shift("max", MON, "10:00", "17:00"), _shift("max", MON + timedelta(days=1), "10:00", "17:00")])

    r = sos.find_cover(db, VID, day=FRI, start="19:00", end="23:00")
    by_id = {e["id"]: e for e in r["available"]}
    ruby, mx = by_id["ruby"], by_id["max"]
    assert ruby["hours_this_week"] == 12.0                      # 6h + 6h after the unpaid break
    assert ruby["visa"]["cap_hours_fortnight"] == 48
    assert ruby["visa"]["rostered_fortnight_hours"] == 44.5     # 32h last week + 12.5h rostered
    assert ruby["visa"]["hours_left"] == 3.5 and ruby["visa"]["would_exceed"] is True
    assert mx["visa"]["rostered_fortnight_hours"] == 46 and mx["visa"]["would_exceed"] is True
    assert any("goes over it" in f for f in mx["flags"])
    assert by_id["free"]["visa"] is None
    # people a 4-hour call-in would push past their recorded cap sink below the rest
    assert _ids(r["available"])[0] == "free"

    fits = sos.find_cover(db, VID, day=FRI, start="19:00", end="22:00")
    ruby3 = next(e for e in fits["available"] if e["id"] == "ruby")
    assert ruby3["visa"]["would_exceed"] is False and "3.5h left" in ruby3["visa"]["message"]


def test_sos_unavailable_people_only_when_asked_and_flagged():
    db = get_db()
    _venue(db)
    db.save_employees([_emp("nofri", availability={"friday": []}),
                       _emp("eve", availability={"friday": [{"start": "17:00", "end": "23:59"}]})])
    r = sos.find_cover(db, VID, day=FRI, start="19:00", end="23:00")
    assert _ids(r["available"]) == ["eve"] and r["unavailable"] == [] and r["unavailable_hidden"] == 1
    r = sos.find_cover(db, VID, day=FRI, start="19:00", end="23:00", include_unavailable=True)
    assert _ids(r["unavailable"]) == ["nofri"]
    assert r["unavailable"][0]["status"] == "unavailable"
    assert r["unavailable"][0]["flags"][0] == "Marked unavailable then"


def test_sos_flags_someone_on_at_the_other_venue(monkeypatch):
    db = get_db()
    _venue(db)
    _venue(db, APOTH, "Apothecary")
    db.save_employees([_emp("meca", "Meca Shared", email="meca@x.example"), _emp("solo")])
    db.save_employees([_emp("meca-a", "Meca Shared", vid=APOTH, email="Meca@X.example ")])
    _roster(db, [_shift("meca-a", FRI, "16:00", "23:00")], vid=APOTH)

    fake = types.ModuleType("rosteriq.services.person_identity")

    def same_person_records(db_, emp, scope=None):
        if (emp.email or "").strip().lower() != "meca@x.example":
            return []
        return [(e, APOTH) for e in db_.get_employees(APOTH) if e.id == "meca-a"]

    def busy_elsewhere(db_, emp, day, start=None, end=None, **kw):
        assert (day, start, end) == (FRI, "19:00", "23:00")
        if not same_person_records(db_, emp):
            return []
        return [{"venue_id": APOTH, "venue_name": "Apothecary", "date": FRI.isoformat(),
                 "start": "16:00", "end": "23:00", "shift_id": "x"}]

    fake.same_person_records = same_person_records
    fake.busy_elsewhere = busy_elsewhere
    monkeypatch.setitem(sys.modules, "rosteriq.services.person_identity", fake)

    r = sos.find_cover(db, VID, day=FRI, start="19:00", end="23:00")
    assert _ids(r["available"]) == ["solo"]
    meca = r["already_on"][0]
    assert meca["id"] == "meca" and meca["status"] == "on_elsewhere"
    assert meca["flags"][0] == "On at Apothecary 16:00-23:00"
    assert meca["hours_this_week"] == 7.0


def test_sos_without_person_identity_treats_staff_as_single_venue(monkeypatch):
    db = get_db()
    _venue(db)
    db.save_employees([_emp("solo")])
    monkeypatch.setitem(sys.modules, "rosteriq.services.person_identity", None)
    r = sos.find_cover(db, VID, day=FRI, start="19:00", end="23:00")
    assert _ids(r["available"]) == ["solo"]


def test_sos_area_uses_venue_areas_and_duty_manager_needs_the_skill():
    db = get_db()
    _venue(db)
    db.save_employees([_emp("bar1", skills=("front bar",)), _emp("blank", skills=()),
                       _emp("alex", "Alex CasualDM", skills=("duty manager",)),
                       _emp("chef", skills=("kitchen",))])
    r = sos.find_cover(db, VID, day=FRI, start="19:00", end="23:00", areas=["Front Bar"])
    assert _ids(r["available"]) == ["bar1", "blank"]
    assert r["available"][0]["skill_match"] is True
    assert r["available"][1]["skill_match"] is False
    assert "No areas recorded for them" in r["available"][1]["flags"]
    r = sos.find_cover(db, VID, day=FRI, start="19:00", end="23:00", areas=["duty manager"])
    assert _ids(r["available"]) == ["alex"]


def test_sos_defaults_to_now_until_close_with_a_ready_text():
    db = get_db()
    _venue(db, close="23:00")
    db.save_employees([_emp("cas", phone="0412 345 678")])
    r = sos.find_cover(db, VID, now=datetime(2026, 10, 9, 18, 40))
    assert r["date"] == FRI.isoformat()
    assert (r["window"]["start"], r["window"]["end"]) == ("19:00", "23:00")
    assert r["message"] == "Can you come in 7-11pm tonight? — The Wembley Hotel"
    assert r["available"][0]["tel"] == "0412345678"

    _venue(db, close="00:00")
    assert sos.find_cover(db, VID, now=datetime(2026, 10, 9, 19, 10))["message"] == \
        "Can you come in 7:30pm-midnight tonight? — The Wembley Hotel"

    _venue(db, hours=False)
    r = sos.find_cover(db, VID, now=datetime(2026, 10, 9, 12, 5))
    assert (r["window"]["start"], r["window"]["end"]) == ("12:30", "16:30")
    assert "No trading hours are recorded" in r["notes"][0]
    assert r["message"].startswith("Can you come in 12:30-4:30pm today?")


def test_call_in_wording():
    d = lambda h, m=0, day=FRI: datetime.combine(day, time(h, m))
    assert sos.friendly_range(d(11), d(15)) == "11am-3pm"
    assert sos.friendly_range(d(22), d(1, day=FRI + timedelta(days=1))) == "10pm-1am"
    assert sos.friendly_range(d(19, 30), d(23)) == "7:30-11pm"
    assert sos.call_in_message("Apothecary", d(18), d(22, day=FRI), THU) == \
        "Can you come in 6-10pm tomorrow night? — Apothecary"


# ---------------------------------------------------------------------------
# [17] Cut / hold advice
# ---------------------------------------------------------------------------

def _friday_night(db):
    _venue(db)
    db.save_employees([
        _emp("sam", "Sam BarMgr", EmploymentType.full_time, rate="37.00"),
        _emp("sup", "Sup Level3", EmploymentType.part_time, rate="33.50"),
        _emp("ava", "Ava Nguyen"), _emp("leo", "Leo Smith"),
        _emp("kit", "Kit Brown", skills=("kitchen",)),
        _emp("alex", "Alex CasualDM", skills=("duty manager",), rate="41.00"),
        _emp("thu", "Thursday Close"),
    ])
    _roster(db, [
        _shift("sam", FRI, "11:00", "17:00"), _shift("sup", FRI, "11:00", "19:00"),
        _shift("ava", FRI, "17:00", "23:59"), _shift("leo", FRI, "18:00", "23:00"),
        _shift("kit", FRI, "16:00", "21:30", role="kitchen"),
        _shift("alex", FRI, "17:00", "23:59", role="duty manager"),
        _shift("thu", THU, "20:00", "01:00"),
        _shift("ava", FRI + timedelta(days=1), "17:00", "23:00"),
    ])


@pytest.fixture
def no_afl(monkeypatch):
    async def none(venue_id, db, aware_now):
        return {"status": "ok", "games": []}
    monkeypatch.setattr(cut_advice, "_afl_games", none)


def _probe(emp, start, end):
    from rosteriq.cost_calculator import calculate_shift_cost_breakdown
    s = Shift(id="p", employee_id=emp.id, date=start.date(), start_time=start.time(),
              end_time=end.time(), status=ShiftStatus.scheduled, role="")
    return calculate_shift_cost_breakdown(emp, s, State.wa).total_cost


def test_cut_advice_names_the_casual_finishing_latest_and_keeps_minimums(no_afl):
    db = get_db()
    _friday_night(db)
    at = datetime(2026, 10, 9, 21, 0)
    r = asyncio.run(cut_advice.advise(db, VID, takings=300, now=at))

    assert r["time"] == "21:00" and r["date"] == FRI.isoformat()
    assert sorted(p["name"] for p in r["on_shift"]) == ["Alex CasualDM", "Ava Nguyen", "Kit Brown", "Leo Smith"]
    emps = {e.id: e for e in db.get_employees(VID)}
    expected = sum(_probe(emps[i], at, at + timedelta(hours=1)) for i in ("ava", "leo", "kit", "alex"))
    assert r["labour_per_hour"] == round(float(expected), 2)
    net = Decimal("300") / Decimal("1.1")
    assert r["labour_pct"] == round(float(expected / net * 100), 1)
    assert r["labour_target_pct"] == 30.0

    assert r["recommendation"] == "cut"
    assert [c["name"] for c in r["candidates"]] == ["Ava Nguyen"]
    assert r["headline"] == "Cut 1: Ava Nguyen (till 23:59)"
    ava = r["candidates"][0]
    assert ava["saving_if_sent_now"] == round(float(_probe(emps["ava"], at, datetime(2026, 10, 9, 23, 59))), 2)
    assert r["area_minimums"]["front bar"] == {"on": 2, "minimum": 1}
    assert any("minimum engagement" in n for n in r["notes"])


def test_cut_advice_holds_when_within_target(no_afl):
    db = get_db()
    _friday_night(db)
    r = asyncio.run(cut_advice.advise(db, VID, takings=5000, now=datetime(2026, 10, 9, 21, 0)))
    assert r["recommendation"] == "hold" and r["candidates"] == []
    assert "within your 30% target" in r["headline"]


def test_cut_advice_holds_for_a_local_afl_game(monkeypatch):
    db = get_db()
    _friday_night(db)

    async def game(venue_id, db_, aware_now):
        return {"status": "ok", "games": [{"date": FRI.isoformat(), "kickoff": "19:40",
                                           "matchup": "Fremantle v Sydney", "signal": "home_game"}]}
    monkeypatch.setattr(cut_advice, "_afl_games", game)
    r = asyncio.run(cut_advice.advise(db, VID, takings=300, now=datetime(2026, 10, 9, 21, 0)))
    assert r["recommendation"] == "hold"
    assert r["headline"] == "Hold — Fremantle v Sydney kicked off at 19:40 (home game)."
    assert [c["name"] for c in r["candidates"]] == ["Ava Nguyen"]


def test_cut_advice_holds_for_a_function_booking(no_afl):
    db = get_db()
    _friday_night(db)
    db.save_direct_bookings(VID, [{"date": FRI.isoformat(), "party_size": 40, "time": "22:30"},
                                  {"date": FRI.isoformat(), "party_size": 4, "time": "22:00"}])
    r = asyncio.run(cut_advice.advise(db, VID, takings=300, now=datetime(2026, 10, 9, 21, 0)))
    assert r["recommendation"] == "hold"
    assert r["holds"] == ["A booking for 40 starts at 22:30"]
    assert r["information"] == ["4 covers booked before 23:30"]


def test_cut_advice_asks_for_takings_and_never_falls_back_to_all_areas(no_afl):
    db = get_db()
    _friday_night(db)
    at = datetime(2026, 10, 9, 21, 0)
    r = asyncio.run(cut_advice.advise(db, VID, now=at))
    assert r["recommendation"] == "need_sales"
    assert "takings" in r["headline"]
    assert [c["name"] for c in r["candidates"]] == ["Ava Nguyen"]

    shop = asyncio.run(cut_advice.advise(db, VID, areas=["bottle shop"], takings=300, now=at))
    assert shop["recommendation"] == "no_one_on" and shop["on_shift_count"] == 0

    bar = asyncio.run(cut_advice.advise(db, VID, areas=["front bar"], takings=200, now=at))
    assert sorted(p["name"] for p in bar["on_shift"]) == ["Ava Nguyen", "Leo Smith"]
    assert bar["recommendation"] == "cut" and [c["name"] for c in bar["candidates"]] == ["Ava Nguyen"]


def test_cut_advice_after_midnight_sees_last_nights_close(no_afl):
    db = get_db()
    _friday_night(db)
    r = asyncio.run(cut_advice.advise(db, VID, takings=300, now=datetime(2026, 10, 9, 0, 30)))
    assert [p["name"] for p in r["on_shift"]] == ["Thursday Close"]
    assert r["recommendation"] == "hold"        # the last person in the front bar stays


def test_cut_advice_leaves_salaried_out_of_hourly_labour_and_candidates(monkeypatch, no_afl):
    db = get_db()
    _friday_night(db)
    _override(monkeypatch, db, ava={"pay_basis": "salaried"})
    at = datetime(2026, 10, 9, 21, 0)
    r = asyncio.run(cut_advice.advise(db, VID, takings=300, now=at))
    emps = {e.id: e for e in get_db().get_employees(VID)}
    expected = sum(_probe(emps[i], at, at + timedelta(hours=1)) for i in ("leo", "kit", "alex"))
    assert r["labour_per_hour"] == round(float(expected), 2)
    assert any("salaried" in n for n in r["notes"])
    assert [c["name"] for c in r["candidates"]] == ["Leo Smith"]


def test_cut_advice_reads_the_venue_clock(monkeypatch, no_afl):
    db = get_db()
    _friday_night(db)
    monkeypatch.setattr(cut_advice, "venue_now",
                        lambda vid, db=None: datetime(2026, 10, 9, 21, 30, tzinfo=ZoneInfo("Australia/Perth")))
    r = asyncio.run(cut_advice.advise(db, VID, takings=300))
    assert r["time"] == "21:30" and r["on_shift_count"] == 3     # Kit finished at 21:30


# ---------------------------------------------------------------------------
# The AI gives the same answers as the On-Shift cards
# ---------------------------------------------------------------------------

def test_ai_tools_answer_from_the_same_services(monkeypatch, no_afl):
    from rosteriq.ai_agent import AgentContext
    db = get_db()
    _friday_night(db)
    db.save_employees([_emp("willow", "Willow Singh")])
    _leave(db, "willow", FRI, FRI)
    ctx = AgentContext(VID, user_id="ben-user")

    ai = json.loads(asyncio.run(ctx.execute_tool("find_available_staff", {
        "date": FRI.isoformat(), "start_time": "7pm", "end_time": "23:00", "role": "front bar"})))
    card = sos.find_cover(db, VID, day=FRI, start="19:00", end="23:00", areas=["front bar"],
                          include_unavailable=True)
    assert _ids(ai["available"]) == _ids(card["available"])
    assert "willow" not in _ids(ai["available"]) and ai["on_leave"][0]["name"] == "Willow Singh"
    assert {e["id"] for e in ai["already_on"]} == {"ava", "leo"}

    monkeypatch.setattr(cut_advice, "venue_now", lambda vid, db=None: datetime(2026, 10, 9, 21, 0))
    cut = json.loads(asyncio.run(ctx.execute_tool("get_cut_recommendation", {"current_hourly_sales": 300})))
    assert cut["recommendation"] == "cut" and [c["name"] for c in cut["candidates"]] == ["Ava Nguyen"]

    logged = db.list_events(venue_id=VID, category="audit")
    sources = {(e["action"], e["details"]["source"], e["user_id"]) for e in logged}
    assert ("ops.sos_search", "ai", "ben-user") in sources
    assert ("ops.cut_advice", "ai", "ben-user") in sources


# ---------------------------------------------------------------------------
# [81] GET /api/sos and /api/ops/cut-advice: gated, scoped, logged
# ---------------------------------------------------------------------------

def _app():
    from rosteriq.api import app
    return TestClient(app)


def _user(c, role, venue_ids, grants=None):
    email = f"b8{uuid.uuid4().hex[:8]}@x.example"
    c.post("/api/auth/register", json={"email": email, "password": PW, "name": "U"})
    tok = c.post("/api/auth/login", json={"email": email, "password": PW}).json()["access_token"]
    db = get_db()
    rec = db.get_user_by_email(email)
    rec["role"], rec["venue_ids"], rec["section_grants"] = role, list(venue_ids), grants or {}
    db.save_user(rec)
    return {"Authorization": f"Bearer {tok}"}, rec["id"]


def test_sos_api_is_for_managers_of_the_venue_and_department_scope():
    c = _app()
    _user(c, "owner", [])                      # first account is the platform owner
    db = get_db()
    _venue(db)
    _venue(db, APOTH, "Apothecary")
    db.save_employees([_emp("bar1"), _emp("chef", skills=("kitchen",))])
    q = f"/api/sos?venue_id={VID}&date={FRI.isoformat()}&start=19:00&end=23:00"

    staff, _ = _user(c, "staff", [VID])
    assert c.get(q, headers=staff).status_code == 403
    other, _ = _user(c, "manager", [APOTH])
    assert c.get(q, headers=other).status_code == 403
    bar_lead, _ = _user(c, "manager", [VID], {VID: ["front bar"]})
    assert c.get(q + "&area=kitchen", headers=bar_lead).status_code == 403
    mine = c.get(q, headers=bar_lead)
    assert mine.status_code == 200, mine.text
    assert mine.json()["areas"] == ["front bar"] and _ids(mine.json()["available"]) == ["bar1"]
    assert [a["name"] for a in mine.json()["area_choices"]] == ["front bar"]

    duty, duty_id = _user(c, "manager", [VID])
    r = c.get(q + "&include_unavailable=true", headers=duty)
    assert r.status_code == 200, r.text
    body = r.json()
    assert sorted(_ids(body["available"])) == ["bar1", "chef"]
    assert body["message"].startswith("Can you come in 7-11pm") and body["message"].endswith("— The Wembley Hotel")
    assert c.get(f"/api/sos?venue_id={VID}&start=25:00", headers=duty).status_code == 422

    events = [e for e in db.list_events(venue_id=VID, action_prefix="ops.sos") if e["user_id"] == duty_id]
    assert events, "the search a manager saw is kept"
    d = events[0]["details"]
    assert d["category"] == "audit" and d["source"] == "on_shift"
    assert d["inputs"]["include_unavailable"] is True and d["inputs"]["start"] == "19:00"
    assert sorted(d["shown"]["available"]) == ["Bar1", "Chef"]


def test_cut_advice_api_keeps_what_was_shown(monkeypatch, no_afl):
    c = _app()
    _user(c, "owner", [])
    db = get_db()
    _friday_night(db)
    monkeypatch.setattr(cut_advice, "venue_now", lambda vid, db=None: datetime(2026, 10, 9, 21, 0))
    staff, _ = _user(c, "staff", [VID])
    assert c.get(f"/api/ops/cut-advice?venue_id={VID}", headers=staff).status_code == 403
    duty, duty_id = _user(c, "manager", [VID])
    r = c.get(f"/api/ops/cut-advice?venue_id={VID}&takings=300", headers=duty)
    assert r.status_code == 200, r.text
    assert r.json()["recommendation"] == "cut"
    ev = [e for e in db.list_events(venue_id=VID, action_prefix="ops.cut_advice") if e["user_id"] == duty_id]
    assert ev and ev[0]["details"]["inputs"]["takings"] == 300
    assert ev[0]["details"]["shown"]["candidates"] == ["Ava Nguyen"]
    assert ev[0]["details"]["shown"]["time"] == "21:00"

    kitchen_lead, _ = _user(c, "manager", [VID], {VID: ["kitchen"]})
    k = c.get(f"/api/ops/cut-advice?venue_id={VID}&takings=300", headers=kitchen_lead)
    assert k.status_code == 200 and k.json()["areas"] == ["kitchen"]
    assert [p["name"] for p in k.json()["on_shift"]] == ["Kit Brown"]
    assert c.get(f"/api/ops/cut-advice?venue_id={VID}&area=front%20bar",
                 headers=kitchen_lead).status_code == 403


# ---------------------------------------------------------------------------
# [40] [84] [129] Dashboard: On-Shift now, SOS card, Day view, live revenue
# ---------------------------------------------------------------------------

HERE = os.path.dirname(os.path.abspath(__file__))
DASHBOARD = os.path.join(HERE, "..", "static", "dashboard.html")
NODE = shutil.which("node")
needs_node = pytest.mark.skipif(NODE is None, reason="node is required to run the dashboard JS")


def _html():
    with open(DASHBOARD, encoding="utf-8") as fh:
        return fh.read()


def _main_script():
    blocks = re.findall(r"<script[^>]*>([\s\S]*?)</script>", _html())
    assert len(blocks) == 2
    return blocks[1]


def _between(src, start, end):
    i = src.index(start)
    return src[i:src.index(end, i)]


HARNESS = r"""
const _els = {};
function el(id) {
  if (!_els[id]) _els[id] = { id, innerHTML: '', textContent: '', value: '', checked: false,
    style: {}, classList: { toggle() {} }, setAttribute() {} };
  return _els[id];
}
global.document = { getElementById: el };
global.esc = function (s) { return String(s == null ? '' : s).replace(/[&<>]/g, function (c) {
  return { '&': '&amp;', '<': '&lt;', '>': '&gt;' }[c]; }); };
global.localIso = function (d) { return d.toISOString().slice(0, 10); };
global.toasts = [];
global.showToast = function (m, k) { toasts.push([m, k]); };
global.banner = [];
global.connectionManager = { show() { banner.push('show'); }, hide() { banner.push('hide'); } };
global.state = { venues: [], currentVenue: 'v1', rosters: [], employees: [], isDemo: false };
global.setInterval = function () {};
"""


def _run_js(scenario):
    src = _main_script()
    ops = _between(src, "var OPS_WEEK_KEYS", "// ── Staff Contact Export")
    live = _between(src, "async function fetchAndUpdateLiveRevenue() {", "\nfunction updateVarianceDisplay(")
    prog = HARNESS + ops + "\n" + live + "\n(async () => {\n" + scenario + \
        "\n})().catch(e => { console.log(JSON.stringify({error: String(e && e.stack || e)})); });"
    out = subprocess.run([NODE, "-e", prog], capture_output=True, text=True, timeout=30)
    assert out.returncode == 0, out.stderr
    data = json.loads(out.stdout.strip().splitlines()[-1])
    assert "error" not in data, data.get("error")
    return data


VENUE_JS = """
const venue = { id: 'v1', name: 'The Wembley', timezone: 'Australia/Perth',
  areas: [{ name: 'front bar', label: 'Front Bar', min_by_day: { fri: 2 } },
          { name: 'kitchen', label: 'Kitchen', min_by_day: { fri: 1 } }],
  trading_hours: { fri: { open: '11:00', close: '01:00' } } };
const shifts = [
  { employee_id: 'a', date: '2026-10-09', start_time: '11:00:00', end_time: '17:00:00', role: 'Front Bar' },
  { employee_id: 'b', date: '2026-10-09', start_time: '17:00', end_time: '01:00', role: 'front bar' },
  { employee_id: 'c', date: '2026-10-09', start_time: '17:00', end_time: '01:00', role: 'front bar' },
  { employee_id: 'd', date: '2026-10-09', start_time: '16:00', end_time: '21:30', role: 'kitchen' },
  { employee_id: 'e', date: '2026-10-08', start_time: '20:00', end_time: '02:00', role: 'front bar' },
  { employee_id: 'f', date: '2026-10-09', start_time: '18:00', end_time: '23:00', role: 'functions', status: 'cancelled' },
];
"""


@needs_node
def test_day_view_counts_heads_per_half_hour_against_area_minimums():
    data = _run_js(VENUE_JS + """
      const d = dayTimelineData(shifts, venue, '2026-10-09');
      const at = (row, hhmm) => { const [h, m] = hhmm.split(':').map(Number); const i = d.slots.indexOf(h * 60 + m + (h < 6 ? 1440 : 0));
        const r = d.rows.find(x => x.name === row); return [r.counts[i], r.below[i]]; };
      const html = renderDayTimeline(d, { nowMinute: 21 * 60 + 10 });
      console.log(JSON.stringify({
        first: d.slots[0], last: d.slots[d.slots.length - 1], rows: d.rows.map(r => [r.name, r.min]),
        bar12: at('front bar', '12:00'), bar18: at('front bar', '18:00'), bar0030: at('front bar', '00:30'),
        kit16: at('kitchen', '16:00'), kit2130: at('kitchen', '21:30'),
        total18: d.total[d.slots.indexOf(18 * 60)], below: d.below,
        nowCells: (html.match(/dtl-cell[^"]* now/g) || []).length,
        belowCells: (html.match(/ below/g) || []).length,
        onlyBar: dayTimelineData(shifts, venue, '2026-10-09', { area: 'Front Bar' }).rows.map(r => r.name),
      }));
    """)
    assert data["first"] == 11 * 60 and data["last"] == 24 * 60 + 30
    assert data["rows"] == [["front bar", 2], ["kitchen", 1]]
    assert data["bar12"] == [1, True]           # one on, minimum two: red
    assert data["bar18"] == [2, False]
    assert data["bar0030"] == [2, False]        # the 01:00 close runs on past midnight
    assert data["kit16"] == [1, False] and data["kit2130"] == [0, True]
    assert data["total18"] == 3
    assert data["nowCells"] == 3                # each area row and the total
    assert data["belowCells"] >= data["below"] > 0
    assert data["onlyBar"] == ["front bar"]


@needs_node
def test_on_shift_now_includes_last_nights_close_and_not_tonights():
    data = _run_js(VENUE_JS + """
      const at0030 = opsOnShiftAt(shifts, '2026-10-09', 30).map(s => s.employee_id);
      const at2100 = opsOnShiftAt(shifts, '2026-10-09', 21 * 60).map(s => s.employee_id);
      const sat0030 = opsOnShiftAt(shifts, '2026-10-10', 30).map(s => s.employee_id);
      const td = opsTradingDayNow(venue, shifts, { iso: '2026-10-10', minute: 30 });
      console.log(JSON.stringify({ at0030, at2100, sat0030, td }));
    """)
    assert data["at0030"] == ["e"]              # Thursday's close, not Friday's evening shifts
    assert sorted(data["at2100"]) == ["b", "c", "d"]
    assert sorted(data["sat0030"]) == ["b", "c"]
    assert data["td"] == {"iso": "2026-10-09", "minute": 24 * 60 + 30}


@needs_node
def test_on_shift_list_groups_everyone_by_area():
    data = _run_js(VENUE_JS + """
      state.venues = [venue];
      const many = [];
      for (let i = 0; i < 14; i++) many.push({ employee_id: 'p' + i, date: '2026-10-09', start_time: '17:00', end_time: '23:00', role: i < 10 ? 'front bar' : 'kitchen' });
      state.rosters = [{ shifts: many }];
      state.employees = many.map(s => ({ id: s.employee_id, name: 'Person ' + s.employee_id }));
      opsVenueNow = function () { return { iso: '2026-10-09', minute: 19 * 60 }; };
      renderOnShiftNow();
      const list = el('on-shift-list').innerHTML;
      console.log(JSON.stringify({ count: el('headcount-now').textContent, items: (list.match(/staff-item/g) || []).length,
        groups: (list.match(/ops-group-title/g) || []).length, chips: el('headcount-areas').innerHTML,
        timeline: el('onshift-timeline').innerHTML.indexOf('dtl-table') >= 0 }));
    """)
    assert data["count"] == "14" and data["items"] == 14 and data["groups"] == 2
    assert "<strong>10</strong> / 2 Front Bar" in data["chips"]
    assert data["timeline"] is True


@needs_node
def test_sos_rows_call_and_text_with_escaped_names():
    data = _run_js("""
      const r = { message: "Can you come in 7-11pm tonight? — Tom's Bar", include_unavailable: false, unavailable_hidden: 2,
        available: [{ id: '1', name: '<img src=x onerror=alert(1)>', tel: '+61400111222', employment_type: 'casual',
                      hours_this_week: 12, flags: ['Rostered 11:00-17:00 that day'] },
                    { id: '2', name: 'No Phone', tel: null, employment_type: 'part_time', hours_this_week: 0, flags: [] }],
        unavailable: [], already_on: [{ name: 'Meca', flags: ['On at Apothecary 16:00-23:00'] }],
        on_leave: [{ name: 'Willow' }], notes: [] };
      const html = sosResultsHtml(r);
      opsLastSos = r;
      console.log(JSON.stringify({ html, nums: sosNumbers(r) }));
    """)
    html = data["html"]
    assert "<img" not in html and "&lt;img src=x" in html
    assert 'href="tel:+61400111222"' in html
    assert 'href="sms:+61400111222?&amp;body=Can%20you%20come%20in%207-11pm%20tonight%3F%20%E2%80%94%20Tom&#39;s%20Bar"' in html
    assert "Copy all numbers" in html and "No mobile on file" in html
    assert "2 more marked unavailable" in html
    assert "On at Apothecary 16:00-23:00" in html and "Willow" in html
    assert data["nums"] == ["+61400111222"]


@needs_node
def test_live_revenue_without_a_pos_hides_quietly():
    data = _run_js("""
      global.fetchAPI = async () => { throw new Error('401 Unauthorized'); };
      await fetchAndUpdateLiveRevenue();
      const afterError = { toasts: toasts.slice(), banner: banner.slice(), hidden: el('live-revenue-card').style.display };
      el('live-revenue-card').style.display = '';
      global.fetchAPI = async () => ({ venue_id: 'v1', current_hour: null, today_total: null, message: 'No revenue data yet' });
      await fetchAndUpdateLiveRevenue();
      console.log(JSON.stringify({ afterError, noFeed: el('live-revenue-card').style.display, toasts }));
    """)
    assert data["afterError"] == {"toasts": [], "banner": [], "hidden": "none"}
    assert data["noFeed"] == "none" and data["toasts"] == []


def test_on_shift_page_has_the_sos_card_and_the_roster_has_a_day_view():
    html = _html()
    page = _between(html, 'id="page-on-shift"', "<!-- STAFF PAGE -->")
    assert page.index("SOS — who can I call?") < page.index("Current Headcount")
    assert "Also show people marked unavailable (they might say yes)" in page
    assert 'id="cut-takings"' in page and 'id="onshift-timeline"' in page
    roster = _between(html, 'id="page-roster"', "<!-- Cost Summary -->")
    assert "rosterSetView('day')" in roster and 'id="roster-day-view"' in roster
    src = _main_script()
    assert "onShift.slice(0, 8)" not in src and "onNow.slice(0, 8)" not in src
    assert "Failed to fetch live revenue data" not in src
    assert "No recommendations right now" not in src


def test_ai_cut_call_reads_a_loose_area_name_as_the_venues_areas(monkeypatch, no_afl):
    from rosteriq.ai_agent import AgentContext
    db = get_db()
    _friday_night(db)
    monkeypatch.setattr(cut_advice, "venue_now", lambda vid, db=None: datetime(2026, 10, 9, 21, 0))
    r = json.loads(asyncio.run(AgentContext(VID).execute_tool("get_cut_recommendation", {"area": "bar"})))
    assert r["areas"] == ["front bar"]
    assert sorted(p["name"] for p in r["on_shift"]) == ["Ava Nguyen", "Leo Smith"]
    assert cut_advice.expand_area_names(db.get_venue(VID), ["kitchen", "nothing"]) == ["kitchen", "nothing"]


def test_ai_tools_keep_a_department_manager_to_their_areas(monkeypatch, no_afl):
    from rosteriq.ai_agent import AgentContext
    from rosteriq.middleware.tenant import TenantContext, _tenant_context
    db = get_db()
    _friday_night(db)
    monkeypatch.setattr(cut_advice, "venue_now", lambda vid, db=None: datetime(2026, 10, 9, 21, 0))
    _tenant_context.value = TenantContext(user_id="kitchen-lead", venue_ids=[VID], role="manager",
                                          section_grants={VID: ["kitchen"]})
    try:
        ctx = AgentContext(VID)
        cut = json.loads(asyncio.run(ctx.execute_tool("get_cut_recommendation", {"area": "bar"})))
        assert cut == {"error": "You manage kitchen here, not bar."}
        cut = json.loads(asyncio.run(ctx.execute_tool("get_cut_recommendation", {})))
        assert cut["areas"] == ["kitchen"] and [p["name"] for p in cut["on_shift"]] == ["Kit Brown"]
        found = json.loads(asyncio.run(ctx.execute_tool("find_available_staff", {
            "date": FRI.isoformat(), "start_time": "19:00", "end_time": "23:00"})))
        assert found["areas"] == ["kitchen"]
        assert {e["id"] for e in found["available"] + found["already_on"]} <= {"kit"}
        logged = db.list_events(venue_id=VID, action_prefix="ops.cut_advice")
        assert logged[0]["user_id"] == "kitchen-lead"
    finally:
        _tenant_context.value = None
