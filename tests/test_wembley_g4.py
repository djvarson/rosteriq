"""
The generator and the same person across an owner's venues.

Ben owns The Wembley Hotel and Apothecary. Meca works both, with a staff
record at each; Sarah is Wembley's assistant venue manager and is linked to
Apothecary as staff only, so she can't open Apothecary in the dashboard.

Pins:
* a recorded visa fortnight cap counts last week's hours at the other
  venue, in the generator and in visa_flags, whether or not the person has
  another shift there this week; with the home venue's last week unknown the
  generator still plans no more than half the cap
* the generator keeps the rest gap across the week boundary against the
  person's other-venue shifts and this venue's own next week; weekly hours
  stay inside the week
* a manager who doesn't run the other venue still gets every double-booking
  check (generate, hand edits, SOS, cover approval, conflict scan, publish).
  What the check finds there names no venue and carries no ids ("another
  venue"), and the lookup never reaches a venue another owner runs
* that lookup only follows a person whose own login holds both venues, so
  an email typed onto a staff record reads nobody's shifts; and a venue a
  manager signed up themselves is their business, not their employer's
* the cover check says the person isn't on at another venue only when every
  one of their venues was checked, including when the logins can't be read
* POST /api/bulk/generate-rosters takes the POST /rosters/generate path:
  approved leave, the other venue's bookings, older drafts superseded, and
  says what generate says out loud (lapsed visas, other-venue bookings)
"""

import uuid
from datetime import date, datetime, time as dtime, timedelta
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient

from rosteriq.api import app
from rosteriq.award_rules import MINIMUM_HOURS_BETWEEN_SHIFTS
from rosteriq.database import get_db
from rosteriq.middleware.tenant import TenantContext, _tenant_context_var
from rosteriq.models import (
    AwardLevel, Employee, EmploymentType, Roster, Shift, ShiftStatus, State, VenueConfig,
)
from rosteriq.roster_optimiser import generate_weekly_roster
from rosteriq.services.clock import venue_today
from rosteriq.services.current_roster import roster_state

PW = "Passw0rd!234"
DAYS = ("mon", "tue", "wed", "thu", "fri", "sat", "sun")
BAR = [{"name": "bar", "min_by_day": {d: 1 for d in DAYS}}]
EVENINGS = {d: {"open": "16:00", "close": "23:00"} for d in DAYS}


def _login(c, email):
    c.post("/api/auth/register", json={"email": email, "password": PW, "name": email.split("@")[0]})
    r = c.post("/api/auth/login", json={"email": email, "password": PW})
    assert r.status_code == 200, r.text
    return {"Authorization": f"Bearer {r.json()['access_token']}"}


def _venue(c, h, vid, name, hours=EVENINGS):
    r = c.post("/venues", json={"id": vid, "name": name, "state": "wa", "max_labour_pct": 30,
                                "tanda_org_id": "", "timezone": "Australia/Perth", "areas": BAR,
                                "trading_hours": hours, "created_at": "2026-07-01T00:00:00"},
               headers=h)
    assert r.status_code in (200, 201), r.text


def _emp(c, h, vid, eid, name, rate="30.00", email=None, **extra):
    body = {"id": eid, "venue_id": vid, "name": name, "employment_type": "casual",
            "award_level": "level_2", "state": "wa", "hourly_base_rate": rate,
            "skills": ["bar"], "created_at": "2026-07-01T00:00:00",
            "updated_at": "2026-07-01T00:00:00"}
    if email:
        body["email"] = email
    body.update(extra)
    r = c.post("/employees", json=body, headers=h)
    assert r.status_code == 200, r.text
    return eid


def _link(c, mgr_h, eid, staff_h):
    code = c.get(f"/api/employees/{eid}/join-code", headers=mgr_h).json()["join_code"]
    r = c.post("/api/me/link", json={"code": code}, headers=staff_h)
    assert r.status_code == 200, r.text


def _monday(d):
    return d - timedelta(days=d.weekday())


def _shift(emp, day, start, end, sid=None, brk=0):
    return Shift(id=sid or f"s-{uuid.uuid4().hex[:8]}", employee_id=emp, date=day,
                 start_time=start, end_time=end, break_minutes=brk,
                 status=ShiftStatus.scheduled, role="bar")


def _roster(vid, week_start, shifts, state="published"):
    db = get_db()
    rid = f"{vid}-r{uuid.uuid4().hex[:6]}"
    db.save_roster(Roster(id=rid, venue_id=vid, week_start=week_start,
                          week_end=week_start + timedelta(days=6), shifts=shifts,
                          total_cost=None, created_at=datetime.utcnow()))
    if state:
        db.update_roster_state(rid, state, "seed", "test")
    return rid


def _world(cap=None, wembley_hours=EVENINGS, meca=None, meca_links_apothecary=True):
    """Ben runs both venues; Meca (the cheapest casual) has a record at each,
    linked to one login; two dearer Wembley casuals; Sarah is a full manager
    at Wembley and staff-only at Apothecary."""
    c = TestClient(app)
    tag = uuid.uuid4().hex[:6]
    _login(c, f"g4boot_{tag}@x.com")          # the platform owner is someone else
    ben = _login(c, f"ben_{tag}@x.com")
    vw, va = f"wem-{tag}", f"apo-{tag}"
    _venue(c, ben, vw, "The Wembley Hotel", wembley_hours)
    _venue(c, ben, va, "Apothecary")
    meca_email = f"meca_{tag}@x.com"
    extra = dict(meca or {})
    if cap:
        extra.update(visa_status="student", visa_work_limit_fortnight=cap)
    mw = _emp(c, ben, vw, f"{vw}-meca", "Meca Rossi", "20.00", email=meca_email, **extra)
    ma = _emp(c, ben, va, f"{va}-meca", "Meca Rossi", "20.00", email=meca_email,
              **({"visa_status": "student", "visa_work_limit_fortnight": cap} if cap else {}))
    meca_h = _login(c, meca_email)
    _link(c, ben, mw, meca_h)
    if meca_links_apothecary:
        _link(c, ben, ma, meca_h)
    pats =[_emp(c, ben, vw, f"{vw}-pat{i}", f"Pat {i}", "30.00", email=f"pat{i}_{tag}@x.com")
            for i in range(2)]

    sarah_email = f"sarah_{tag}@x.com"
    sw = _emp(c, ben, vw, f"{vw}-sarah", "Sarah Lee", "40.00", email=sarah_email,
              availability={d: [] for d in ("monday", "tuesday", "wednesday", "thursday",
                                            "friday", "saturday", "sunday")})
    sarah_h = _login(c, sarah_email)
    _link(c, ben, sw, sarah_h)
    r = c.post(f"/api/employees/{sw}/access-role", json={"role": "manager"}, headers=ben)
    assert r.status_code == 200, r.text
    sa = _emp(c, ben, va, f"{va}-sarah", "Sarah Lee", "40.00", email=sarah_email)
    sarah_h = _login(c, sarah_email)
    _link(c, ben, sa, sarah_h)
    sarah_h = _login(c, sarah_email)
    acct = get_db().get_user_by_email(sarah_email)
    assert acct["venue_ids"] == [vw] and acct["staff_venue_ids"] == [va]

    week = _monday(venue_today(vw, get_db())) + timedelta(days=14)
    return SimpleNamespace(c=c, tag=tag, ben=ben, vw=vw, va=va, mw=mw, ma=ma, meca_h=meca_h,
                           meca_email=meca_email, pats=pats, sarah=sarah_h, week=week)


def _generate(w, headers, week=None):
    r = w.c.post("/rosters/generate", json={"venue_id": w.vw, "week_start": (week or w.week).isoformat()},
                 headers=headers)
    assert r.status_code == 200, r.text
    return r


def _gross(body, eid):
    shifts = [Shift(**{k: s[k] for k in ("id", "employee_id", "date", "start_time", "end_time",
                                         "break_minutes", "status", "role")})
              for s in body["shifts"] if s["employee_id"] == eid]
    return sum(s.duration_hours for s in shifts), shifts


class _As:
    """Run service code as a given caller (the tenant context a request has)."""

    def __init__(self, venue_ids, role="manager", is_owner=False):
        self.ctx = TenantContext(user_id="t", venue_ids=list(venue_ids), is_owner=is_owner, role=role)

    def __enter__(self):
        self.token = _tenant_context_var.set(self.ctx)

    def __exit__(self, *exc):
        _tenant_context_var.reset(self.token)


# --- recorded visa fortnight caps across both venues ------------------------------

def test_generator_counts_last_weeks_hours_at_the_other_venue_against_the_visa_cap():
    w = _world(cap=48)
    prev = w.week - timedelta(days=7)
    # 5 x 8h at Apothecary last week; Wembley's last week is known and hasn't got her
    _roster(w.va, prev, [_shift(w.ma, prev + timedelta(days=i), dtime(16), dtime(0)) for i in range(5)])
    _roster(w.vw, prev, [_shift(w.pats[0], prev, dtime(16), dtime(23))])
    body = _generate(w, w.ben).json()
    gross, _ = _gross(body, w.mw)
    assert gross <= 48 - 40, f"Meca got {gross}h at Wembley on top of 40h at Apothecary last week"


def test_with_wembleys_last_week_unknown_the_generator_keeps_both_limits():
    w = _world(cap=48)
    prev = w.week - timedelta(days=7)
    # 30h at Apothecary last week, no Wembley roster at all: cap - 30 = 18 is tighter than cap / 2
    _roster(w.va, prev, [_shift(w.ma, prev + timedelta(days=i), dtime(16), dtime(22)) for i in range(5)])
    body = _generate(w, w.ben).json()
    gross, _ = _gross(body, w.mw)
    assert 0 < gross <= 18


def _bar_venue(vid):
    return VenueConfig(id=vid, name="The Wembley Hotel", tanda_org_id="", state=State.wa,
                       timezone="Australia/Perth", min_staff={}, areas=BAR,
                       trading_hours={d: {"open": "10:00", "close": "17:00"} for d in DAYS},
                       max_labour_pct=30, created_at=datetime(2026, 7, 1))


def _model_emp(eid, rate, vid, **extra):
    return Employee(id=eid, venue_id=vid, name=eid, employment_type=EmploymentType.casual,
                    award_level=AwardLevel.level_2, state=State.wa, hourly_base_rate=rate,
                    skills=["bar"], created_at=datetime(2026, 7, 1), updated_at=datetime(2026, 7, 1),
                    **extra)


def test_other_venue_hours_alone_never_lift_the_half_cap_for_an_unknown_home_week():
    week = date(2026, 10, 19)
    venue = _bar_venue("wem-half")
    meca = _model_emp("h-meca", "20.00", "wem-half", visa_work_limit_fortnight=48.0)
    others = [_model_emp(f"h-{i}", "30.00", "wem-half") for i in range(3)]
    # 10h at the other venue last week; this venue's own last week unknown
    elsewhere = [_shift("h-meca", week - timedelta(days=d), dtime(16), dtime(21), sid=f"x{d}")
                 for d in (2, 3)]
    roster = generate_weekly_roster(week, [], [meca] + others, venue, existing_shifts=elsewhere)
    gross = sum(s.duration_hours for s in roster.shifts if s.employee_id == "h-meca")
    assert 0 < gross <= 24


def test_visa_flags_count_the_other_venues_last_week_with_nothing_there_this_week():
    w = _world(cap=48)
    prev = w.week - timedelta(days=7)
    _roster(w.va, prev, [_shift(w.ma, prev + timedelta(days=i), dtime(14), dtime(0)) for i in range(5)])
    body = _generate(w, w.ben).json()
    assert _gross(body, w.mw)[0] == 0
    flags = [f for f in body.get("visa_flags", []) if f["employee_id"] == w.mw]
    assert flags and flags[0]["prev_week_hours"] == 50 and flags[0]["rostered_fortnight_hours"] == 50


# --- rest across the week boundary -------------------------------------------------

DAY_AND_NIGHT = {d: {"open": "10:00", "close": "23:00"} for d in DAYS}
EDGES_ONLY = {"meca": {"availability": {"monday": [{"start": "00:00", "end": "17:00"}],
                                        "tuesday": [], "wednesday": [{"start": "00:00", "end": "23:59"}],
                                        "thursday": [], "friday": [], "saturday": [],
                                        "sunday": [{"start": "14:00", "end": "23:59"}]}}}


def _gaps_ok(mine, theirs):
    rest = timedelta(hours=MINIMUM_HOURS_BETWEEN_SHIFTS)
    for s in mine:
        a = datetime.combine(s.date, s.start_time)
        b = a + timedelta(hours=s.duration_hours)
        for t in theirs:
            c = datetime.combine(t.date, t.start_time)
            d = c + timedelta(hours=t.duration_hours)
            assert not (a < d and c < b), (s, t)
            assert (a - d if d <= a else c - b) >= rest, (s.date, s.start_time, t.date, t.start_time)


def test_generator_keeps_the_rest_gap_to_the_other_venue_either_side_of_the_week():
    w = _world(wembley_hours=DAY_AND_NIGHT, **EDGES_ONLY)
    sunday_before = w.week - timedelta(days=1)
    monday_after = w.week + timedelta(days=7)
    close = _shift(w.ma, sunday_before, dtime(17), dtime(2))
    early = _shift(w.ma, monday_after, dtime(6), dtime(12))
    # a heavy week there before doesn't eat into this week's hours here
    _roster(w.va, w.week - timedelta(days=7),
            [_shift(w.ma, w.week - timedelta(days=7 - i), dtime(16), dtime(23)) for i in range(5)]
            + [close])
    _roster(w.va, monday_after, [early])
    body = _generate(w, w.ben).json()
    _, mine = _gross(body, w.mw)
    assert w.week + timedelta(days=2) in {s.date for s in mine}
    _gaps_ok(mine, [close, early])


def test_generator_keeps_the_rest_gap_to_this_venues_next_week():
    w = _world(wembley_hours=DAY_AND_NIGHT, **EDGES_ONLY)
    monday_after = w.week + timedelta(days=7)
    early = _shift(w.mw, monday_after, dtime(6), dtime(12))
    _roster(w.vw, monday_after, [early], state=None)          # next week's draft here
    body = _generate(w, w.ben).json()
    _, mine = _gross(body, w.mw)
    _gaps_ok(mine, [early])


# --- a manager who doesn't run the other venue ------------------------------------

def test_wembleys_avm_generates_around_apothecary_without_seeing_its_name():
    w = _world()
    _roster(w.va, w.week, [_shift(w.ma, w.week + timedelta(days=i), dtime(10), dtime(23))
                           for i in range(7)])
    r = _generate(w, w.sarah)
    body = r.json()
    assert _gross(body, w.mw)[0] == 0
    assert body["booked_elsewhere"] and all("Meca Rossi is on at another venue 10:00-23:00" in m
                                            for m in body["booked_elsewhere"])
    assert "Apothecary" not in r.text and w.va not in r.text

    # Ben, who runs both, sees the name
    assert any("Apothecary" in m for m in _generate(w, w.ben).json()["booked_elsewhere"])


def test_wembleys_avm_gets_the_sos_and_hand_edit_checks_redacted():
    w = _world(cap=60)
    day = w.week + timedelta(days=2)
    _roster(w.va, w.week, [_shift(w.ma, w.week + timedelta(days=i), dtime(16), dtime(23))
                           for i in range(7)])
    r = w.c.get("/api/sos", params={"venue_id": w.vw, "date": day.isoformat(), "start": "18:00",
                                    "end": "22:00"}, headers=w.sarah)
    assert r.status_code == 200, r.text
    meca = next(e for e in r.json()["already_on"] if e["id"] == w.mw)
    assert meca["status"] == "on_elsewhere"
    assert meca["flags"][0] == "On at another venue 16:00-23:00"
    assert meca["elsewhere"] == [{"venue_id": None, "venue_name": "another venue",
                                  "start": "16:00", "end": "23:00"}]
    assert meca["visa"]["rostered_fortnight_hours"] == 49
    assert "Apothecary" not in r.text and w.va not in r.text

    rid = _roster(w.vw, w.week, [_shift(w.pats[0], day, dtime(16), dtime(23))], state=None)
    r = w.c.post(f"/rosters/{rid}/shifts", headers=w.sarah, json={
        "employee_id": w.mw, "date": day.isoformat(), "start_time": "18:00", "end_time": "22:00",
        "break_minutes": 0, "role": "bar"})
    assert r.status_code == 200, r.text
    busy = [x for x in r.json()["warnings"] if x["code"] == "busy_elsewhere"]
    assert busy and "another venue 16:00-23:00" in busy[0]["message"]
    assert "Apothecary" not in r.text and w.va not in r.text


def test_wembleys_avm_gets_the_conflict_scan_and_publish_warnings_redacted():
    w = _world()
    fri = w.week + timedelta(days=4)
    rid = _roster(w.vw, w.week, [_shift(w.mw, fri, dtime(16), dtime(23))], state=None)
    _roster(w.va, w.week, [_shift(w.ma, fri, dtime(18), dtime(22))])
    r = w.c.get(f"/api/v1/rosters/{rid}/conflicts", headers=w.sarah)
    assert r.status_code == 200, r.text
    rows = [x for x in r.json()["conflicts"] if x["conflict_type"] == "cross_venue"]
    assert [x["message"] for x in rows] == [
        f"Meca Rossi is on at another venue 18:00-22:00 on {fri.strftime('%a')} {fri.day} {fri.strftime('%b')}"]
    assert "Apothecary" not in r.text and w.va not in r.text

    r = w.c.post(f"/api/v1/rosters/{rid}/publish", json={"skip_approval": True}, headers=w.sarah)
    assert r.status_code == 200, r.text
    assert any("Meca Rossi is on at another venue 18:00-22:00" in m
               for m in r.json()["cross_venue_warnings"])
    assert "Apothecary" not in r.text and w.va not in r.text


def _offer_and_claim(w, day):
    kay = w.pats[1]
    kay_h = _login(w.c, f"pat1_{w.tag}@x.com")
    _link(w.c, w.ben, kay, kay_h)
    sid = f"{w.vw}-kay-{uuid.uuid4().hex[:4]}"
    _roster(w.vw, _monday(day), [_shift(kay, day, dtime(17), dtime(22), sid=sid)])
    r = w.c.post(f"/api/me/shifts/{sid}/cover", json={"reason": "sick"}, headers=kay_h)
    assert r.status_code == 200, r.text
    cover_id = r.json()["cover_id"]
    r = w.c.post(f"/api/me/cover/{cover_id}/claim", headers=w.meca_h)
    assert r.status_code == 200, r.text
    return cover_id


def _elsewhere_check(w, cover_id):
    r = w.c.get(f"/api/cover/{cover_id}/checks?venue_id={w.vw}", headers=w.sarah)
    assert r.status_code == 200, r.text
    return next(x for x in r.json()["checks"] if x["check"] == "elsewhere"), r


def test_wembleys_avm_cover_approval_is_blocked_by_a_shift_at_the_other_venue():
    w = _world()
    day = w.week + timedelta(days=4)
    cover_id = _offer_and_claim(w, day)
    elsewhere, _ = _elsewhere_check(w, cover_id)
    assert elsewhere["result"] == "ok"
    assert elsewhere["message"] == "Meca Rossi isn't rostered at another venue then"

    _roster(w.va, w.week, [_shift(w.ma, day, dtime(16), dtime(23))])
    elsewhere, r = _elsewhere_check(w, cover_id)
    assert elsewhere["result"] == "block" and "at another venue" in elsewhere["message"]
    assert "Apothecary" not in r.text
    r = w.c.post(f"/api/cover/{cover_id}/decide", json={"venue_id": w.vw, "approve": True},
                 headers=w.sarah)
    assert r.status_code == 409 and "another venue" in r.text and "Apothecary" not in r.text


def test_cover_check_says_when_the_other_venue_could_not_be_checked(monkeypatch):
    from rosteriq.services import person_identity
    w = _world()
    day = w.week + timedelta(days=4)
    cover_id = _offer_and_claim(w, day)

    def broken(*a, **k):
        raise RuntimeError("roster store unavailable")

    monkeypatch.setattr(person_identity, "unseen_shifts", broken)
    elsewhere, _ = _elsewhere_check(w, cover_id)
    assert elsewhere["result"] == "warn"
    assert "isn't rostered" not in elsewhere["message"]
    assert "couldn't" in elsewhere["message"].lower()


def test_the_lookup_never_reaches_a_venue_another_owner_runs():
    from rosteriq.services.person_identity import busy_elsewhere, unseen_venue_ids
    w = _world()
    db = get_db()
    olly = _login(w.c, f"olly_{w.tag}@x.com")
    vo = f"oth-{w.tag}"
    _venue(w.c, olly, vo, "Unrelated Pub")
    mo = _emp(w.c, olly, vo, f"{vo}-meca", "Meca R", email=f"meca_{w.tag}@x.com")
    fri = w.week + timedelta(days=4)
    _roster(w.va, w.week, [_shift(w.ma, fri, dtime(18), dtime(22))])
    _roster(vo, w.week, [_shift(mo, fri, dtime(10), dtime(14))])
    meca_w = db.get_employee(w.mw)

    with _As([w.vw]):
        assert unseen_venue_ids(db, w.vw) == [w.va]
        hits = busy_elsewhere(db, meca_w, fri)
        assert [(h["venue_id"], h["venue_name"], h["start"], h["end"], h["shift_id"]) for h in hits] == \
            [(None, "another venue", "18:00", "22:00", None)]
        assert unseen_venue_ids(db, vo) == []
    with _As([vo]):
        assert unseen_venue_ids(db, vo) == []
        assert busy_elsewhere(db, db.get_employee(mo), fri) == []
    with _As([w.vw, w.va], role="staff"):
        assert unseen_venue_ids(db, w.vw) == []
    with _As([w.va]):                     # not a manager of Wembley
        assert unseen_venue_ids(db, w.vw) == []


def _sarahs_cafe(w):
    """Sarah, Wembley's AVM, signs up her own cafe and makes Carl, who has
    nothing to do with Ben, its manager. A manager someone else appointed
    can no longer add venues, so this is a login from before that rule: the
    check here is the second line of defence for accounts that already
    exist."""
    vx = f"cafe-{w.tag}"
    with pytest.MonkeyPatch.context() as mp:
        mp.setattr("rosteriq.services.venue_access.is_manager_login", lambda _u: False)
        _venue(w.c, w.sarah, vx, "Sarah's Cafe")
    sarah = _login(w.c, f"sarah_{w.tag}@x.com")
    carl_email = f"carl_{w.tag}@x.com"
    cx = _emp(w.c, sarah, vx, f"{vx}-carl", "Carl", email=carl_email)
    _link(w.c, sarah, cx, _login(w.c, carl_email))
    r = w.c.post(f"/api/employees/{cx}/access-role", json={"role": "manager"}, headers=sarah)
    assert r.status_code == 200, r.text
    return vx, sarah, _login(w.c, carl_email)


def _carl_reads_nothing(w, vx, carl, mx, fri):
    r = w.c.get("/api/sos", params={"venue_id": vx, "date": fri.isoformat(), "start": "18:00",
                                    "end": "22:00"}, headers=carl)
    assert r.status_code == 200, r.text
    probe = next(e for e in r.json()["available"] + r.json()["already_on"] if e["id"] == mx)
    assert probe["status"] != "on_elsewhere" and not probe.get("elsewhere")
    assert probe["hours_this_week"] == 0
    assert "another venue" not in r.text

    r = w.c.post("/rosters/generate", json={"venue_id": vx, "week_start": w.week.isoformat()},
                 headers=carl)
    assert r.status_code == 200, r.text
    assert not r.json().get("booked_elsewhere") and "another venue" not in r.text

    rid = _roster(vx, w.week, [], state=None)
    r = w.c.post(f"/rosters/{rid}/shifts", headers=carl, json={
        "employee_id": mx, "date": fri.isoformat(), "start_time": "18:00", "end_time": "22:00",
        "break_minutes": 0, "role": "bar"})
    assert r.status_code == 200, r.text
    assert not [x for x in r.json()["warnings"] if x["code"] == "busy_elsewhere"]


def test_a_manager_of_a_cafe_a_wembley_manager_signed_up_reads_no_wembley_shifts():
    w = _world()
    vx, _, carl = _sarahs_cafe(w)
    fri = w.week + timedelta(days=4)
    _roster(w.vw, w.week, [_shift(w.mw, fri, dtime(17), dtime(23))])
    # Carl types Meca's email onto a record at his cafe; Meca never joined it
    mx = _emp(w.c, carl, vx, f"{vx}-probe", "Probe", email=w.meca_email)
    _carl_reads_nothing(w, vx, carl, mx, fri)


def test_someone_who_really_works_at_both_businesses_still_isnt_shared_with_the_cafe():
    from rosteriq.services.person_identity import unseen_venue_ids
    w = _world()
    db = get_db()
    vx, sarah, carl = _sarahs_cafe(w)
    fri = w.week + timedelta(days=4)
    _roster(w.vw, w.week, [_shift(w.mw, fri, dtime(17), dtime(23))])
    # Meca also works at the cafe, joined with her own login: the cafe is
    # still Sarah's business, not Ben's
    mx = _emp(w.c, sarah, vx, f"{vx}-meca", "Meca Rossi", email=w.meca_email)
    _link(w.c, sarah, mx, w.meca_h)
    _carl_reads_nothing(w, vx, carl, mx, fri)
    with _As([vx]):
        assert unseen_venue_ids(db, vx) == []
    with _As([w.vw]):
        assert unseen_venue_ids(db, w.vw) == [w.va]


def test_an_email_typed_onto_a_record_reads_nobodys_shifts_even_inside_one_owner():
    from rosteriq.services import person_identity as pi
    w = _world()
    db = get_db()
    pat_email = f"pat0_{w.tag}@x.com"
    fri = w.week + timedelta(days=4)
    _roster(w.vw, w.week, [_shift(w.pats[0], fri, dtime(17), dtime(23))])
    # an Apothecary-only manager types a Wembley casual's email onto a record
    probe_id = _emp(w.c, w.ben, w.va, f"{w.va}-pat0", "Pat 0", email=pat_email)
    probe = db.get_employee(probe_id)
    rid = _roster(w.va, w.week, [_shift(probe_id, fri, dtime(16), dtime(20))], state=None)

    def reads():
        with _As([w.va]):
            assert pi.unseen_venue_ids(db, w.va) == [w.vw]
            return (pi.busy_elsewhere(db, probe, fri),
                    pi.busy_elsewhere_many(db, w.va, [probe], fri),
                    pi.booked_elsewhere(db, w.va, [probe], w.week, w.week + timedelta(days=6)),
                    pi.cross_venue_clashes(db, db.get_roster(rid)),
                    pi.unseen_shifts(db, probe, w.week, w.week + timedelta(days=6)))

    assert reads() == ([], {}, [], [], [])
    with _As([w.va]), pytest.raises(LookupError):
        pi.unseen_shifts(db, probe, w.week, w.week + timedelta(days=6), strict=True)

    # once Pat joins both venues with her own login, the check follows her
    pat_h = _login(w.c, pat_email)
    _link(w.c, w.ben, w.pats[0], pat_h)
    _link(w.c, w.ben, probe_id, pat_h)
    busy, many, booked, clashes, unseen = reads()
    assert [(h["venue_name"], h["start"], h["shift_id"]) for h in busy] == [("another venue", "17:00", None)]
    assert list(many) == [probe_id] and [b[1] for b in booked] == [None]
    assert [c["venue_name"] for c in clashes] == ["another venue"] and len(unseen) == 1


def test_cover_check_warns_when_the_other_venues_record_isnt_proven_to_be_them():
    w = _world(meca_links_apothecary=False)
    day = w.week + timedelta(days=4)
    cover_id = _offer_and_claim(w, day)
    _roster(w.va, w.week, [_shift(w.ma, day, dtime(16), dtime(23))])
    elsewhere, r = _elsewhere_check(w, cover_id)
    assert elsewhere["result"] == "warn" and "couldn't" in elsewhere["message"].lower()
    assert "Apothecary" not in r.text and "another venue" not in r.text


def test_cover_check_warns_when_the_logins_cannot_be_read(monkeypatch):
    w = _world()
    db = get_db()
    day = w.week + timedelta(days=4)
    cover_id = _offer_and_claim(w, day)
    _roster(w.va, w.week, [_shift(w.ma, day, dtime(16), dtime(23))])

    def broken():
        raise RuntimeError("server closed the connection unexpectedly")

    monkeypatch.setattr(db, "list_users", broken)
    elsewhere, _ = _elsewhere_check(w, cover_id)
    assert elsewhere["result"] == "warn"
    assert "isn't rostered" not in elsewhere["message"]
    assert "couldn't" in elsewhere["message"].lower()


# --- bulk generation ---------------------------------------------------------------

def test_bulk_generate_takes_the_generate_path():
    w = _world()
    db = get_db()
    nina = _emp(w.c, w.ben, w.vw, f"{w.vw}-nina", "Nina Patel", "19.00")
    db.save_leave_request({
        "id": f"{w.vw}-lv", "venue_id": w.vw, "employee_id": nina,
        "start_date": w.week, "end_date": w.week + timedelta(days=6), "reason": "Trip",
        "status": "approved", "created_at": datetime(2026, 10, 1)})
    control = _generate(w, w.ben).json()["id"]
    r = w.c.post("/forecasts/generate", json={"venue_id": w.va, "week_start": w.week.isoformat()},
                 headers=w.ben)
    assert r.status_code == 200, r.text

    r = w.c.post("/api/bulk/generate-rosters", json={"venue_ids": [w.vw, w.va],
                                                     "week_start": w.week.isoformat()}, headers=w.ben)
    assert r.status_code == 200, r.text
    out = r.json()
    assert out["successful"] == 2, out
    ids = {x["venue_id"]: x["roster_id"] for x in out["results"]}
    rw, ra = db.get_roster(ids[w.vw]), db.get_roster(ids[w.va])

    assert not [s for s in rw.shifts if s.employee_id == nina]
    here = [s for s in rw.shifts if s.employee_id == w.mw]
    there = [s for s in ra.shifts if s.employee_id == w.ma]
    assert here or there
    _gaps_ok(here, there)
    assert roster_state(db, control) == "superseded"

    # the per-venue manager gate still holds
    r = w.c.post("/api/bulk/generate-rosters", json={"venue_ids": [w.vw, w.va],
                                                     "week_start": w.week.isoformat()}, headers=w.sarah)
    assert r.status_code == 403


def test_bulk_generate_says_what_generate_says_out_loud():
    w = _world()
    nina = _emp(w.c, w.ben, w.vw, f"{w.vw}-nina", "Nina Patel", "15.00", visa_status="student",
                visa_expiry=(date.today() - timedelta(days=30)).isoformat())
    _roster(w.va, w.week, [_shift(w.ma, w.week + timedelta(days=4), dtime(16), dtime(23))])
    r = w.c.post("/api/bulk/generate-rosters", json={"venue_ids": [w.vw], "week_start": w.week.isoformat()},
                 headers=w.sarah)
    assert r.status_code == 200, r.text
    out = r.json()["results"][0]
    assert any(s.employee_id == nina for s in get_db().get_roster(out["roster_id"]).shifts)
    assert any("Nina Patel" in m and "expired" in m for m in out["warnings"]), out
    assert any("Meca Rossi is on at another venue 16:00-23:00" in m for m in out["warnings"]), out
    assert "Apothecary" not in r.text and w.va not in r.text
