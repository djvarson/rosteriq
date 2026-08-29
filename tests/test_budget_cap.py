"""
Budget-capped roster generation — the Tanda-parity beat, with the integrity
rule theirs doesn't state: adjust to budget, NEVER below minimum coverage,
and say so when a budget is unmeetable.

Pins:
* apply_budget_cap trims most-expensive-first until under the cap
* shifts that would drop any hour below the venue's min_staff are untouchable
* an unmeetable budget stops at the coverage floor with met=False and words
* POST /rosters/generate honours weekly_budget and reports the outcome
* the AI's generate_roster action EXECUTES (same engine, same scoping) —
  it used to 501 and point at the Roster tab
"""

import uuid
from datetime import date, datetime, time as dtime, timedelta
from decimal import Decimal

import pytest
from fastapi.testclient import TestClient

from rosteriq.api import app
from rosteriq.database import get_db
from rosteriq.models import (
    AwardLevel, Employee, EmploymentType, Roster, Shift, ShiftStatus, State,
)
from rosteriq.roster_optimiser import apply_budget_cap


PW = "Passw0rd!234"


def _emp(i, rate="30.00"):
    return Employee(
        id=f"bc-e{i}", venue_id="bc-v", name=f"Emp {i}",
        employment_type=EmploymentType.part_time, award_level=AwardLevel.level_2,
        state=State.wa, hourly_base_rate=Decimal(rate), skills=["bar"],
        created_at=datetime(2026, 7, 1), updated_at=datetime(2026, 7, 1))


def _shift(i, emp_id, day, start, end, cost):
    s = Shift(id=f"bc-s{i}", employee_id=emp_id, date=day,
              start_time=dtime(start, 0), end_time=dtime(end, 0),
              break_minutes=0, status=ShiftStatus.scheduled, role="bar")
    s.cost = Decimal(str(cost))
    return s


def _roster(shifts):
    day = shifts[0].date
    monday = day - timedelta(days=day.weekday())
    return Roster(id="bc-r", venue_id="bc-v", week_start=monday,
                  week_end=monday + timedelta(days=6), shifts=list(shifts),
                  created_at=datetime(2026, 7, 1))


# ---------------------------------------------------------------------------
# the reconciliation itself
# ---------------------------------------------------------------------------

def test_trims_most_expensive_first_until_under_cap():
    """The cap is derived from the REAL award-priced totals (the reconciler
    recomputes with calculate_roster_cost, not the per-shift labels), pitched
    so exactly one cut suffices — and the cut must be the priciest shift."""
    from rosteriq.roster_optimiser import calculate_roster_cost
    day = date(2026, 9, 2)
    emps = [_emp(1, "30.00"), _emp(2, "40.00"), _emp(3, "25.00")]
    shifts = [
        _shift(1, "bc-e1", day, 9, 17, 240),
        _shift(2, "bc-e2", day, 9, 17, 320),      # priciest — first out
        _shift(3, "bc-e3", day, 9, 17, 200),
    ]
    emp_dict = {e.id: e for e in emps}
    with_all = calculate_roster_cost(_roster(shifts), emp_dict, State.wa)
    without_s2 = calculate_roster_cost(_roster([shifts[0], shifts[2]]), emp_dict, State.wa)
    cap = (with_all + without_s2) / 2              # one cut fits, zero doesn't

    r = _roster(shifts)
    rep = apply_budget_cap(r, emps, State.wa, cap, {})
    assert rep["met"] is True
    assert [x["shift_id"] for x in rep["removed"]] == ["bc-s2"]
    assert {s.id for s in r.shifts} == {"bc-s1", "bc-s3"}
    assert Decimal(str(rep["final_cost"])) <= cap


def test_never_cuts_below_minimum_coverage():
    """With min_staff bar=2, only ONE of three overlapping shifts is
    removable; the budget stops at the coverage floor and says so."""
    day = date(2026, 9, 2)
    emps = [_emp(1), _emp(2), _emp(3)]
    shifts = [
        _shift(1, "bc-e1", day, 9, 17, 240),
        _shift(2, "bc-e2", day, 9, 17, 300),
        _shift(3, "bc-e3", day, 9, 17, 200),
    ]
    r = _roster(shifts)
    rep = apply_budget_cap(r, emps, State.wa, Decimal("100"), {"bar": 2})
    assert rep["met"] is False
    assert len(r.shifts) == 2                      # floor: two must remain
    assert "minimum coverage" in rep["note"]
    assert rep["final_cost"] > 100                 # honest about the miss


def test_already_under_budget_touches_nothing():
    day = date(2026, 9, 2)
    r = _roster([_shift(1, "bc-e1", day, 9, 17, 240)])
    rep = apply_budget_cap(r, [_emp(1)], State.wa, Decimal("1000"), {"bar": 1})
    assert rep["met"] is True and rep["removed"] == []
    assert len(r.shifts) == 1


# ---------------------------------------------------------------------------
# the route
# ---------------------------------------------------------------------------

def _world():
    c = TestClient(app)
    tag = uuid.uuid4().hex[:6]
    email = f"bcap_{tag}@x.com"
    c.post("/api/auth/register", json={"email": email, "password": PW, "name": "B"})
    db = get_db()
    rec = db.get_user_by_email(email)
    rec["role"] = "owner"
    db.save_user(rec)
    tok = c.post("/api/auth/login", json={"email": email, "password": PW}).json()["access_token"]
    h = {"Authorization": f"Bearer {tok}"}
    vid = f"bcap-{tag}"
    r = c.post("/venues", json={"id": vid, "name": vid, "state": "wa", "max_labour_pct": 30,
                                "tanda_org_id": "", "created_at": "2026-07-01T00:00:00"}, headers=h)
    assert r.status_code in (200, 201), r.text
    for i in range(4):
        r = c.post("/employees", json={
            "id": f"{vid}-e{i}", "name": f"P{i}", "employment_type": "casual",
            "award_level": "level_2", "state": "wa", "venue_id": vid,
            "hourly_base_rate": f"{28 + i}.00", "email": f"p{i}@x.com", "skills": ["bar"],
            "created_at": "2026-07-01T00:00:00", "updated_at": "2026-07-01T00:00:00"}, headers=h)
        assert r.status_code in (200, 201), r.text
    monday = date.today() + timedelta(days=(7 - date.today().weekday()) % 7 or 7)
    return c, h, vid, monday


def test_generate_with_budget_reports_the_outcome():
    c, h, vid, monday = _world()
    r = c.post("/rosters/generate", json={
        "venue_id": vid, "week_start": monday.isoformat(), "weekly_budget": 50,
    }, headers=h)
    assert r.status_code == 200, r.text
    body = r.json()
    assert "budget" in body, "budget outcome missing from the response"
    b = body["budget"]
    assert b["cap"] == 50
    assert isinstance(b["met"], bool) and b["note"]
    if not b["met"]:
        assert "minimum coverage" in b["note"] or "budget" in b["note"].lower()


def test_generate_without_budget_is_unchanged():
    c, h, vid, monday = _world()
    r = c.post("/rosters/generate", json={
        "venue_id": vid, "week_start": monday.isoformat()}, headers=h)
    assert r.status_code == 200, r.text
    assert "budget" not in r.json()


# ---------------------------------------------------------------------------
# the AI action is wired to the same engine
# ---------------------------------------------------------------------------

def test_ai_generate_action_actually_generates():
    c, h, vid, monday = _world()
    r = c.post("/api/ai/action", json={
        "venue_id": vid, "action_type": "generate_roster",
        "params": {"start_date": monday.isoformat(), "end_date": (monday + timedelta(days=6)).isoformat()},
    }, headers=h)
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["status"] == "done"
    assert "shifts" in body["message"] and "$" in body["message"]
    # the roster is REAL — saved and listable
    rosters = c.get("/rosters", headers=h).json()
    items = rosters.get("items") or rosters
    assert any(x["venue_id"] == vid for x in items), "no roster persisted"


def test_ai_generate_action_honours_the_budget_and_is_scoped():
    c, h, vid, monday = _world()
    r = c.post("/api/ai/action", json={
        "venue_id": vid, "action_type": "generate_roster",
        "params": {"start_date": monday.isoformat(), "budget_limit": 50},
    }, headers=h)
    assert r.status_code == 200, r.text
    assert r.json()["budget"] is not None

    # a manager of ANOTHER venue can't fire actions here
    tag2 = uuid.uuid4().hex[:6]
    email2 = f"bcap2_{tag2}@x.com"
    c.post("/api/auth/register", json={"email": email2, "password": PW, "name": "X"})
    db = get_db()
    rec = db.get_user_by_email(email2)
    rec["role"] = "manager"
    rec["venue_ids"] = ["some-other-venue"]
    db.save_user(rec)
    tok2 = c.post("/api/auth/login", json={"email": email2, "password": PW}).json()["access_token"]
    r = c.post("/api/ai/action", json={
        "venue_id": vid, "action_type": "generate_roster",
        "params": {"start_date": monday.isoformat()},
    }, headers={"Authorization": f"Bearer {tok2}"})
    assert r.status_code == 403, r.text


def test_ai_action_still_fails_loud_for_unwired_actions():
    c, h, vid, monday = _world()
    r = c.post("/api/ai/action", json={
        "venue_id": vid, "action_type": "send_message", "params": {}}, headers=h)
    assert r.status_code == 501
