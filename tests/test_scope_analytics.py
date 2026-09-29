"""
Labour-cost analytics are manager-private to their venue.

routes/analytics.py, routes/cost_trends.py and the {venue_id} routes of
routes/industry_benchmarks.py had no venue gate, and the services priced
shifts against the PLATFORM-WIDE employee dict — so any signed-in user
(including the public demo and a venue-less self-registered account) could
read another venue's labour cost, casual mix, and staff names/pay rates.
"""

import uuid
from datetime import date, datetime, time as dtime, timedelta
from decimal import Decimal

import pytest
from fastapi.testclient import TestClient

from rosteriq.api import app
from rosteriq.database import get_db
from rosteriq.models import Roster, Shift, ShiftStatus
from rosteriq.services.demo import DEMO_VENUE_ID

PW = "Passw0rd!234"


def _login(c, email, register=True):
    if register:
        c.post("/api/auth/register", json={"email": email, "password": PW, "name": "U"})
    r = c.post("/api/auth/login", json={"email": email, "password": PW})
    return {"Authorization": f"Bearer {r.json()['access_token']}"}


def _employee(c, h, vid, eid, name, etype, rate):
    r = c.post("/employees", json={
        "id": eid, "venue_id": vid, "name": name, "employment_type": etype,
        "award_level": "level_2", "state": "vic", "hourly_base_rate": rate,
        "max_hours_per_week": 20,
        "created_at": "2025-01-01T00:00:00", "updated_at": "2025-01-01T00:00:00",
    }, headers=h)
    assert r.status_code == 200, r.text


def _roster(rid, vid, ws, shifts):
    get_db().save_roster(Roster(id=rid, venue_id=vid, week_start=ws,
                                week_end=ws + timedelta(days=6), shifts=shifts,
                                total_cost=None, created_at=datetime(2026, 7, 1)))


def _shifts(prefix, emp_id, ws, cost, start=dtime(8, 0), end=dtime(18, 0), brk=30, role="bar"):
    return [Shift(id=f"{prefix}{i}", employee_id=emp_id, date=ws + timedelta(days=i),
                  start_time=start, end_time=end, break_minutes=brk,
                  status=ShiftStatus.completed, role=role, cost=Decimal(cost))
            for i in range(6)]


@pytest.fixture
def w():
    c = TestClient(app)
    tag = uuid.uuid4().hex[:8]
    _login(c, f"sa_boot_{tag}@x.com")
    a_email, b_email = f"sa_mgrA_{tag}@x.com", f"sa_mgrB_{tag}@x.com"
    ha, hb = _login(c, a_email), _login(c, b_email)
    va, vb = f"sa-venA-{tag}", f"sa-venB-{tag}"
    for h, v in ((ha, va), (hb, vb)):
        r = c.post("/venues", json={"id": v, "name": v, "state": "vic", "max_labour_pct": 30,
                                    "tanda_org_id": "", "created_at": "2026-06-20T00:00:00"},
                   headers=h)
        assert r.status_code == 200, r.text
    ha, hb = _login(c, a_email, False), _login(c, b_email, False)
    db = get_db()
    for e, v in ((a_email, va), (b_email, vb)):
        u = db.get_user_by_email(e)
        assert not u.get("is_owner") and u.get("role") == "manager" and u["venue_ids"] == [v]

    a_emp, a_name = f"sa-empA-{tag}", f"AliceOwn-{tag}"
    b_emp, b_cas, b_name = f"sa-empB-{tag}", f"sa-empBc-{tag}", f"SecretBob-{tag}"
    _employee(c, ha, va, a_emp, a_name, "part_time", "40.00")
    _employee(c, hb, vb, b_emp, b_name, "part_time", "61.23")
    _employee(c, hb, vb, b_cas, f"CasualOfB-{tag}", "casual", "33.00")

    today = date.today()
    ws = today - timedelta(days=today.weekday() + 14)   # a past Monday
    # A: 6 x 9.5h at $250 = $1500 / 57h. B: 6 x 9.5h at $600 + 6 x 6h at $200 = $4800.
    _roster(f"sa-rosA-{tag}", va, ws, _shifts(f"sa-shA-{tag}-", a_emp, ws, "250.00"))
    _roster(f"sa-rosB-{tag}", vb, ws,
            _shifts(f"sa-shB-{tag}-", b_emp, ws, "600.00")
            + _shifts(f"sa-shBc-{tag}-", b_cas, ws, "200.00",
                      start=dtime(17, 0), end=dtime(23, 0), brk=0, role="floor"))
    # The week before, A's roster references B's employee id: A's own reports
    # must not resolve it against B's employee record (name, rate, contract).
    prev = ws - timedelta(days=7)
    _roster(f"sa-rosAx-{tag}", va, prev, _shifts(f"sa-shAx-{tag}-", b_emp, prev, "90.00"))

    return dict(c=c, ha=ha, hb=hb, va=va, vb=vb, a_name=a_name, b_name=b_name,
                a_emp=a_emp, b_emp=b_emp, tag=tag,
                start=ws.isoformat(), end=(ws + timedelta(days=6)).isoformat(),
                prev_start=prev.isoformat(), prev_end=(prev + timedelta(days=6)).isoformat())


def _calls(w, v, own=None):
    """Every venue-parameterised analytics endpoint, aimed at venue ``v``.
    ``own`` adds the mixed own+foreign list variants of the multi-venue ones."""
    s, e = w["start"], w["end"]
    calls = {
        "labour-trend": ("GET", f"/api/analytics/labour-trend/{v}", {}),
        "labour-breakdown": ("GET", f"/api/analytics/labour-breakdown/{v}",
                             {"params": {"start": s, "end": e}}),
        "forecast-accuracy": ("GET", f"/api/analytics/forecast-accuracy/{v}", {}),
        "accuracy-history": ("GET", f"/api/analytics/accuracy-history/{v}", {}),
        "benchmarks": ("GET", "/api/analytics/benchmarks",
                       {"params": {"venue_ids": v, "start": s, "end": e}}),
        "peak-hours": ("GET", f"/api/analytics/peak-hours/{v}", {}),
        "optimisation": ("GET", f"/api/analytics/optimisation/{v}", {}),
        "summary": ("GET", f"/api/analytics/summary/{v}", {}),
        "cost-trends": ("GET", "/api/v1/analytics/cost-trends",
                        {"params": {"venue_id": v, "start_date": s, "end_date": e}}),
        "cost-trends/compare": ("GET", "/api/v1/analytics/cost-trends/compare",
                                {"params": {"venue_ids": v, "start_date": s, "end_date": e}}),
        "cost-forecast": ("GET", "/api/v1/analytics/cost-forecast", {"params": {"venue_id": v}}),
        "overtime": ("GET", "/api/v1/analytics/overtime",
                     {"params": {"venue_id": v, "start_date": s, "end_date": e}}),
        "casual-dependency": ("GET", "/api/v1/analytics/casual-dependency",
                              {"params": {"venue_id": v, "start_date": s, "end_date": e}}),
        "industry-benchmark": ("GET", f"/api/v1/venues/{v}/industry-benchmark",
                               {"params": {"venue_type": "bar_pub", "start_date": s, "end_date": e}}),
        "benchmark-percentile": ("GET", f"/api/v1/venues/{v}/benchmark-percentile",
                                 {"params": {"venue_type": "bar_pub", "start_date": s, "end_date": e}}),
        "benchmark-recommendations": ("GET", f"/api/v1/venues/{v}/benchmark-recommendations",
                                      {"params": {"venue_type": "bar_pub", "start_date": s,
                                                  "end_date": e}}),
        "compare-venues": ("POST", "/api/v1/venues/benchmarks/compare-venues",
                           {"json": {"venue_configs": [{"venue_id": v, "venue_type": "bar_pub"}],
                                     "start_date": s, "end_date": e}}),
    }
    if own:
        calls["benchmarks[mixed]"] = ("GET", "/api/analytics/benchmarks",
                                      {"params": {"venue_ids": f"{own},{v}", "start": s, "end": e}})
        calls["cost-trends/compare[mixed]"] = (
            "GET", "/api/v1/analytics/cost-trends/compare",
            {"params": {"venue_ids": f"{own},{v}", "start_date": s, "end_date": e}})
        calls["compare-venues[mixed]"] = (
            "POST", "/api/v1/venues/benchmarks/compare-venues",
            {"json": {"venue_configs": [{"venue_id": own, "venue_type": "bar_pub"},
                                        {"venue_id": v, "venue_type": "bar_pub"}],
                      "start_date": s, "end_date": e}})
    return calls


def _run(c, h, calls):
    return {k: c.request(m, url, headers=h, **kw) for k, (m, url, kw) in calls.items()}


def _assert_refused(w, outs):
    bad = {k: (r.status_code, r.text[:160]) for k, r in outs.items()
           if r.status_code != 403 or w["b_name"] in r.text or "4800.0" in r.text}
    assert not bad, bad


def test_manager_cannot_read_another_venues_analytics(w):
    _assert_refused(w, _run(w["c"], w["ha"], _calls(w, w["vb"], own=w["va"])))


def test_demo_cannot_read_a_real_venues_analytics(w):
    c = w["c"]
    r = c.post("/api/auth/demo")
    assert r.status_code == 200, r.text
    demo_h = {"Authorization": f"Bearer {r.json()['access_token']}"}
    _assert_refused(w, _run(c, demo_h, _calls(w, w["vb"], own=DEMO_VENUE_ID)))
    # its own demo venue still works
    assert c.get(f"/api/analytics/labour-trend/{DEMO_VENUE_ID}", headers=demo_h).status_code == 200


def test_venueless_account_is_refused(w):
    c, tag = w["c"], w["tag"]
    email = f"sa_nobody_{tag}@x.com"
    h = _login(c, email)
    u = get_db().get_user_by_email(email)
    assert not u.get("is_owner") and u.get("role") != "owner" and not u.get("venue_ids")
    _assert_refused(w, _run(c, h, _calls(w, w["vb"])))


def test_staff_of_the_venue_cannot_read_its_labour_costs(w):
    """Pay/cost analytics are manager-level: venue membership alone is not enough."""
    c, tag = w["c"], w["tag"]
    email = f"sa_staffA_{tag}@x.com"
    _login(c, email)
    db = get_db()
    u = db.get_user_by_email(email)
    assert u.get("role") == "staff" and not u.get("is_owner")
    db.save_user({**u, "venue_ids": [w["va"]]})
    h = _login(c, email, False)
    outs = _run(c, h, _calls(w, w["va"]))
    bad = {k: r.status_code for k, r in outs.items() if r.status_code != 403}
    assert not bad, bad


def test_own_venue_analytics_still_work_and_hold_only_own_data(w):
    c, ha, va, vb = w["c"], w["ha"], w["va"], w["vb"]
    outs = _run(c, ha, _calls(w, va))
    bad = {k: (r.status_code, r.text[:200]) for k, r in outs.items()
           if r.status_code != 200 or w["b_name"] in r.text or vb in r.text or "4800.0" in r.text}
    assert not bad, bad

    j = outs["labour-breakdown"].json()
    assert j["total"]["total_cost"] == 1500.0 and j["total"]["unique_staff"] == 1, j["total"]
    assert outs["cost-trends"].json()["total_cost"] == 1500.0
    assert list(outs["cost-trends/compare"].json()["venues"]) == [va]
    assert outs["cost-trends/compare"].json()["venues"][va]["total_cost"] == 1500.0
    assert list(outs["benchmarks"].json()["venues"]) == [va]
    assert outs["benchmarks"].json()["venues"][va]["total_cost"] == 1500.0
    assert outs["industry-benchmark"].json()["total_labour_cost"] == 1500.0
    assert [x["venue_id"] for x in outs["compare-venues"].json()["comparisons"]] == [va]
    assert outs["casual-dependency"].json()["casual_cost"] == 0.0
    ot = outs["overtime"].json()["employees"]
    assert [e["employee_name"] for e in ot] == [w["a_name"]], ot
    # 57h worked against a 20h contract, priced at A's own $40 x 1.5
    assert ot[0]["overtime_hours"] == 37.0 and ot[0]["overtime_cost"] == 2220.0, ot


def test_own_report_does_not_resolve_a_foreign_employee_id(w):
    """Defence in depth: the services price shifts against the venue's OWN
    staff, so a shift in A's roster carrying B's employee id reveals nothing
    of B's employee record (name, pay rate, contract hours)."""
    r = w["c"].get("/api/v1/analytics/overtime", headers=w["ha"],
                   params={"venue_id": w["va"], "start_date": w["prev_start"],
                           "end_date": w["prev_end"]})
    assert r.status_code == 200, r.text
    assert w["b_name"] not in r.text and r.json()["employees"] == [], r.text


def test_operators_shared_staff_still_count_at_their_other_venue():
    """Scoping must not drop an operator's OWN shared staff: an employee
    homed at v1 who works shifts at v2 (both run by the same manager) is
    part of v2's labour cost. Only another tenant's staff are excluded."""
    c = TestClient(app)
    tag = uuid.uuid4().hex[:8]
    _login(c, f"sa_boot2_{tag}@x.com")
    email = f"sa_multi_{tag}@x.com"
    h = _login(c, email)
    v1, v2 = f"sa-v1-{tag}", f"sa-v2-{tag}"
    for v in (v1, v2):
        r = c.post("/venues", json={"id": v, "name": v, "state": "vic", "max_labour_pct": 30,
                                    "tanda_org_id": "", "created_at": "2026-06-20T00:00:00"}, headers=h)
        assert r.status_code == 200, r.text
    h = _login(c, email, False)
    _employee(c, h, v1, f"{v1}-shared", "Shared Sam", "casual", "30.00")
    _employee(c, h, v2, f"{v2}-local", "Local Lee", "casual", "30.00")
    ws = date(2026, 7, 6)
    _roster(f"r-{v2}", v2, ws, _shifts("sh", f"{v1}-shared", ws, "120") +
            _shifts("lo", f"{v2}-local", ws, "100"))
    r = c.get(f"/api/analytics/labour-breakdown/{v2}?start=2026-07-06&end=2026-07-12", headers=h)
    assert r.status_code == 200, r.text
    body = r.json()
    total = sum(float(v["total_cost"]) for v in body["by_day_type"].values())
    assert total == 6 * 120 + 6 * 100, body
    assert max(v["unique_staff"] for v in body["by_day_type"].values()) == 2, body
