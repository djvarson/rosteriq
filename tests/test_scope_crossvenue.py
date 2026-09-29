"""
Cross-venue schedule views are venue-private.

routes/cross_venue.py had no gate on any endpoint: any signed-in user
(including the public demo) could read another venue's employee's shifts,
hours and availability by id, list another tenant's shared staff and their
hours by passing its venue ids, and use check-schedule as a venue-name
oracle. The service also walked every roster on the platform, so even an
in-scope employee's shifts at a venue the caller does not hold leaked.
"""

import uuid
from datetime import date, timedelta

import pytest
from fastapi.testclient import TestClient

from rosteriq.api import app

PW = "Passw0rd!234"
WS = date.today() - timedelta(days=date.today().weekday())
WE = WS + timedelta(days=6)


def _login(c, email):
    c.post("/api/auth/register", json={"email": email, "password": PW, "name": "U"})
    r = c.post("/api/auth/login", json={"email": email, "password": PW})
    return {"Authorization": f"Bearer {r.json()['access_token']}"}


def _venue(c, h, vid, name):
    r = c.post("/venues", json={"id": vid, "name": name, "state": "vic", "max_labour_pct": 30,
                                "tanda_org_id": "", "created_at": "2026-07-01T00:00:00"}, headers=h)
    assert r.status_code in (200, 201), r.text


def _emp(c, h, vid, eid, name):
    r = c.post("/employees", json={
        "id": eid, "venue_id": vid, "name": name, "employment_type": "full_time",
        "award_level": "level_2", "state": "vic", "hourly_base_rate": "31.50",
        "created_at": "2026-07-01T00:00:00", "updated_at": "2026-07-01T00:00:00"}, headers=h)
    assert r.status_code == 200, r.text


def _roster_shifts(c, h, vid, eid):
    r = c.post("/rosters/generate", json={"venue_id": vid, "week_start": WS.isoformat()}, headers=h)
    assert r.status_code == 200, r.text
    items = c.get("/rosters", params={"venue_id": vid}, headers=h).json()["items"]
    shifts = [s for ro in items for s in ro["shifts"] if s["employee_id"] == eid]
    assert shifts, f"generator gave {eid} no shift at {vid}"
    return shifts


def _per_employee(c, h, eid, day):
    rng = {"start_date": WS.isoformat(), "end_date": WE.isoformat()}
    return {
        "shifts": c.get(f"/api/v1/employees/{eid}/cross-venue-shifts", params=rng, headers=h),
        "conflicts": c.get(f"/api/v1/employees/{eid}/cross-venue-conflicts", params=rng, headers=h),
        "hours": c.get(f"/api/v1/employees/{eid}/cross-venue-hours",
                       params={"week_start": WS.isoformat()}, headers=h),
        "availability": c.get(f"/api/v1/employees/{eid}/cross-venue-availability/{day}", headers=h),
    }


def _check(c, h, eid, vid, shift):
    return c.post("/api/v1/multi-venue/check-schedule", params={"employee_id": eid},
                  json={"venue_id": vid, "date": shift["date"],
                        "start_time": shift["start_time"], "end_time": shift["end_time"]},
                  headers=h)


def _multi(c, h, venues):
    ids = ",".join(venues)
    return {
        "shared": c.get("/api/v1/multi-venue/shared-employees", params={"venue_ids": ids}, headers=h),
        "conflicts": c.get("/api/v1/multi-venue/conflicts", params={
            "venue_ids": ids, "start_date": WS.isoformat(), "end_date": WE.isoformat()}, headers=h),
    }


@pytest.fixture
def world():
    """Owner (first user), manager A holding va + va2, manager B holding vb + vb2.
    A's staff member ea and B's staff member eb each work at BOTH of their
    manager's venues (transferred mid-week), so each tenant has a genuine
    shared employee."""
    c = TestClient(app)
    tag = uuid.uuid4().hex[:8]
    owner_h = _login(c, f"cv_boot_{tag}@x.com")
    ae, be = f"cvA_{tag}@x.com", f"cvB_{tag}@x.com"
    a_h, b_h = _login(c, ae), _login(c, be)
    w = dict(c=c, tag=tag, owner_h=owner_h,
             va=f"cvA-{tag}", va2=f"cvA2-{tag}", vb=f"cvB-{tag}", vb2=f"cvB2-{tag}",
             ea=f"cvEA-{tag}", eb=f"cvEB-{tag}",
             a_name=f"OwnStaff-{tag}", b_name=f"HiddenStaff-{tag}", b_venue=f"HiddenB-{tag}")
    _venue(c, a_h, w["va"], f"VenueA-{tag}")
    _venue(c, b_h, w["vb"], w["b_venue"])
    a_h, b_h = _login(c, ae), _login(c, be)
    _venue(c, a_h, w["va2"], f"VenueA2-{tag}")
    _venue(c, b_h, w["vb2"], f"HiddenB2-{tag}")
    a_h, b_h = _login(c, ae), _login(c, be)
    w.update(a_h=a_h, b_h=b_h)

    _emp(c, a_h, w["va"], w["ea"], w["a_name"])
    w["a_shifts"] = _roster_shifts(c, a_h, w["va"], w["ea"])
    _emp(c, a_h, w["va2"], w["ea"], w["a_name"])
    w["a2_shifts"] = _roster_shifts(c, a_h, w["va2"], w["ea"])

    _emp(c, b_h, w["vb"], w["eb"], w["b_name"])
    w["b_shifts"] = _roster_shifts(c, b_h, w["vb"], w["eb"])
    _emp(c, b_h, w["vb2"], w["eb"], w["b_name"])
    _roster_shifts(c, b_h, w["vb2"], w["eb"])
    return w


def _no_b(w, r):
    for secret in (w["b_name"], w["b_venue"], w["vb"]):
        assert secret not in r.text, (secret, r.text[:300])


def test_other_venues_employee_is_404_everywhere(world):
    w = world
    c, a_h = w["c"], w["a_h"]
    day = w["b_shifts"][0]["date"]
    for name, r in _per_employee(c, a_h, w["eb"], day).items():
        assert r.status_code == 404, (name, r.status_code, r.text[:200])
        _no_b(w, r)
    r = _check(c, a_h, w["eb"], w["va"], w["b_shifts"][0])
    assert r.status_code == 404, r.text[:200]
    _no_b(w, r)
    # a foreign id and a missing id are indistinguishable
    assert _per_employee(c, a_h, f"nope-{w['tag']}", day)["shifts"].status_code == 404


def test_multi_venue_refuses_other_venues(world):
    w = world
    c, a_h = w["c"], w["a_h"]
    for venues in ([w["vb"], w["vb2"]], [w["va"], w["vb"]]):
        for name, r in _multi(c, a_h, venues).items():
            assert r.status_code == 403, (venues, name, r.status_code, r.text[:200])
            _no_b(w, r)


def test_check_schedule_is_not_a_venue_name_oracle(world):
    w = world
    r = _check(w["c"], w["a_h"], w["ea"], w["vb"], w["a_shifts"][0])
    assert r.status_code == 403, r.text[:200]
    _no_b(w, r)


def test_venueless_login_reads_nothing(world):
    w = world
    c = w["c"]
    h = _login(c, f"cv_nobody_{w['tag']}@x.com")
    day = w["b_shifts"][0]["date"]
    for name, r in _per_employee(c, h, w["eb"], day).items():
        assert r.status_code == 404, (name, r.status_code)
    for name, r in _multi(c, h, [w["vb"], w["vb2"]]).items():
        assert r.status_code == 403, (name, r.status_code)


def test_demo_cannot_reach_a_real_venue(world):
    w = world
    c = w["c"]
    r = c.post("/api/auth/demo")
    assert r.status_code == 200, r.text
    demo_h = {"Authorization": f"Bearer {r.json()['access_token']}"}
    day = w["b_shifts"][0]["date"]
    for name, r in _per_employee(c, demo_h, w["eb"], day).items():
        assert r.status_code == 404, (name, r.status_code)
        _no_b(w, r)
    for name, r in _multi(c, demo_h, [w["vb"], w["vb2"]]).items():
        assert r.status_code == 403, (name, r.status_code)
    assert _check(c, demo_h, w["eb"], "demo-venue-001", w["b_shifts"][0]).status_code == 404
    # the demo's own seeded staff member, proposed at a real venue: refused
    assert _check(c, demo_h, "demo-staff-001", w["vb"], w["b_shifts"][0]).status_code == 403
    # and the demo still works on its own venue
    r = c.get("/api/v1/employees/demo-staff-001/cross-venue-shifts",
              params={"start_date": WS.isoformat(), "end_date": WE.isoformat()}, headers=demo_h)
    assert r.status_code == 200, r.text[:200]
    assert set(r.json()["shifts_by_venue"]) <= {"demo-venue-001"}


def test_in_scope_employee_hides_shifts_at_venues_the_caller_lacks(world):
    """An employee re-homed from B to A: A may see them, but not their B shifts."""
    w = world
    c, a_h = w["c"], w["a_h"]
    _emp(c, w["owner_h"], w["va"], w["eb"], w["b_name"])
    b_shift = w["b_shifts"][0]
    per = _per_employee(c, a_h, w["eb"], b_shift["date"])
    for r in per.values():
        assert r.status_code == 200, r.text[:200]
        assert w["vb"] not in r.text and w["b_venue"] not in r.text, r.text[:300]
    assert per["shifts"].json()["total_hours"] == 0
    assert per["hours"].json()["per_venue"] == {}
    assert per["conflicts"].json() == []
    r = _check(c, a_h, w["eb"], w["va"], b_shift)
    assert r.status_code == 200, r.text[:200]
    assert r.json()["conflicts"] == [] and r.json()["can_schedule"] is True
    # the platform owner still sees the whole picture
    r = c.get(f"/api/v1/employees/{w['eb']}/cross-venue-shifts",
              params={"start_date": WS.isoformat(), "end_date": WE.isoformat()},
              headers=w["owner_h"])
    assert r.status_code == 200 and w["vb"] in r.json()["shifts_by_venue"], r.text[:300]


def test_own_venues_still_work(world):
    w = world
    c, a_h = w["c"], w["a_h"]
    own = {w["va"], w["va2"]}
    per = _per_employee(c, a_h, w["ea"], w["a_shifts"][0]["date"])
    for name, r in per.items():
        assert r.status_code == 200, (name, r.text[:200])
        _no_b(w, r)
    assert set(per["shifts"].json()["shifts_by_venue"]) == own
    assert per["shifts"].json()["employee_name"] == w["a_name"]
    assert set(per["hours"].json()["per_venue"]) == own

    m = _multi(c, a_h, [w["va"], w["va2"]])
    for name, r in m.items():
        assert r.status_code == 200, (name, r.text[:200])
        _no_b(w, r)
    rows = m["shared"].json()
    assert [row["employee_id"] for row in rows] == [w["ea"]]
    assert set(rows[0]["venues"]) == own and rows[0]["total_weekly_hours"] > 0
    for row in m["conflicts"].json():
        assert {row["shift_a"]["venue_id"], row["shift_b"]["venue_id"]} <= own

    r = _check(c, a_h, w["ea"], w["va"], w["a2_shifts"][0])
    assert r.status_code == 200, r.text[:200]
    _no_b(w, r)
    for conflict in r.json()["conflicts"]:
        assert conflict["shift_b"]["venue_id"] == w["va2"]

    # B's own view of B's venues is unaffected
    r = _multi(c, w["b_h"], [w["vb"], w["vb2"]])["shared"]
    assert r.status_code == 200 and [row["employee_id"] for row in r.json()] == [w["eb"]]
