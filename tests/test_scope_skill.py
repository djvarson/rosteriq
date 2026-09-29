"""
Skill matrix endpoints are venue-private and manager-level.

routes/skill_matrix.py had no venue gate on any endpoint, and every
SkillMatrixService method read db.list_employees() (the whole platform) — so
any signed-in user could read any venue's staff names/skills, and even a
venue's OWN matrix, training gaps, absence simulation and hiring profile were
computed over (and named) other tenants' staff.
"""

import uuid

from fastapi.testclient import TestClient

from rosteriq.api import app
from rosteriq.database import get_db

PW = "Passw0rd!234"
VENUE_GETS = [
    "/api/v1/venues/{v}/skill-matrix",
    "/api/v1/venues/{v}/training-gaps",
    "/api/v1/venues/{v}/resilience-score",
    "/api/v1/venues/{v}/hiring-profile",
]


def _login(c, email):
    c.post("/api/auth/register", json={"email": email, "password": PW, "name": "U"})
    r = c.post("/api/auth/login", json={"email": email, "password": PW})
    return {"Authorization": f"Bearer {r.json()['access_token']}"}


def _emp(c, h, vid, eid, name, skills):
    r = c.post("/employees", json={
        "id": eid, "venue_id": vid, "name": name, "employment_type": "casual",
        "award_level": "level_2", "state": "wa", "hourly_base_rate": "31.50",
        "skills": skills,
        "created_at": "2026-07-01T00:00:00", "updated_at": "2026-07-01T00:00:00"},
        headers=h)
    assert r.status_code == 200, r.text


def _venue(c, tag, staff_name, skills):
    email = f"sk_{tag}@x.com"
    h = _login(c, email)
    vid = f"sk-{tag}"
    assert c.post("/venues", json={"id": vid, "name": vid, "state": "wa", "max_labour_pct": 30,
                                   "tanda_org_id": "", "created_at": "2026-07-01T00:00:00"},
                  headers=h).status_code in (200, 201)
    h = _login(c, email)
    user = get_db().get_user_by_email(email)
    assert not user.get("is_owner") and user.get("role") == "manager", user
    eid = f"{vid}-e0"
    _emp(c, h, vid, eid, staff_name, skills)
    return h, vid, eid


def _two_venues():
    c = TestClient(app)
    _login(c, f"sk_boot_{uuid.uuid4().hex[:6]}@x.com")
    tag = uuid.uuid4().hex[:6]
    a = _venue(c, f"a{tag}", f"Alice Own {tag}", [f"bar_{tag}"])
    b = _venue(c, f"b{tag}", f"Bob Neighbour {tag}", [f"bsecret_{tag}"])
    return c, tag, a, b


def test_skill_endpoints_refuse_other_venues():
    c, _, (a_h, _, _), (_, b_vid, b_emp) = _two_venues()
    for ep in VENUE_GETS:
        r = c.get(ep.format(v=b_vid), headers=a_h)
        assert r.status_code == 403, (ep, r.status_code, r.text[:200])
    r = c.post(f"/api/v1/venues/{b_vid}/simulate-absence",
               params={"employee_id": b_emp}, headers=a_h)
    assert r.status_code == 403, r.text[:200]
    r = c.get(f"/api/v1/venues/employees/{b_emp}/versatility", headers=a_h)
    assert r.status_code == 404, r.text[:200]


def test_simulate_absence_refuses_other_venues_employee_on_own_venue():
    c, _, (a_h, a_vid, _), (_, _, b_emp) = _two_venues()
    r = c.post(f"/api/v1/venues/{a_vid}/simulate-absence",
               params={"employee_id": b_emp}, headers=a_h)
    assert r.status_code == 404, r.text[:200]
    assert "Bob Neighbour" not in r.text


def test_demo_cannot_reach_a_real_venues_skill_data():
    c, _, _, (_, b_vid, b_emp) = _two_venues()
    r = c.post("/api/auth/demo")
    assert r.status_code == 200, r.text
    demo_h = {"Authorization": f"Bearer {r.json()['access_token']}"}
    for ep in VENUE_GETS:
        assert c.get(ep.format(v=b_vid), headers=demo_h).status_code == 403, ep
    assert c.post(f"/api/v1/venues/{b_vid}/simulate-absence",
                  params={"employee_id": b_emp}, headers=demo_h).status_code == 403
    assert c.get(f"/api/v1/venues/employees/{b_emp}/versatility",
                 headers=demo_h).status_code == 404
    # The demo venue's own matrix must not include the real venue's staff either.
    r = c.get("/api/v1/venues/demo-venue-001/skill-matrix", headers=demo_h)
    assert r.status_code == 200, r.text[:200]
    assert b_emp not in r.json()["employees"]


def test_staff_member_of_the_venue_is_refused():
    c, tag, (a_h, a_vid, a_emp), _ = _two_venues()
    email = f"sk_staff_{tag}@x.com"
    _login(c, email)
    db = get_db()
    rec = db.get_user_by_email(email)
    rec["venue_ids"] = [a_vid]
    rec["role"] = "staff"
    db.save_user(rec)
    s_h = _login(c, email)
    for ep in VENUE_GETS:
        assert c.get(ep.format(v=a_vid), headers=s_h).status_code == 403, ep
    assert c.get(f"/api/v1/venues/employees/{a_emp}/versatility",
                 headers=s_h).status_code == 403


def test_own_venue_sees_only_own_staff():
    c, tag, (a_h, a_vid, a_emp), (b_h, b_vid, b_emp) = _two_venues()
    bar, cook, secret = f"bar_{tag}", f"zcook_{tag}", f"bsecret_{tag}"
    a_emp2 = f"{a_vid}-e1"
    _emp(c, a_h, a_vid, a_emp2, f"Carol Own {tag}", [cook])
    # B hires more staff with A's skill and a "hot" skill of its own; none of it
    # may reach A's answers.
    for i in range(4):
        _emp(c, b_h, b_vid, f"{b_vid}-x{i}", f"Bx{i} {tag}", [bar, f"bhot_{tag}"])

    r = c.get(f"/api/v1/venues/{a_vid}/skill-matrix", headers=a_h)
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["employees"] == sorted([a_emp, a_emp2])
    assert body["roles"] == [bar, cook]
    assert body["coverage"][bar]["trained_count"] == 1
    assert body["coverage"][bar]["total_employees"] == 2

    r = c.get(f"/api/v1/venues/{a_vid}/training-gaps", headers=a_h)
    assert r.status_code == 200, r.text
    assert "Bob Neighbour" not in r.text and secret not in r.text and b_vid not in r.text
    spof = [s for s in r.json()["spof_risks"] if s["role"] == bar]
    assert spof and spof[0]["sole_employee_id"] == a_emp
    assert [x["id"] for x in spof[0]["backup_candidates"]] == [a_emp2]

    assert c.get(f"/api/v1/venues/{a_vid}/resilience-score", headers=a_h).status_code == 200

    r = c.get(f"/api/v1/venues/employees/{a_emp}/versatility", headers=a_h)
    assert r.status_code == 200, r.text
    assert r.json()["critical_roles"] == [bar]
    assert r.json()["versatility_score"] == 60.0  # 1 of 2 venue skills + sole-trainer bonus

    r = c.post(f"/api/v1/venues/{a_vid}/simulate-absence",
               params={"employee_id": a_emp}, headers=a_h)
    assert r.status_code == 200, r.text
    assert set(r.json()["coverage_loss"]) == {bar, cook}
    assert r.json()["critical_gaps_created"] == [bar, cook]

    r = c.get(f"/api/v1/venues/{a_vid}/hiring-profile", headers=a_h)
    assert r.status_code == 200, r.text
    assert r.json()["primary_skill"] == bar
    assert r.json()["secondary_skills"] == [cook]
    assert f"bhot_{tag}" not in r.text and secret not in r.text
