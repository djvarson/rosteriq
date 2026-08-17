"""
Tenant scoping, round 3 — the route families the adversarial audit found
gating on ROLE (or nothing) without checking the record's VENUE:

* changelog + roster_diff (bare db.get_roster by id; venue path param)
* onboarding checklists (no auth dependency at all; employee/venue from URL)
* no-show predictor (venue/employee/shift ids from URL; backups drawn from
  ALL venues' staff)
* penalty calculator (employee_id in body → pay rate + name)
* preference learner (train/list by venue; profile/predict/rank/suggest by
  employee id; roster-score by roster id)
* POST /employees + /bulk upsert-by-client-id could re-home another venue's
  employee; POST /forecasts had no gate; /demo/load open to everyone; Tanda
  push/diff loaded any roster id
* legacy /api/staff swap board: global list; accept/reject by anyone
* account linking: unverified registration email auto-linked (and granted)
  another venue → replaced by a manager-issued JOIN CODE

Every by-id denial is a 404 identical to "missing" so ids are not an oracle;
venue-query denials are 404 too where the venue id came from the URL.
"""

import uuid
from datetime import date, datetime, time, timedelta

import pytest
from fastapi.testclient import TestClient

from rosteriq.api import app
from rosteriq.database import get_db
from rosteriq.models import Roster, Shift, ShiftStatus
from rosteriq.services import linking


PW = "Passw0rd!234"


def _register(c, email):
    r = c.post("/api/auth/register", json={"email": email, "password": PW, "name": "U"})
    assert r.status_code in (200, 201), r.text


def _login(c, email):
    r = c.post("/api/auth/login", json={"email": email, "password": PW})
    body = r.json()
    tok = body.get("access_token") or body.get("tokens", {}).get("access_token")
    assert tok, r.text
    return {"Authorization": f"Bearer {tok}"}


def _user(c, role, venue_ids, email=None):
    email = email or f"{role}_{uuid.uuid4().hex[:8]}@x.com"
    _register(c, email)
    db = get_db()
    rec = db.get_user_by_email(email)
    rec["role"] = role
    rec["venue_ids"] = list(venue_ids)
    db.save_user(rec)
    return _login(c, email)


def _venue(c, headers, vid):
    r = c.post("/venues", json={
        "id": vid, "name": vid, "state": "wa", "max_labour_pct": 30,
        "tanda_org_id": "", "created_at": "2026-06-20T00:00:00",
    }, headers=headers)
    assert r.status_code in (200, 201), r.text


def _emp_payload(eid, venue_id, email=None, rate="30.00"):
    return {
        "id": eid, "name": f"Emp {eid}", "employment_type": "casual",
        "award_level": "level_2", "state": "wa", "venue_id": venue_id,
        "hourly_base_rate": rate, "email": email if email is not None else f"{eid}@x.com",
        "skills": ["bar"],
        "created_at": "2026-06-20T00:00:00", "updated_at": "2026-06-20T00:00:00",
    }


def _employee(c, headers, eid, venue_id, **kw):
    r = c.post("/employees", json=_emp_payload(eid, venue_id, **kw), headers=headers)
    assert r.status_code in (200, 201), r.text


def _seed_roster(venue_id, rid, emp_id):
    db = get_db()
    d = date.today() + timedelta(days=3)
    sh = Shift(id=f"sh_{rid}", employee_id=emp_id, date=d,
               start_time=time(10, 0), end_time=time(16, 0), break_minutes=30,
               status=ShiftStatus.scheduled, role="bar")
    db.save_roster(Roster(
        id=rid, venue_id=venue_id,
        week_start=d - timedelta(days=d.weekday()),
        week_end=d - timedelta(days=d.weekday()) + timedelta(days=6),
        shifts=[sh], created_at=datetime(2026, 6, 20),
    ))
    return sh.id


def _err(r):
    body = r.json()
    return body.get("detail") or body.get("error", {}).get("message")


@pytest.fixture
def world():
    c = TestClient(app)
    tag = uuid.uuid4().hex[:6]
    ven_a, ven_b = f"r3A_{tag}", f"r3B_{tag}"
    owner = _user(c, "owner", [])
    _venue(c, owner, ven_a)
    _venue(c, owner, ven_b)
    emp_a, emp_b = f"empA_{tag}", f"empB_{tag}"
    _employee(c, owner, emp_a, ven_a)
    _employee(c, owner, emp_b, ven_b)
    ros_a, ros_b = f"rosA_{tag}", f"rosB_{tag}"
    shift_a = _seed_roster(ven_a, ros_a, emp_a)
    shift_b = _seed_roster(ven_b, ros_b, emp_b)
    mgr_a = _user(c, "manager", [ven_a])
    mgr_b = _user(c, "manager", [ven_b])
    return dict(c=c, tag=tag, ven_a=ven_a, ven_b=ven_b, owner=owner, mgr_a=mgr_a,
                mgr_b=mgr_b, emp_a=emp_a, emp_b=emp_b, ros_a=ros_a, ros_b=ros_b,
                shift_a=shift_a, shift_b=shift_b)


# ============================================================================
# changelog + roster_diff
# ============================================================================

@pytest.mark.parametrize("path", [
    "/api/v1/rosters/{rid}/changelog",
    "/api/v1/rosters/{rid}/changelog/version",
    "/api/v1/rosters/{rid}/changelog/export",
    "/api/v1/rosters/{rid}/changelog/stats",
    "/api/v1/rosters/{rid}/history",
])
def test_roster_by_id_routes_hide_foreign_rosters(world, path):
    c = world["c"]
    foreign = c.get(path.format(rid=world["ros_b"]), headers=world["mgr_a"])
    missing = c.get(path.format(rid="nope-" + world["tag"]), headers=world["mgr_a"])
    assert foreign.status_code == 404, foreign.text
    assert missing.status_code == 404
    assert _err(foreign) == _err(missing)          # same body: no oracle
    own = c.get(path.format(rid=world["ros_a"]), headers=world["mgr_a"])
    assert own.status_code == 200, own.text


def test_changelog_diff_and_revert_hide_foreign(world):
    c = world["c"]
    r = c.get(f"/api/v1/rosters/{world['ros_b']}/changelog/diff",
              params={"from_version": 1, "to_version": 2}, headers=world["mgr_a"])
    assert r.status_code == 404
    r = c.post(f"/api/v1/rosters/{world['ros_b']}/changelog/revert/1", headers=world["mgr_a"])
    assert r.status_code == 404


def test_recent_activity_venue_param_scoped(world):
    c = world["c"]
    r = c.get(f"/api/v1/venues/{world['ven_b']}/recent-changes", headers=world["mgr_a"])
    assert r.status_code == 404
    r = c.get(f"/api/v1/venues/{world['ven_a']}/recent-changes", headers=world["mgr_a"])
    assert r.status_code == 200


def test_compare_rosters_needs_both_in_scope(world):
    """POST /api/v1/rosters/compare is served by the optimiser router (query
    params); the roster_diff router's same-path handler is shadowed. Both are
    scoped now."""
    c = world["c"]
    r = c.post("/api/v1/rosters/compare",
               params={"original_roster_id": world["ros_a"], "optimised_roster_id": world["ros_b"]},
               headers=world["mgr_a"])
    assert r.status_code == 404, r.text
    r = c.post("/api/v1/rosters/compare",
               params={"original_roster_id": world["ros_a"], "optimised_roster_id": world["ros_a"]},
               headers=world["mgr_a"])
    assert r.status_code == 200, r.text
    # optimiser GET by id likewise
    assert c.get(f"/api/v1/rosters/{world['ros_b']}", headers=world["mgr_a"]).status_code == 404


def test_diff_from_previous_hides_foreign(world):
    c = world["c"]
    r = c.get(f"/api/v1/rosters/{world['ros_a']}/diff",
              params={"previous_roster_id": world["ros_b"]}, headers=world["mgr_a"])
    assert r.status_code == 404
    r = c.get(f"/api/v1/rosters/{world['ros_b']}/diff",
              params={"previous_roster_id": world["ros_a"]}, headers=world["mgr_a"])
    assert r.status_code == 404


# ============================================================================
# onboarding checklists
# ============================================================================

def test_onboarding_create_for_foreign_employee_404(world):
    c = world["c"]
    r = c.post(f"/api/v1/employees/{world['emp_b']}/onboarding",
               json={"employee_id": world["emp_b"], "venue_id": world["ven_b"], "role": "bar"},
               headers=world["mgr_a"])
    assert r.status_code == 404, r.text
    # nor by claiming it under my own venue
    r = c.post(f"/api/v1/employees/{world['emp_b']}/onboarding",
               json={"employee_id": world["emp_b"], "venue_id": world["ven_a"], "role": "bar"},
               headers=world["mgr_a"])
    assert r.status_code == 404, r.text


def test_onboarding_read_update_ready_remind_scoped(world):
    c = world["c"]
    # venue-B manager sets up their own employee
    r = c.post(f"/api/v1/employees/{world['emp_b']}/onboarding",
               json={"employee_id": world["emp_b"], "venue_id": world["ven_b"], "role": "bar"},
               headers=world["mgr_b"])
    assert r.status_code == 200, r.text
    item_id = r.json()["items"][0]["id"]
    # venue-A manager: every path is a 404
    assert c.get(f"/api/v1/employees/{world['emp_b']}/onboarding", headers=world["mgr_a"]).status_code == 404
    assert c.put(f"/api/v1/employees/{world['emp_b']}/onboarding/{item_id}",
                 json={"status": "completed"}, headers=world["mgr_a"]).status_code == 404
    assert c.get(f"/api/v1/employees/{world['emp_b']}/onboarding/ready", headers=world["mgr_a"]).status_code == 404
    assert c.post(f"/api/v1/employees/{world['emp_b']}/onboarding/remind",
                  json={}, headers=world["mgr_a"]).status_code == 404
    # venue-B manager and owner: fine
    assert c.get(f"/api/v1/employees/{world['emp_b']}/onboarding", headers=world["mgr_b"]).status_code == 200
    assert c.get(f"/api/v1/employees/{world['emp_b']}/onboarding", headers=world["owner"]).status_code == 200


def test_onboarding_venue_routes_scoped(world):
    c = world["c"]
    vb = world["ven_b"]
    assert c.get(f"/api/v1/venues/{vb}/onboarding-template", headers=world["mgr_a"]).status_code == 404
    assert c.put(f"/api/v1/venues/{vb}/onboarding-template", json={"items": []},
                 headers=world["mgr_a"]).status_code == 404
    assert c.get(f"/api/v1/venues/{vb}/onboarding-status", headers=world["mgr_a"]).status_code == 404
    assert c.post(f"/api/v1/venues/{vb}/onboarding/auto-remind", headers=world["mgr_a"]).status_code == 404
    assert c.get(f"/api/v1/venues/{vb}/onboarding-template", headers=world["mgr_b"]).status_code == 200


# ============================================================================
# no-show
# ============================================================================

def test_noshow_routes_scoped(world):
    c = world["c"]
    d0, d1 = date.today().isoformat(), (date.today() + timedelta(days=7)).isoformat()
    a = world["mgr_a"]
    assert c.get(f"/api/v1/venues/{world['ven_b']}/noshow-risk",
                 params={"date_from": d0, "date_to": d1}, headers=a).status_code == 404
    assert c.get(f"/api/v1/venues/{world['ven_b']}/risk-summary",
                 params={"date_from": d0, "date_to": d1}, headers=a).status_code == 404
    assert c.get(f"/api/v1/employees/{world['emp_b']}/reliability", headers=a).status_code == 404
    assert c.post(f"/api/v1/shifts/{world['shift_b']}/record-outcome",
                  json={"outcome": "no_show"}, headers=a).status_code == 404
    assert c.get(f"/api/v1/shifts/{world['shift_b']}/backups", headers=a).status_code == 404
    # own venue still works
    assert c.get(f"/api/v1/venues/{world['ven_a']}/noshow-risk",
                 params={"date_from": d0, "date_to": d1}, headers=a).status_code == 200
    assert c.get(f"/api/v1/employees/{world['emp_a']}/reliability", headers=a).status_code == 200


def test_noshow_backups_only_from_the_shifts_venue(world):
    """suggest_backups used to draw candidates from EVERY venue's staff."""
    c = world["c"]
    # add a second venue-B employee who is free and skilled — must not appear
    _employee(c, world["owner"], f"empB2_{world['tag']}", world["ven_b"])
    r = c.get(f"/api/v1/shifts/{world['shift_a']}/backups", headers=world["mgr_a"])
    assert r.status_code == 200, r.text
    ids = {b["employee_id"] for b in r.json()["backups"]}
    assert not any(i.startswith("empB") for i in ids), ids


# ============================================================================
# penalty + preferences
# ============================================================================

def test_penalty_calculator_hides_foreign_employee(world):
    c = world["c"]
    body = {"employee_id": world["emp_b"], "date": date.today().isoformat(),
            "start_time": "18:00", "end_time": "23:00", "role": "bar", "break_minutes": 0}
    assert c.post("/api/v1/calculate-penalty", json=body, headers=world["mgr_a"]).status_code == 404
    body["employee_id"] = world["emp_a"]
    r = c.post("/api/v1/calculate-penalty", json=body, headers=world["mgr_a"])
    assert r.status_code == 200, r.text


def test_preferences_routes_scoped(world):
    c = world["c"]
    a = world["mgr_a"]
    assert c.post(f"/api/preferences/train/{world['ven_b']}", headers=a).status_code == 404
    assert c.get(f"/api/preferences/venue/{world['ven_b']}/profiles", headers=a).status_code == 404
    assert c.get(f"/api/preferences/profile/{world['emp_b']}", headers=a).status_code == 404
    assert c.get(f"/api/preferences/suggestions/{world['emp_b']}", headers=a).status_code == 404
    assert c.get(f"/api/preferences/roster-score/{world['ros_b']}", headers=a).status_code == 404
    r = c.post("/api/preferences/rank-employees", headers=a, json={
        "shift_date": date.today().isoformat(), "shift_start_hour": 10, "shift_end_hour": 16,
        "role": "bar", "candidates": [world["emp_a"], world["emp_b"]]})
    assert r.status_code == 404, r.text
    # own venue works
    assert c.post(f"/api/preferences/train/{world['ven_a']}", headers=a).status_code == 200


# ============================================================================
# api.py: employee upsert, forecasts, demo/load, tanda
# ============================================================================

def test_employee_upsert_cannot_rehome_foreign_employee(world):
    c = world["c"]
    # venue-A manager posts venue-B's employee id under venue A with a new rate
    payload = _emp_payload(world["emp_b"], world["ven_a"], rate="99.00")
    r = c.post("/employees", json=payload, headers=world["mgr_a"])
    assert r.status_code == 404, r.text
    r = c.post("/employees/bulk", json=[payload], headers=world["mgr_a"])
    assert r.status_code == 404, r.text
    still = get_db().get_employee(world["emp_b"])
    assert still.venue_id == world["ven_b"]
    assert str(still.hourly_base_rate) != "99.00"
    # legitimately updating my own employee still works
    mine = _emp_payload(world["emp_a"], world["ven_a"], rate="31.00")
    assert c.post("/employees", json=mine, headers=world["mgr_a"]).status_code == 200


def test_forecasts_post_gated_by_venue(world):
    c = world["c"]
    f = {"id": f"fc_{world['tag']}", "venue_id": world["ven_b"], "date": date.today().isoformat(),
         "hour": 12, "predicted_covers": 40, "predicted_revenue": "1200.00", "confidence": 0.8,
         "model_version": "test", "created_at": "2026-06-20T00:00:00"}
    r = c.post("/forecasts", json=[f], headers=world["mgr_a"])
    assert r.status_code == 403, r.text
    f["venue_id"] = world["ven_a"]
    r = c.post("/forecasts", json=[f], headers=world["mgr_a"])
    assert r.status_code == 200, r.text


def test_demo_load_owner_only(world):
    c = world["c"]
    assert c.post("/demo/load", headers=world["mgr_a"]).status_code == 403


def test_tanda_push_roster_must_belong_to_venue(world):
    c = world["c"]
    r = c.post("/tanda/push-roster",
               params={"roster_id": world["ros_b"], "venue_id": world["ven_a"], "dry_run": True},
               headers=world["mgr_a"])
    assert r.status_code == 404, r.text
    r = c.post("/tanda/diff-roster",
               params={"roster_id": world["ros_b"], "venue_id": world["ven_a"]},
               headers=world["mgr_a"])
    assert r.status_code == 404, r.text


# ============================================================================
# legacy /api/staff swap board
# ============================================================================

def _staff_user(c, venue_id, email):
    """A staff login whose email is on an employee record in venue_id."""
    return _user(c, "staff", [venue_id], email=email)


def test_swap_board_and_decisions_scoped(world):
    c = world["c"]
    tag = world["tag"]
    staff_a = _staff_user(c, world["ven_a"], f"empA_{tag}@x.com")
    staff_b = _staff_user(c, world["ven_b"], f"empB_{tag}@x.com")
    # B offers a shift
    r = c.post("/api/staff/swap/offer", json={"shift_id": world["shift_b"]}, headers=staff_b)
    assert r.status_code == 200, r.text
    swap_b = r.json()["swap_id"]
    # A's board must not show it; A cannot accept/reject/request it
    board = c.get("/api/staff/swap-board", headers=staff_a).json()
    assert all(s.get("id") != swap_b for s in board["swaps"])
    assert c.post(f"/api/staff/swap/{swap_b}/accept", headers=staff_a).status_code == 404
    assert c.post(f"/api/staff/swap/{swap_b}/reject", headers=staff_a).status_code == 404
    r = c.post("/api/staff/swap/request",
               json={"my_shift_id": world["shift_a"], "swap_id": swap_b}, headers=staff_a)
    assert r.status_code == 404, r.text
    # B's board shows it; a random venue-B staff who is not a party can't decide it,
    # B's manager can.
    board_b = c.get("/api/staff/swap-board", headers=staff_b).json()
    assert any(s.get("id") == swap_b for s in board_b["swaps"])
    other_b = _user(c, "staff", [world["ven_b"]])
    assert c.post(f"/api/staff/swap/{swap_b}/reject", headers=other_b).status_code == 403
    assert c.post(f"/api/staff/swap/{swap_b}/reject", headers=world["mgr_b"]).status_code == 200


# ============================================================================
# account linking: no more auto-link by unverified email; join code instead
# ============================================================================

def test_self_registered_email_match_is_not_auto_linked(world):
    """Registering with a venue-B employee's email must NOT link/grant."""
    c = world["c"]
    email = f"empB_{world['tag']}@x.com"          # on venue-B's employee record
    _register(c, email)
    h = _login(c, email)
    prof = c.get("/api/me/profile", headers=h).json()
    assert prof["linked"] is False
    assert prof["link_pending"] is True            # we tell them to enter the code
    # and no venue was granted behind the scenes
    rec = get_db().get_user_by_email(email)
    assert world["ven_b"] not in (rec.get("venue_ids") or [])
    # legacy portal likewise finds no employee
    r = c.get("/api/staff/my-shifts", params={"week_start": date.today().isoformat()}, headers=h)
    assert r.status_code == 404


def test_join_code_links_and_grants(world):
    c = world["c"]
    email = f"empB_{world['tag']}@x.com"
    _register(c, email)
    h = _login(c, email)
    # manager of venue B reads the code; manager of A gets 404 for B's employee
    r = c.get(f"/api/employees/{world['emp_b']}/join-code", headers=world["mgr_b"])
    assert r.status_code == 200, r.text
    code = r.json()["join_code"]
    assert code == linking.join_code(world["emp_b"])
    assert c.get(f"/api/employees/{world['emp_b']}/join-code", headers=world["mgr_a"]).status_code == 404
    # staff cannot read codes
    assert c.get(f"/api/employees/{world['emp_b']}/join-code", headers=h).status_code == 403
    # wrong code → 400; right code (lower-case, no dash) → linked + granted
    assert c.post("/api/me/link", json={"code": "AAAA-AAAA"}, headers=h).status_code == 400
    r = c.post("/api/me/link", json={"code": code.lower().replace("-", "")}, headers=h)
    assert r.status_code == 200, r.text
    assert r.json()["employee_id"] == world["emp_b"]
    rec = get_db().get_user_by_email(email)
    assert world["ven_b"] in rec["venue_ids"]
    # re-login picks up the venue in the token → profile now linked
    h2 = _login(c, email)
    prof = c.get("/api/me/profile", headers=h2).json()
    assert prof["linked"] is True and prof["employee_id"] == world["emp_b"]


def test_join_code_refuses_other_persons_record(world):
    """A leaked code for a record with a DIFFERENT email must not link."""
    c = world["c"]
    _register(c, f"stranger_{world['tag']}@x.com")
    h = _login(c, f"stranger_{world['tag']}@x.com")
    code = linking.join_code(world["emp_b"])
    r = c.post("/api/me/link", json={"code": code}, headers=h)
    assert r.status_code == 400
    assert "different email" in _err(r)
    rec = get_db().get_user_by_email(f"stranger_{world['tag']}@x.com")
    assert world["ven_b"] not in (rec.get("venue_ids") or [])


def test_join_code_stamps_email_on_blank_record(world):
    c = world["c"]
    eid = f"empNoMail_{world['tag']}"
    _employee(c, world["owner"], eid, world["ven_a"], email="")
    _register(c, f"newbie_{world['tag']}@x.com")
    h = _login(c, f"newbie_{world['tag']}@x.com")
    r = c.post("/api/me/link", json={"code": linking.join_code(eid)}, headers=h)
    assert r.status_code == 200, r.text
    assert get_db().get_employee(eid).email == f"newbie_{world['tag']}@x.com"


def test_join_code_throttled(world, monkeypatch):
    c = world["c"]
    _register(c, f"brute_{world['tag']}@x.com")
    h = _login(c, f"brute_{world['tag']}@x.com")
    linking._attempts.clear()
    for _ in range(linking.MAX_CODE_ATTEMPTS):
        assert c.post("/api/me/link", json={"code": "ZZZZ-ZZZZ"}, headers=h).status_code == 400
    assert c.post("/api/me/link", json={"code": "ZZZZ-ZZZZ"}, headers=h).status_code == 429


def test_join_code_format_and_normalisation():
    code = linking.join_code("emp-1")
    assert len(code) == 9 and code[4] == "-"
    assert all(ch in linking._ALPHABET for ch in code.replace("-", ""))
    assert linking.normalise_code(" abcd efgh ") == "ABCD-EFGH"
    assert linking.normalise_code("oIl2-3456") == "0112-3456"
    assert linking.code_matches("emp-1", code.lower())
    assert linking.join_code("emp-1") != linking.join_code("emp-2")


# ============================================================================
# load_shift_in_scope must not fail open when the store's Shift has no venue
# (the Postgres shape): the venue is resolved via the roster join, and an
# unresolvable venue DENIES a non-owner.
# ============================================================================

def test_load_shift_in_scope_resolves_venue_via_roster_and_fails_closed():
    from rosteriq.middleware import tenant as T

    d = date.today()
    sh = Shift(id="pg-shift-1", employee_id="e", date=d, start_time=time(9, 0),
               end_time=time(17, 0), break_minutes=0, status=ShiftStatus.scheduled, role="bar")

    class PGLikeDB:
        def __init__(self, venue): self.venue = venue
        def get_shift(self, sid): return sh if sid == "pg-shift-1" else None
        def venue_id_for_shift(self, sid): return self.venue if sid == "pg-shift-1" else None
        def list_rosters(self): return []

    tok = T._tenant_context_var.set(T.TenantContext("u", ["A"], is_owner=False))
    try:
        # shift belongs to B → 404 for a venue-A user
        with pytest.raises(Exception) as ei:
            T.load_shift_in_scope(PGLikeDB("B"), "pg-shift-1")
        assert getattr(ei.value, "status_code", None) == 404
        # belongs to A → returned
        assert T.load_shift_in_scope(PGLikeDB("A"), "pg-shift-1").id == "pg-shift-1"
        # venue unresolvable → fail closed for a non-owner
        with pytest.raises(Exception) as ei:
            T.load_shift_in_scope(PGLikeDB(None), "pg-shift-1")
        assert getattr(ei.value, "status_code", None) == 404
    finally:
        T._tenant_context_var.reset(tok)
    # owner: unresolvable venue is still returned (owner passes everything)
    tok = T._tenant_context_var.set(T.TenantContext("o", [], is_owner=True))
    try:
        assert T.load_shift_in_scope(PGLikeDB(None), "pg-shift-1").id == "pg-shift-1"
    finally:
        T._tenant_context_var.reset(tok)


def test_require_role_inline_form_actually_checks(world):
    """require_role(user, [roles]) used to be a no-op decorator-factory call;
    changelog/roster-diff routes believed they were manager-only."""
    c = world["c"]
    staff = _user(c, "staff", [world["ven_a"]])
    r = c.get(f"/api/v1/rosters/{world['ros_a']}/changelog", headers=staff)
    assert r.status_code == 403, r.text
    r = c.get(f"/api/v1/rosters/{world['ros_a']}/history", headers=staff)
    assert r.status_code == 403, r.text
    # manager of the venue still fine
    assert c.get(f"/api/v1/rosters/{world['ros_a']}/changelog", headers=world["mgr_a"]).status_code == 200
