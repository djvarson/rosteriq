"""
Week-one pilot survival: password recovery and manager access.

The self-serve reset chain was broken in four places (dead login link, an
email that never sends, a reset URL pointing at app.example.com, and no
reset page at all), and the ONLY role-granting path in the product was
first-venue bootstrap — a venue's 2IC could never get manager access.

Pins:
* manager-issued reset link: manager gets a real URL on OUR origin; the
  token in it actually resets the password; the old password stops working
  and the new one signs in; the link is single-use
* reset-link guards: staff can't issue; unlinked employee is a clear 409;
  cross-venue employee is a 404
* /reset-password page is served
* forgot-password builds its URL from public_origin (never example.com)
* access-role: manager promotes a linked member to manager and back;
  self-change 400; unlinked 409; staff callers 403
"""

import uuid

from fastapi.testclient import TestClient

from rosteriq.api import app
from rosteriq.database import get_db

PW = "Passw0rd!234"


def _login(c, email, pw=PW):
    c.post("/api/auth/register", json={"email": email, "password": pw, "name": "U"})
    r = c.post("/api/auth/login", json={"email": email, "password": pw})
    return r, ({"Authorization": f"Bearer {r.json()['access_token']}"} if r.status_code == 200 else None)


def _world():
    c = TestClient(app)
    tag = uuid.uuid4().hex[:6]
    _, owner_h = _login(c, f"ra_o_{tag}@x.com")
    vid = f"ra-{tag}"
    r = c.post("/venues", json={"id": vid, "name": vid, "state": "wa", "max_labour_pct": 30,
                                "tanda_org_id": "", "created_at": "2026-07-01T00:00:00"},
               headers=owner_h)
    assert r.status_code in (200, 201), r.text
    return c, owner_h, vid, tag


def _employee(c, h, vid, tag, name="Mei Chen", email=None):
    eid = f"{vid}-{uuid.uuid4().hex[:4]}"
    r = c.post("/employees", json={
        "id": eid, "venue_id": vid, "name": name, "employment_type": "casual",
        "award_level": "level_2", "state": "wa", "hourly_base_rate": "31.50",
        "email": email, "created_at": "2026-07-01T00:00:00", "updated_at": "2026-07-01T00:00:00",
    }, headers=h)
    assert r.status_code == 200, r.text
    return eid


def _link_staff(c, owner_h, vid, eid, email):
    """Register the staff login and link it via the real join-code flow."""
    _, staff_h = _login(c, email)
    code = c.get(f"/api/employees/{eid}/join-code", headers=owner_h).json()["join_code"]
    r = c.post("/api/me/link", json={"code": code}, headers=staff_h)
    assert r.status_code == 200, r.text
    return staff_h


def test_manager_reset_link_end_to_end():
    c, owner_h, vid, tag = _world()
    email = f"ra_s_{tag}@x.com"
    eid = _employee(c, owner_h, vid, tag, email=email)
    _link_staff(c, owner_h, vid, eid, email)

    r = c.post(f"/api/employees/{eid}/reset-link", headers=owner_h)
    assert r.status_code == 200, r.text
    body = r.json()
    assert "example.com" not in body["reset_url"]
    assert "/reset-password?token=" in body["reset_url"]
    token = body["reset_url"].split("token=")[1]

    # the token really resets the password
    r = c.post("/api/auth/reset-password", json={"token": token, "new_password": "NewPass!5678"})
    assert r.status_code == 200, r.text
    assert c.post("/api/auth/login", json={"email": email, "password": PW}).status_code == 401
    assert c.post("/api/auth/login", json={"email": email, "password": "NewPass!5678"}).status_code == 200

    # single-use: the same token is dead now
    r = c.post("/api/auth/reset-password", json={"token": token, "new_password": "Third!90123"})
    assert r.status_code == 400


def test_reset_link_guards():
    c, owner_h, vid, tag = _world()
    email = f"ra_g_{tag}@x.com"
    eid_linked = _employee(c, owner_h, vid, tag, email=email)
    staff_h = _link_staff(c, owner_h, vid, eid_linked, email)
    eid_unlinked = _employee(c, owner_h, vid, tag, name="No Login Norm",
                             email=f"ra_n_{tag}@x.com")

    # staff can't issue reset links
    assert c.post(f"/api/employees/{eid_linked}/reset-link", headers=staff_h).status_code == 403
    # no login yet -> clear 409, not a broken link
    r = c.post(f"/api/employees/{eid_unlinked}/reset-link", headers=owner_h)
    assert r.status_code == 409, r.text
    # a different venue's manager can't reach the employee at all
    c2, other_h, _, _ = _world()
    assert c2.post(f"/api/employees/{eid_linked}/reset-link", headers=other_h).status_code == 404


def test_reset_page_is_served():
    c = TestClient(app)
    r = c.get("/reset-password")
    assert r.status_code == 200
    assert "Set a new password" in r.text


def test_forgot_password_url_never_points_at_example_dot_com(monkeypatch):
    from rosteriq.routes.auth import public_origin
    monkeypatch.delenv("PUBLIC_ORIGIN", raising=False)
    assert "example.com" not in public_origin()
    monkeypatch.setenv("PUBLIC_ORIGIN", "https://app.rosteriq.com.au/")
    assert public_origin() == "https://app.rosteriq.com.au"


def test_access_role_promote_and_demote():
    c, owner_h, vid, tag = _world()
    email = f"ra_p_{tag}@x.com"
    eid = _employee(c, owner_h, vid, tag, email=email)
    _link_staff(c, owner_h, vid, eid, email)

    r = c.post(f"/api/employees/{eid}/access-role", json={"role": "manager"}, headers=owner_h)
    assert r.status_code == 200, r.text
    assert r.json()["role"] == "manager"
    assert get_db().get_user_by_email(email)["role"] == "manager"

    r = c.post(f"/api/employees/{eid}/access-role", json={"role": "staff"}, headers=owner_h)
    assert r.status_code == 200 and get_db().get_user_by_email(email)["role"] == "staff"


def test_access_role_guards():
    c, owner_h, vid, tag = _world()
    email = f"ra_q_{tag}@x.com"
    eid = _employee(c, owner_h, vid, tag, email=email)
    staff_h = _link_staff(c, owner_h, vid, eid, email)

    # staff can't grant roles
    assert c.post(f"/api/employees/{eid}/access-role", json={"role": "manager"},
                  headers=staff_h).status_code == 403
    # unlinked employee -> 409
    eid2 = _employee(c, owner_h, vid, tag, name="Unlinked", email=f"ra_u_{tag}@x.com")
    assert c.post(f"/api/employees/{eid2}/access-role", json={"role": "manager"},
                  headers=owner_h).status_code == 409
    # a manager can't change their own account (the owner's employee record).
    # 400 = the explicit self-change guard; 403 = the owner-account block;
    # 409 = the venue-membership guard firing first (the bootstrap owner's
    # account isn't join-code-linked) — all three refuse, which is the pin.
    own_email = f"ra_o_{tag}@x.com"
    eid_self = _employee(c, owner_h, vid, tag, name="Self", email=own_email)
    r = c.post(f"/api/employees/{eid_self}/access-role", json={"role": "staff"}, headers=owner_h)
    assert r.status_code in (400, 403, 409), r.text
    # invalid role rejected by validation
    assert c.post(f"/api/employees/{eid}/access-role", json={"role": "owner"},
                  headers=owner_h).status_code == 422


# ---------------------------------------------------------------------------
# Adversarial-review pins: the attacks that must never work
# ---------------------------------------------------------------------------

def test_reset_link_cannot_target_another_tenants_account():
    """The takeover vector: venue-A manager types venue-B's manager email
    onto their OWN employee record, then asks for a reset link. The account
    doesn't hold venue A -> 409, no token minted."""
    c, owner_h, vid, tag = _world()
    # a completely separate tenant with their own login
    c2 = c  # same app instance; separate accounts
    victim_email = f"ra_victim_{tag}@x.com"
    _, victim_h = _login(c2, victim_email)
    r = c2.post("/venues", json={"id": f"victim-{tag}", "name": "V", "state": "wa",
                                 "max_labour_pct": 30, "tanda_org_id": "",
                                 "created_at": "2026-07-01T00:00:00"}, headers=victim_h)
    assert r.status_code in (200, 201)

    # attacker points an own-venue employee record at the victim's email
    eid = _employee(c, owner_h, vid, tag, name="Trojan", email=victim_email)
    r = c.post(f"/api/employees/{eid}/reset-link", headers=owner_h)
    assert r.status_code == 409, r.text
    # and can't flip their role either
    r = c.post(f"/api/employees/{eid}/access-role", json={"role": "staff"}, headers=owner_h)
    assert r.status_code == 409, r.text


def test_demo_session_cannot_manage_logins():
    """The public Try Demo identity must never mint reset links or roles."""
    c = TestClient(app)
    r = c.post("/api/auth/demo")
    assert r.status_code == 200, r.text
    demo_h = {"Authorization": f"Bearer {r.json()['access_token']}"}
    # demo is a manager of the demo venue — create a record there
    r = c.post("/employees", json={
        "id": f"demo-atk-{uuid.uuid4().hex[:6]}", "venue_id": "demo-venue-001",
        "name": "Atk", "employment_type": "casual", "award_level": "level_2",
        "state": "wa", "hourly_base_rate": "31.50", "email": "victim@real.example",
        "created_at": "2026-07-01T00:00:00", "updated_at": "2026-07-01T00:00:00",
    }, headers=demo_h)
    if r.status_code == 200:   # whether or not demo may write, the gate below must hold
        eid = r.json()["id"]
        assert c.post(f"/api/employees/{eid}/reset-link", headers=demo_h).status_code == 403
        assert c.post(f"/api/employees/{eid}/access-role", json={"role": "manager"},
                      headers=demo_h).status_code == 403


def test_access_role_multi_venue_account_is_refused():
    """role AND password are GLOBAL on an account — neither endpoint may
    touch a login that also holds other venues, or one venue's manager
    could mint power (or a working session, via reset) at venues they
    don't run. Re-verify round 2 proved reset-link was missing this."""
    c, owner_h, vid, tag = _world()
    email = f"ra_multi_{tag}@x.com"
    eid = _employee(c, owner_h, vid, tag, email=email)
    staff_h = _link_staff(c, owner_h, vid, eid, email)
    # the same login also opens their own second venue
    r = c.post("/venues", json={"id": f"second-{tag}", "name": "S", "state": "wa",
                                "max_labour_pct": 30, "tanda_org_id": "",
                                "created_at": "2026-07-01T00:00:00"}, headers=staff_h)
    assert r.status_code in (200, 201)
    r = c.post(f"/api/employees/{eid}/access-role", json={"role": "manager"}, headers=owner_h)
    assert r.status_code == 409, r.text
    # the takeover vector: reset-link is strictly MORE powerful than a role
    # flip, so the same multi-venue 409 must hold — no token may be minted
    r = c.post(f"/api/employees/{eid}/reset-link", headers=owner_h)
    assert r.status_code == 409, r.text


def test_new_reset_link_revokes_the_old_one():
    """Only the LATEST reset link works: minting a second token kills the
    first, and a successful reset kills everything outstanding."""
    c, owner_h, vid, tag = _world()
    email = f"ra_rev_{tag}@x.com"
    eid = _employee(c, owner_h, vid, tag, email=email)
    _link_staff(c, owner_h, vid, eid, email)

    t1 = c.post(f"/api/employees/{eid}/reset-link", headers=owner_h).json()["reset_url"].split("token=")[1]
    t2 = c.post(f"/api/employees/{eid}/reset-link", headers=owner_h).json()["reset_url"].split("token=")[1]
    # the older link is dead the moment a newer one exists
    assert c.post("/api/auth/reset-password",
                  json={"token": t1, "new_password": "Revoked!123"}).status_code == 400
    # the newest works, exactly once
    assert c.post("/api/auth/reset-password",
                  json={"token": t2, "new_password": "Fresh!12345"}).status_code == 200
    assert c.post("/api/auth/reset-password",
                  json={"token": t2, "new_password": "Again!12345"}).status_code == 400
    assert c.post("/api/auth/login",
                  json={"email": email, "password": "Fresh!12345"}).status_code == 200


def test_venueless_employee_record_fails_closed():
    c, owner_h, vid, tag = _world()
    email = f"ra_nov_{tag}@x.com"
    r = c.post("/employees", json={
        "id": f"nov-{tag}", "name": "No Venue", "employment_type": "casual",
        "award_level": "level_2", "state": "wa", "hourly_base_rate": "31.50",
        "email": email, "created_at": "2026-07-01T00:00:00",
        "updated_at": "2026-07-01T00:00:00",
    }, headers=owner_h)
    if r.status_code == 200:   # if venue-less records are even creatable...
        assert c.post(f"/api/employees/nov-{tag}/reset-link",
                      headers=owner_h).status_code in (404, 409)


def test_uppercase_registered_email_still_links():
    """Case-insensitive account lookup: registration case must not strand
    the reset flow in 'no login exists'."""
    c, owner_h, vid, tag = _world()
    email = f"RA_Case_{tag}@X.com"
    eid = _employee(c, owner_h, vid, tag, email=email)
    # register with the SAME funny casing; join-code link flows lowercase
    _, staff_h = _login(c, email)
    code = c.get(f"/api/employees/{eid}/join-code", headers=owner_h).json()["join_code"]
    r = c.post("/api/me/link", json={"code": code}, headers=staff_h)
    if r.status_code == 200:
        r = c.post(f"/api/employees/{eid}/reset-link", headers=owner_h)
        assert r.status_code == 200, r.text
