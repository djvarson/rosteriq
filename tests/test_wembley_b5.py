"""The Wembley's time clock and timesheets.

The bar iPad runs on a time clock DEVICE token a manager set up once (never on
a person's login), staff punch with a PIN that locks after five wrong tries,
breaks are recorded on the kiosk (or the rostered meal break is deducted and
flagged), a forgotten 1am clock-out is caught the next day instead of becoming
a 23-hour shift, split shifts are matched to the right half, and a fortnight
of clean timesheets is approved in one go.

Time-dependent tests freeze the time clock's view of "now" at a venue-local
Perth wall time, so they behave the same whatever hour the suite runs.
"""

import hashlib
import inspect
import json
import re
import uuid
from contextlib import contextmanager
from datetime import date, datetime, time as dtime, timedelta, timezone
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest
from fastapi.testclient import TestClient

from rosteriq.api import app
from rosteriq.database import MemoryStore, PostgresStore, get_db
from rosteriq.models import Roster, Shift, ShiftStatus
from rosteriq.routes import timeclock as timeclock_routes
from rosteriq.services import kiosk
from rosteriq.tests.kiosk_support import kiosk_headers

PW = "Passw0rd!234"
PERTH = ZoneInfo("Australia/Perth")
DAY = date(2026, 10, 14)            # a Wednesday
NEXT = DAY + timedelta(days=1)


def _login(c, email):
    c.post("/api/auth/register", json={"email": email, "password": PW, "name": "U"})
    r = c.post("/api/auth/login", json={"email": email, "password": PW})
    return {"Authorization": f"Bearer {r.json()['access_token']}"}


def _venue(c, h, vid, name):
    r = c.post("/venues", json={"id": vid, "name": name, "state": "wa", "max_labour_pct": 30,
                                "tanda_org_id": "", "timezone": "Australia/Perth",
                                "created_at": "2026-07-01T00:00:00"}, headers=h)
    assert r.status_code in (200, 201), r.text


def _employee(c, h, vid, emp_id, name, email=None):
    body = {"id": emp_id, "venue_id": vid, "name": name, "employment_type": "casual",
            "award_level": "level_2", "state": "wa", "hourly_base_rate": "30.00",
            "skills": ["bar"], "created_at": "2026-07-01T00:00:00",
            "updated_at": "2026-07-01T00:00:00"}
    if email:
        body["email"] = email
    r = c.post("/employees", json=body, headers=h)
    assert r.status_code == 200, r.text


def _world():
    """The Wembley: a manager (who is also on the staff list), a linked casual
    and two more casuals."""
    c = TestClient(app)
    _login(c, f"b5boot{uuid.uuid4().hex[:6]}@x.com")          # platform owner, unused
    mgr_email = f"b5m{uuid.uuid4().hex[:8]}@x.com"
    h = _login(c, mgr_email)
    vid = f"wem-{uuid.uuid4().hex[:6]}"
    _venue(c, h, vid, "The Wembley Hotel")
    mgr = _login(c, mgr_email)
    staff_email = f"b5s{uuid.uuid4().hex[:8]}@x.com"
    _employee(c, mgr, vid, f"{vid}-mgr", "Dana Duty", email=mgr_email)
    _employee(c, mgr, vid, f"{vid}-e0", "Casey Casual", email=staff_email)
    _employee(c, mgr, vid, f"{vid}-e1", "Riley Restaurant")
    _employee(c, mgr, vid, f"{vid}-e2", "Sam Sportsbar")
    staff = _login(c, staff_email)
    db = get_db()
    rec = db.get_user_by_email(staff_email)
    rec["venue_ids"], rec["role"] = [vid], "staff"
    db.save_user(rec)
    staff = _login(c, staff_email)
    return SimpleNamespace(c=c, mgr=mgr, staff=staff, vid=vid, mgr_email=mgr_email,
                           staff_email=staff_email, me=f"{vid}-e0", mgr_emp=f"{vid}-mgr",
                           e1=f"{vid}-e1", e2=f"{vid}-e2")


def _msg(r):
    body = r.json()
    return (body.get("error") or {}).get("message") or body.get("detail") or ""


def _punch(c, k, vid, emp, direction="in", **extra):
    return c.post(f"/api/clock/{direction}", json={"venue_id": vid, "employee_id": emp, **extra},
                  headers=k)


@pytest.fixture
def clock(monkeypatch):
    """Set the time clock's 'now' to a Perth wall time."""
    def set_local(local_dt):
        utc = local_dt.replace(tzinfo=PERTH).astimezone(timezone.utc).replace(tzinfo=None)

        class _Frozen(datetime):
            @classmethod
            def utcnow(cls):
                return utc

        monkeypatch.setattr(timeclock_routes, "datetime", _Frozen)
        monkeypatch.setattr(timeclock_routes, "venue_today", lambda vid, db=None: local_dt.date())
        return utc
    return set_local


def _at(day, hh, mm=0):
    return datetime.combine(day, dtime(hh, mm))


def _roster(vid, shifts):
    """Publish a week's roster holding (shift_id, employee_id, day, start, end, break)."""
    db = get_db()
    week = DAY - timedelta(days=DAY.weekday())
    rid = f"r-{uuid.uuid4().hex[:6]}"
    db.save_roster(Roster(
        id=rid, venue_id=vid, week_start=week, week_end=week + timedelta(days=6),
        shifts=[Shift(id=sid, employee_id=emp, date=day, start_time=s, end_time=e,
                      break_minutes=brk, status=ShiftStatus.scheduled, role="bar")
                for sid, emp, day, s, e, brk in shifts],
        total_cost=None, created_at=datetime(2026, 10, 1)))
    db.update_roster_state(rid, "published", "test", "test")


# ---------------------------------------------------------------------------
# [27] The time clock runs on a device token scoped to one venue's clock
# ---------------------------------------------------------------------------

def test_a_kiosk_token_reaches_only_its_venues_clock_routes():
    w = _world()
    c = w.c
    k = kiosk_headers(c, w.mgr, w.vid)

    assert c.get(f"/api/clock/board?venue_id={w.vid}", headers=k).status_code == 200
    info = c.get("/api/clock/kiosk", headers=k).json()
    assert info["venue_id"] == w.vid and info["venue_name"] == "The Wembley Hotel"
    assert _punch(c, k, w.vid, w.e1).status_code == 200

    refused = [
        ("GET", f"/employees?venue_id={w.vid}", None),
        ("GET", "/venues", None),
        ("GET", f"/rosters?venue_id={w.vid}", None),
        ("GET", "/api/me/profile", None),
        ("GET", f"/api/clock/timesheets?venue_id={w.vid}&status=open", None),
        ("POST", "/api/clock/timesheets/approve", {"venue_id": w.vid, "ids": ["x"]}),
        ("POST", "/api/clock/pin", {"venue_id": w.vid, "employee_id": w.e1, "pin": "1357"}),
        ("POST", "/api/kiosk/devices", {"venue_id": w.vid}),
        ("GET", f"/api/kiosk/devices?venue_id={w.vid}", None),
        ("POST", "/api/kiosk/pins/generate", {"venue_id": w.vid}),
    ]
    for method, path, body in refused:
        r = c.request(method, path, headers=k, json=body)
        assert r.status_code == 403, (method, path, r.status_code, r.text)
    # Not a person's login anywhere, including the auth routes
    assert c.get("/api/auth/me", headers=k).status_code == 401

    # Apothecary: the same manager's other venue is out of this device's reach
    _venue(c, w.mgr, "apo-" + w.vid, "Apothecary")
    assert c.get(f"/api/clock/board?venue_id=apo-{w.vid}", headers=k).status_code == 403


def test_a_device_token_is_stored_hashed_listed_and_revocable():
    w = _world()
    c = w.c
    r = c.post("/api/kiosk/devices", json={"venue_id": w.vid, "name": "Front bar iPad"},
               headers=w.mgr)
    assert r.status_code == 200 and r.headers["cache-control"] == "no-store"
    token, device = r.json()["token"], r.json()["device"]
    row = get_db().get_kiosk_device(device["id"])
    assert row["token_hash"] == hashlib.sha256(token.encode()).hexdigest()
    assert token not in json.dumps(row, default=str)
    k = {"Authorization": f"Bearer {token}"}

    listed = c.get(f"/api/kiosk/devices?venue_id={w.vid}", headers=w.mgr).json()["devices"]
    assert [(d["name"], d["active"]) for d in listed] == [("Front bar iPad", True)]
    assert "token" not in listed[0] and "token_hash" not in listed[0]

    # Staff can neither set a device up nor see the list
    assert c.post("/api/kiosk/devices", json={"venue_id": w.vid}, headers=w.staff).status_code == 403
    assert c.get(f"/api/kiosk/devices?venue_id={w.vid}", headers=w.staff).status_code == 403

    rv = c.post(f"/api/kiosk/devices/{device['id']}/revoke", json={"venue_id": w.vid}, headers=w.mgr)
    assert rv.status_code == 200
    gone = c.get(f"/api/clock/board?venue_id={w.vid}", headers=k)
    assert gone.status_code == 401 and "switched off" in _msg(gone)
    listed = c.get(f"/api/kiosk/devices?venue_id={w.vid}", headers=w.mgr).json()["devices"]
    assert listed[0]["active"] is False


def test_a_kiosk_token_gets_its_own_rate_bucket_and_no_live_feed():
    from rosteriq.middleware.rate_limiter import RateLimiterMiddleware
    from rosteriq.websocket_hub import _may_watch_venue
    w = _world()
    r = w.c.post("/api/kiosk/devices", json={"venue_id": w.vid}, headers=w.mgr).json()
    assert RateLimiterMiddleware._token_user_id(r["token"]) == f"kiosk:{r['device']['id']}"
    assert _may_watch_venue(r["token"], w.vid) == (False, None)


def test_staff_cannot_punch_anyone_or_read_the_clock():
    w = _world()
    c = w.c
    assert _punch(c, w.staff, w.vid, w.e1).status_code == 403          # a colleague
    assert _punch(c, w.staff, w.vid, w.mgr_emp).status_code == 403     # the duty manager
    assert _punch(c, w.staff, w.vid, w.me).status_code == 403          # themselves: off by default
    assert c.get(f"/api/clock/board?venue_id={w.vid}", headers=w.staff).status_code == 403
    assert c.get(f"/api/clock/timesheets?venue_id={w.vid}&status=open",
                 headers=w.staff).status_code == 403
    assert c.post("/api/clock/pin/check", json={"venue_id": w.vid, "employee_id": w.e1, "pin": "1234"},
                  headers=w.staff).status_code == 403
    # A manager's login doesn't punch other people either: that's the device's job
    assert _punch(c, w.mgr, w.vid, w.e1).status_code == 403


def test_staff_clock_themselves_in_from_my_only_when_the_venue_allows_it():
    w = _world()
    c = w.c
    assert c.get("/api/me/clock", headers=w.staff).json()["self_clock"] is False
    r = c.put("/api/kiosk/settings", json={"venue_id": w.vid, "allow_self_clock": True}, headers=w.mgr)
    assert r.status_code == 200 and r.json()["allow_self_clock"] is True
    assert c.put("/api/kiosk/settings", json={"venue_id": w.vid, "allow_self_clock": False},
                 headers=w.staff).status_code == 403

    r = _punch(c, w.staff, w.vid, w.me)
    assert r.status_code == 200, r.text
    assert get_db().get_timesheet(r.json()["timesheet_id"])["source"] == "self"
    assert c.get("/api/me/clock", headers=w.staff).json()["state"] == "on"
    assert _punch(c, w.staff, w.vid, w.e1).status_code == 403          # still never a colleague
    assert _punch(c, w.staff, w.vid, w.me, "out").status_code == 200


def test_exit_kiosk_needs_a_managers_pin():
    w = _world()
    c = w.c
    for emp, pin in ((w.mgr_emp, "8642"), (w.e1, "1357")):
        assert c.post("/api/clock/pin", json={"venue_id": w.vid, "employee_id": emp, "pin": pin},
                      headers=w.mgr).status_code == 200
    k = kiosk_headers(c, w.mgr, w.vid)

    assert c.post("/api/clock/kiosk/exit", json={"pin": "1357"}, headers=k).status_code == 403  # staff PIN
    assert c.post("/api/clock/kiosk/exit", json={"pin": "0000"}, headers=k).status_code == 403
    ok = c.post("/api/clock/kiosk/exit", json={"pin": "8642"}, headers=k)
    assert ok.status_code == 200 and ok.json()["status"] == "revoked"
    assert c.get(f"/api/clock/board?venue_id={w.vid}", headers=k).status_code == 401
    assert c.post("/api/clock/kiosk/exit", json={"pin": "8642"}, headers=w.mgr).status_code == 403

    k2 = kiosk_headers(c, w.mgr, w.vid)
    for _ in range(kiosk.MAX_PIN_FAILURES - 1):
        assert c.post("/api/clock/kiosk/exit", json={"pin": "1111"}, headers=k2).status_code == 403
    assert c.post("/api/clock/kiosk/exit", json={"pin": "1111"}, headers=k2).status_code == 429
    locked = c.post("/api/clock/kiosk/exit", json={"pin": "8642"}, headers=k2)
    assert locked.status_code == 429 and "sign in as a manager" in _msg(locked)


# ---------------------------------------------------------------------------
# [58] PINs: set by a manager, generated in bulk, or set by staff on /my
# ---------------------------------------------------------------------------

def test_a_kiosk_pin_locks_for_15_minutes_after_5_wrong_tries():
    w = _world()
    c = w.c
    assert c.post("/api/clock/pin", json={"venue_id": w.vid, "employee_id": w.e1, "pin": "2468"},
                  headers=w.mgr).status_code == 200
    stored = get_db().get_timeclock_pin(w.vid, w.e1)
    assert stored.startswith("pbkdf2$") and "2468" not in stored
    k = kiosk_headers(c, w.mgr, w.vid)
    check = lambda pin: c.post("/api/clock/pin/check",  # noqa: E731
                               json={"venue_id": w.vid, "employee_id": w.e1, "pin": pin}, headers=k)

    first = check("1111")
    assert first.status_code == 403 and "4 tries left" in _msg(first)
    for _ in range(3):
        check("1111")
    fifth = check("1111")
    assert fifth.status_code == 429 and "locked for 15 minutes" in _msg(fifth)
    assert check("2468").status_code == 429                    # even the right PIN, while locked
    assert _punch(c, k, w.vid, w.e1, pin="2468").status_code == 429

    get_db()._timeclock_pins[f"{w.vid}:{w.e1}"]["locked_until"] = datetime.utcnow() - timedelta(minutes=1)
    ok = check("2468")
    assert ok.status_code == 200 and ok.json()["pin_set"] is True
    assert ok.json()["card"]["name"] == "Riley Restaurant"

    for _ in range(5):
        check("9999")
    assert check("2468").status_code == 429
    c.post("/api/clock/pin", json={"venue_id": w.vid, "employee_id": w.e1, "pin": "2468"}, headers=w.mgr)
    assert check("2468").status_code == 200                    # a manager's reset lifts the lock


def test_generate_pins_for_everyone_without_one_shown_once():
    w = _world()
    c = w.c
    c.post("/api/clock/pin", json={"venue_id": w.vid, "employee_id": w.e1, "pin": "2468"}, headers=w.mgr)
    assert c.post("/api/kiosk/pins/generate", json={"venue_id": w.vid},
                  headers=w.staff).status_code == 403
    r = c.post("/api/kiosk/pins/generate", json={"venue_id": w.vid}, headers=w.mgr)
    assert r.status_code == 200 and r.headers["cache-control"] == "no-store"
    pins = r.json()["pins"]
    assert {p["employee_id"] for p in pins} == {w.mgr_emp, w.me, w.e2}
    for p in pins:
        assert re.fullmatch(r"\d{4}", p["pin"])
        stored = get_db().get_timeclock_pin(w.vid, p["employee_id"])
        assert p["pin"] not in stored
        assert kiosk.pin_matches(stored, w.vid, p["employee_id"], p["pin"])
    assert c.post("/api/kiosk/pins/generate", json={"venue_id": w.vid},
                  headers=w.mgr).json()["count"] == 0


def test_staff_set_their_own_kiosk_pin_on_my():
    w = _world()
    c = w.c
    assert c.get("/api/me/clock", headers=w.staff).json()["has_pin"] is False
    assert c.post("/api/me/clock-pin", json={"pin": "12ab"}, headers=w.staff).status_code == 422
    r = c.post("/api/me/clock-pin", json={"pin": "97531"}, headers=w.staff)
    assert r.status_code == 200, r.text
    assert c.get("/api/me/clock", headers=w.staff).json()["has_pin"] is True

    k = kiosk_headers(c, w.mgr, w.vid)
    ok = c.post("/api/clock/pin/check", json={"venue_id": w.vid, "employee_id": w.me, "pin": "97531"},
                headers=k)
    assert ok.status_code == 200 and ok.json()["pin_set"] is True
    assert _punch(c, k, w.vid, w.me, pin="97531").json()["pin_verified"] is True

    stranger = _login(c, f"b5x{uuid.uuid4().hex[:6]}@x.com")
    assert c.post("/api/me/clock-pin", json={"pin": "4444"}, headers=stranger).status_code == 409


# ---------------------------------------------------------------------------
# [11] Breaks
# ---------------------------------------------------------------------------

def test_break_buttons_record_the_actual_unpaid_break(clock):
    w = _world()
    c = w.c
    k = kiosk_headers(c, w.mgr, w.vid)
    clock(_at(DAY, 11))
    assert _punch(c, k, w.vid, w.e1).status_code == 200
    clock(_at(DAY, 14))
    assert _punch(c, k, w.vid, w.e1, "break/start").json()["status"] == "on_break"
    assert _punch(c, k, w.vid, w.e1, "break/start").status_code == 409
    board = c.get(f"/api/clock/board?venue_id={w.vid}", headers=k).json()
    assert {s["employee_id"]: s["state"] for s in board["staff"]}[w.e1] == "break"
    clock(_at(DAY, 14, 25))
    end = _punch(c, k, w.vid, w.e1, "break/end").json()
    assert end["break_minutes"] == 25 and end["total_break_minutes"] == 25
    clock(_at(DAY, 17))
    out = _punch(c, k, w.vid, w.e1, "out").json()
    assert out["break_minutes"] == 25 and out["break_source"] == "recorded"
    assert out["worked_minutes"] == 360 - 25
    assert not {"auto_break", "no_break"} & set(out["flags"])


def test_the_rostered_meal_break_is_deducted_and_flagged_when_none_was_recorded(clock):
    w = _world()
    c = w.c
    _roster(w.vid, [("sh-lunch", w.e1, DAY, dtime(11), dtime(17), 30),
                    ("sh-next", w.e1, NEXT, dtime(11), dtime(17), 30)])
    c.post("/api/clock/pin", json={"venue_id": w.vid, "employee_id": w.e1, "pin": "2468"}, headers=w.mgr)
    k = kiosk_headers(c, w.mgr, w.vid)

    clock(_at(DAY, 10, 58))
    _punch(c, k, w.vid, w.e1, pin="2468")
    clock(_at(DAY, 17, 2))
    out = _punch(c, k, w.vid, w.e1, "out", pin="2468").json()
    assert out["break_minutes"] == 30 and out["break_source"] == "auto"
    assert out["worked_minutes"] == 364 - 30 and out["variance_minutes"] == 4
    assert "auto_break" in out["flags"]

    rows = c.get(f"/api/clock/timesheets?venue_id={w.vid}&start_date={DAY}&end_date={DAY}",
                 headers=w.mgr).json()["timesheets"]
    assert rows[0]["break_source"] == "auto"
    assert "meal break auto-deducted (rostered)" in rows[0]["flag_labels"]
    assert rows[0]["clean"] is True                         # within 15 min, PIN, break auto

    # Venue setting off (advisor to confirm): nothing deducted, flagged for review
    c.put("/api/kiosk/settings", json={"venue_id": w.vid, "auto_meal_break": False}, headers=w.mgr)
    clock(_at(NEXT, 11))
    _punch(c, k, w.vid, w.e1, pin="2468")
    clock(_at(NEXT, 17))
    out = _punch(c, k, w.vid, w.e1, "out", pin="2468").json()
    assert out["break_minutes"] == 0 and "no_break" in out["flags"]
    review = c.get(f"/api/clock/timesheets?venue_id={w.vid}&start_date={NEXT}&end_date={NEXT}&status=review",
                   headers=w.mgr).json()["timesheets"]
    assert len(review) == 1 and "no break recorded" in review[0]["review_reasons"]


# ---------------------------------------------------------------------------
# [59] Forgotten clock-outs
# ---------------------------------------------------------------------------

def test_a_forgotten_clock_out_asks_for_the_finish_time_next_day(clock):
    w = _world()
    c = w.c
    k = kiosk_headers(c, w.mgr, w.vid)
    clock(_at(DAY, 17))
    _punch(c, k, w.vid, w.e2)
    clock(_at(NEXT, 15))                                    # 22 hours later

    card = [s for s in c.get(f"/api/clock/board?venue_id={w.vid}", headers=k).json()["staff"]
            if s["employee_id"] == w.e2][0]
    assert card["state"] == "on" and card["stale"] is True
    again = _punch(c, k, w.vid, w.e2)
    assert again.status_code == 409
    assert "still clocked in from" in _msg(again) and "finish time" in _msg(again)

    future = _punch(c, k, w.vid, w.e2, "out", finished_at="16:00")   # tomorrow 16:00 hasn't happened
    assert future.status_code == 422
    out = _punch(c, k, w.vid, w.e2, "out", finished_at="23:30")
    assert out.status_code == 200, out.text
    body = out.json()
    assert body["worked_minutes"] == 390                    # unrostered: nothing to deduct
    assert {"finish_entered_later", "no_break"} <= set(body["flags"]) and body["note"]
    finished = get_db().get_timesheet(body["timesheet_id"])["clock_out"]
    assert finished == _at(DAY, 23, 30) - timedelta(hours=8)
    assert _punch(c, k, w.vid, w.e2).status_code == 200


def test_a_stale_rostered_punch_is_auto_closed_at_the_rostered_end(clock):
    w = _world()
    c = w.c
    _roster(w.vid, [("sh-close", w.e1, DAY, dtime(17), dtime(23), 30)])
    k = kiosk_headers(c, w.mgr, w.vid)
    clock(_at(DAY, 16, 58))
    ts_id = _punch(c, k, w.vid, w.e1).json()["timesheet_id"]
    clock(_at(NEXT, 9))                                     # 16 hours on

    board = c.get(f"/api/clock/board?venue_id={w.vid}", headers=k).json()
    assert {s["employee_id"]: s["state"] for s in board["staff"]}[w.e1] == "off"   # no longer ON SHIFT
    ts = get_db().get_timesheet(ts_id)
    assert ts["clock_out"] == _at(DAY, 23) - timedelta(hours=8)
    assert "auto_closed" in ts["flags"] and ts["status"] == "closed"
    events = get_db().list_events(venue_id=w.vid, action_prefix="timesheet.auto_close")
    assert [e["resource_id"] for e in events] == [ts_id]
    row = c.get(f"/api/clock/timesheets?venue_id={w.vid}&start_date={DAY}&end_date={DAY}",
                headers=w.mgr).json()["timesheets"][0]
    assert "auto-closed at rostered end — check" in row["flag_labels"] and row["clean"] is False


def test_a_clock_out_over_12_hours_warns_and_is_flagged(clock):
    w = _world()
    c = w.c
    k = kiosk_headers(c, w.mgr, w.vid)
    clock(_at(DAY, 8))
    _punch(c, k, w.vid, w.e2)
    clock(_at(DAY, 21, 30))
    out = _punch(c, k, w.vid, w.e2, "out").json()
    assert "13h 30m" in out["warning"] and "long_shift" in out["flags"]


def test_open_punches_are_listed_whatever_their_date_and_edited_with_a_note():
    w = _world()
    c = w.c
    db = get_db()
    old = datetime.utcnow() - timedelta(days=10)
    db.save_timesheet({"id": "ts-old-open", "venue_id": w.vid, "employee_id": w.e2,
                       "work_date": old.date(), "clock_in": old, "clock_out": None,
                       "break_minutes": 0, "status": "open", "pin_verified": False,
                       "created_at": old})
    week = date.today() - timedelta(days=date.today().weekday())
    in_range = c.get(f"/api/clock/timesheets?venue_id={w.vid}&start_date={week}&end_date={date.today()}",
                     headers=w.mgr).json()
    assert in_range["count"] == 0
    open_rows = c.get(f"/api/clock/timesheets?venue_id={w.vid}&status=open", headers=w.mgr).json()
    assert [r["id"] for r in open_rows["timesheets"]] == ["ts-old-open"]
    assert open_rows["timesheets"][0]["open_minutes"] >= 10 * 24 * 60

    fix = {"venue_id": w.vid, "approve": True, "clock_out": (old + timedelta(hours=6)).isoformat()}
    assert c.post("/api/clock/timesheets/ts-old-open/review", json=fix, headers=w.mgr).status_code == 422
    assert db.get_timesheet("ts-old-open")["clock_out"] is None       # refused edit leaves it alone
    done = c.post("/api/clock/timesheets/ts-old-open/review",
                  json={**fix, "note": "Closed at 1am, forgot to punch"}, headers=w.mgr)
    assert done.status_code == 200 and done.json()["status"] == "approved"
    assert done.json()["worked_minutes"] == 360


# ---------------------------------------------------------------------------
# [94] Approve all clean / approve selected
# ---------------------------------------------------------------------------

def _sheet(vid, ts_id, emp, **over):
    ci = datetime.combine(date.today(), dtime(2, 0))
    ts = {"id": ts_id, "venue_id": vid, "employee_id": emp, "work_date": date.today(),
          "clock_in": ci, "clock_out": ci + timedelta(hours=6), "break_minutes": 30,
          "break_source": "recorded", "status": "closed", "pin_verified": True,
          "rostered_shift_id": "sh-x", "variance_minutes": 5, "flags": [], "created_at": ci}
    ts.update(over)
    get_db().save_timesheet(ts)


def test_approve_all_clean_skips_anything_that_needs_a_look():
    w = _world()
    c = w.c
    _sheet(w.vid, "ts-clean", w.e1)
    _sheet(w.vid, "ts-nopin", w.e2, pin_verified=False)
    _sheet(w.vid, "ts-late", w.me, variance_minutes=40)
    _sheet(w.vid, "ts-open", w.mgr_emp, clock_out=None, status="open")
    later = datetime.combine(date.today(), dtime(9, 0))      # e1's other shift that day, not a double
    _sheet(w.vid, "ts-done", w.e1, status="approved", clock_in=later, clock_out=later + timedelta(hours=6))
    _sheet("apo-other", "ts-elsewhere", "someone")
    ids = ["ts-clean", "ts-nopin", "ts-late", "ts-open", "ts-done", "ts-elsewhere"]

    listed = c.get(f"/api/clock/timesheets?venue_id={w.vid}&start_date={date.today()}"
                   f"&end_date={date.today()}", headers=w.mgr).json()
    assert {r["id"] for r in listed["timesheets"] if r["clean"]} == {"ts-clean"}

    body = {"venue_id": w.vid, "ids": ids, "only_clean": True}
    assert c.post("/api/clock/timesheets/approve", json=body, headers=w.staff).status_code == 403
    r = c.post("/api/clock/timesheets/approve", json=body, headers=w.mgr)
    assert r.status_code == 200, r.text
    out = r.json()
    assert out["approved_ids"] == ["ts-clean"]
    why = {s["id"]: s["reason"] for s in out["skipped"]}
    assert why["ts-nopin"] == "PIN not verified"
    assert "+40 min against the roster" in why["ts-late"]
    assert "still clocked in" in why["ts-open"]
    assert why["ts-done"] == "already approved" and why["ts-elsewhere"] == "not found"
    assert get_db().get_timesheet("ts-elsewhere")["status"] == "closed"

    # The rows a manager ticked
    r = c.post("/api/clock/timesheets/approve",
               json={"venue_id": w.vid, "ids": ["ts-nopin", "ts-late"]}, headers=w.mgr)
    assert sorted(r.json()["approved_ids"]) == ["ts-late", "ts-nopin"]
    events = get_db().list_events(venue_id=w.vid, action_prefix="timesheet.approve")
    assert sorted(e["resource_id"] for e in events) == ["ts-clean", "ts-late", "ts-nopin"]
    assert all(e["details"]["batch"] is True for e in events)


def test_a_department_manager_cannot_bulk_approve():
    w = _world()
    c = w.c
    _sheet(w.vid, "ts-clean", w.e1)
    lead_email = f"b5k{uuid.uuid4().hex[:6]}@x.com"
    _login(c, lead_email)
    db = get_db()
    rec = db.get_user_by_email(lead_email)
    rec["venue_ids"], rec["role"], rec["section_grants"] = [w.vid], "manager", {w.vid: ["kitchen"]}
    db.save_user(rec)
    lead = _login(c, lead_email)
    r = c.post("/api/clock/timesheets/approve", json={"venue_id": w.vid, "ids": ["ts-clean"]}, headers=lead)
    assert r.status_code == 403
    assert db.get_timesheet("ts-clean")["status"] == "closed"


# ---------------------------------------------------------------------------
# [150] Split shifts / [154] which venue, search and 'rostered now'
# ---------------------------------------------------------------------------

def test_split_shift_punches_are_matched_to_the_nearest_rostered_shift(clock):
    w = _world()
    c = w.c
    _roster(w.vid, [("sh-lunch", w.e1, DAY, dtime(11), dtime(15), 0),
                    ("sh-dinner", w.e1, DAY, dtime(17), dtime(22), 0)])
    k = kiosk_headers(c, w.mgr, w.vid)

    clock(_at(DAY, 10, 30))
    card = [s for s in c.get(f"/api/clock/board?venue_id={w.vid}", headers=k).json()["staff"]
            if s["employee_id"] == w.e1][0]
    assert [(s["start"], s["end"]) for s in card["shifts"]] == [("11:00", "15:00"), ("17:00", "22:00")]
    assert card["rostered_now"] is True

    clock(_at(DAY, 10, 58))
    assert _punch(c, k, w.vid, w.e1).json()["shift"]["shift_id"] == "sh-lunch"
    clock(_at(DAY, 15, 1))
    assert _punch(c, k, w.vid, w.e1, "out").json()["variance_minutes"] == 3

    clock(_at(DAY, 16, 57))
    assert _punch(c, k, w.vid, w.e1).json()["shift"]["shift_id"] == "sh-dinner"
    clock(_at(DAY, 22))
    assert _punch(c, k, w.vid, w.e1, "out").json()["variance_minutes"] == 3

    clock(_at(DAY, 23, 30))
    board = c.get(f"/api/clock/board?venue_id={w.vid}", headers=k).json()
    me = [s for s in board["staff"] if s["employee_id"] == w.e1][0]
    assert me["state"] == "done" and me["rostered_now"] is False


# ---------------------------------------------------------------------------
# Postgres parity (recorded SQL; no live server)
# ---------------------------------------------------------------------------

class _Cur:
    def __init__(self, log, row=None):
        self.log, self.row = log, row

    def execute(self, sql, params=None):
        self.log.append((" ".join(sql.split()), params))

    def fetchone(self):
        return self.row

    def fetchall(self):
        return [self.row] if self.row else []

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


class _PG:
    _TABLE_DDL = PostgresStore._TABLE_DDL
    _ensure_table = PostgresStore._ensure_table

    def __init__(self, row=None):
        self.log, self.row = [], row

    @contextmanager
    def _cursor(self):
        yield _Cur(self.log, self.row)


def test_postgres_time_clock_statements_bind_every_placeholder():
    pg = _PG(row={"pin_hash": "h", "failed_attempts": 1, "locked_until": None, "id": "kd-1",
                  "employee_id": "e1"})
    ts = {"id": "ts-1", "venue_id": "v", "employee_id": "e1", "work_date": DAY,
          "clock_in": datetime(2026, 10, 14, 3), "clock_out": None, "flags": ["auto_break"],
          "break_source": "auto", "source": "kiosk", "device_id": "kd-1"}
    device = {"id": "kd-1", "venue_id": "v", "name": "Bar iPad", "token_hash": "x"}
    calls = [
        (PostgresStore.save_timesheet, (ts,)),
        (PostgresStore.list_open_timesheets, ("v",)),
        (PostgresStore.set_timeclock_pin, ("v", "e1", "h")),
        (PostgresStore.get_timeclock_pin_record, ("v", "e1")),
        (PostgresStore.list_timeclock_pin_employee_ids, ("v",)),
        (PostgresStore.note_timeclock_pin_failure, ("v", "e1", 5, 15)),
        (PostgresStore.clear_timeclock_pin_failures, ("v", "e1")),
        (PostgresStore.save_kiosk_device, (device,)),
        (PostgresStore.get_kiosk_device, ("kd-1",)),
        (PostgresStore.list_kiosk_devices, ("v",)),
        (PostgresStore.touch_kiosk_device, ("kd-1",)),
        (PostgresStore.note_kiosk_exit_failure, ("kd-1", 5, 15)),
        (PostgresStore.clear_kiosk_exit_failures, ("kd-1",)),
    ]
    for fn, args in calls:
        fn(pg, *args)
    for sql, params in pg.log:
        if sql.startswith("CREATE TABLE"):
            continue
        assert sql.count("%s") == len(params or ()), sql
    upsert = [p for s, p in pg.log if s.startswith("INSERT INTO timesheets")][0]
    assert '["auto_break"]' in upsert and "auto" in upsert and "kd-1" in upsert


def test_postgres_gets_the_new_time_clock_columns_on_existing_tables():
    ensure = inspect.getsource(PostgresStore._ensure_core_schema)
    for col in ("break_started_at", "break_source", "flags", "source", "device_id"):
        assert f"ALTER TABLE timesheets ADD COLUMN IF NOT EXISTS {col} " in ensure, col
        assert col in PostgresStore._TABLE_DDL["timesheets"]
    for col in ("failed_attempts", "locked_until"):
        assert f"ALTER TABLE timeclock_pins ADD COLUMN IF NOT EXISTS {col} " in ensure, col
    for col in ("token_hash", "revoked_at", "exit_failures", "exit_locked_until"):
        assert col in PostgresStore._TABLE_DDL["kiosk_devices"]


def test_memory_and_postgres_stores_both_implement_the_time_clock():
    names = ("list_open_timesheets", "get_timeclock_pin_record", "list_timeclock_pin_employee_ids",
             "note_timeclock_pin_failure", "clear_timeclock_pin_failures", "save_kiosk_device",
             "get_kiosk_device", "list_kiosk_devices", "touch_kiosk_device",
             "note_kiosk_exit_failure", "clear_kiosk_exit_failures")
    for n in names:
        assert n in MemoryStore.__dict__ and n in PostgresStore.__dict__, n


def test_the_demo_reset_clears_demo_time_clocks_only():
    from rosteriq.services.demo import DEMO_VENUE_ID as D
    from rosteriq.services.demo_reset import _memory_reset
    s = MemoryStore()
    s.save_kiosk_device({"id": "kd-demo", "venue_id": D, "name": "x", "token_hash": "h"})
    s.save_kiosk_device({"id": "kd-wem", "venue_id": "wem", "name": "x", "token_hash": "h"})
    s.save_to_key(f"timeclock_settings:{D}", {"allow_self_clock": True})
    s.save_to_key("timeclock_settings:wem", {"allow_self_clock": True})
    _memory_reset(s)
    assert s.get_kiosk_device("kd-demo") is None and s.get_kiosk_device("kd-wem")
    assert s.load_from_key(f"timeclock_settings:{D}") is None
    assert s.load_from_key("timeclock_settings:wem")


# ---------------------------------------------------------------------------
# The pages (their real JS, run under node with a small fake DOM)
# ---------------------------------------------------------------------------

from rosteriq.tests.test_wembley_a6 import _main, _run, _top, needs_node  # noqa: E402


def _fn(src: str, name: str) -> str:
    """A top-level function: a one-liner, or up to its closing brace in column 0."""
    m = re.search(r"^(?:async )?function " + re.escape(name) + r"\(", src, re.M)
    assert m, "function %s not found" % name
    line = src[m.start():src.index("\n", m.start())]
    if line.rstrip().endswith("}") and line.count("{") == line.count("}"):
        return line + "\n"
    return src[m.start():src.index("\n}\n", m.start()) + 2]


def _fns(src: str, *names: str) -> str:
    return "\n".join(_fn(src, n) for n in names)

NO_TIMERS = r"""
global.setInterval = function () { return 1; };
global.clearInterval = function () {};
global.setTimeout = function () { return 1; };
global.clearTimeout = function () {};
"""

FAKE_FETCH = r"""
global.fetched = [];
global.replies = {};
global.fetch = async function (url, opts) {
  opts = opts || {};
  const h = opts.headers || {};
  fetched.push({ url: url, method: opts.method || 'GET', auth: h.Authorization || h.authorization || null,
                 body: opts.body ? JSON.parse(opts.body) : null });
  const path = String(url).split('?')[0];
  const r = replies[path] || { status: 200, body: {} };
  return { ok: r.status < 400, status: r.status, json: async function () { return r.body; } };
};
"""


def _kiosk_js(*names):
    src = _main("timeclock.html")
    return _top(src, "KIOSK_KEY") + "\n" + _fns(src, *names)


@needs_node
def test_the_kiosk_page_sends_its_own_token_and_shows_the_servers_words():
    data = _run(NO_TIMERS + FAKE_FETCH + r"""
var kioskToken = 'KIOSK', managerToken = 'MANAGER';
replies['/api/clock/pin/check'] = { status: 403, body: { error: { message: 'Incorrect PIN — 4 tries left before it locks for 15 minutes.' } } };
replies['/api/clock/in'] = { status: 409, body: { detail: 'Sam Sportsbar is already clocked in' } };
""" + _kiosk_js("errorMessage", "api") + r"""
async function main() {
  const out = {};
  try { await api('/api/clock/pin/check', { method: 'POST', body: '{}' }); } catch (e) { out.pin = e.message; out.status = e.status; }
  try { await api('/api/clock/in', { method: 'POST', body: '{}' }); } catch (e) { out.dup = e.message; }
  out.auth = fetched.map(function (f) { return f.auth; });
  out.blank = errorMessage({}, 500);
  console.log(JSON.stringify(out));
}
""")
    assert data["auth"] == ["Bearer KIOSK", "Bearer KIOSK"]       # never the manager's login
    assert data["pin"].startswith("Incorrect PIN — 4 tries left") and data["status"] == 403
    assert data["dup"] == "Sam Sportsbar is already clocked in"
    assert "Error 500" not in data["blank"] and "ask a manager" in data["blank"]


@needs_node
def test_setting_the_device_up_keeps_the_kiosk_key_and_signs_the_manager_out():
    data = _run(NO_TIMERS + FAKE_FETCH + r"""
localStorage.setItem('jwt_token', 'MANAGER');
localStorage.setItem('jwt_refresh_token', 'REFRESH');
var kioskToken = '', managerToken = 'MANAGER', venueId = 'wem', venueName = 'The Wembley Hotel', deviceId = '';
var staff = [], query = '', filterMode = 'all', boardTimer = null, current = null, pin = '', exitPin = '', toastTimer = null;
replies['/api/kiosk/devices'] = { status: 200, body: { token: 'DEVICE', venue_name: 'The Wembley Hotel', device: { id: 'kd-1' } } };
replies['/api/clock/kiosk'] = { status: 200, body: { venue_id: 'wem', venue_name: 'The Wembley Hotel', device_id: 'kd-1' } };
replies['/api/clock/board'] = { status: 200, body: { staff: [{ employee_id: 'e1', name: 'Riley', state: 'off', shifts: [] }] } };
""" + _kiosk_js("storeGet", "storeSet", "storeDel", "esc", "errorMessage", "api", "setupError", "showSection",
                "signOutHere", "setupDevice", "showOffline", "hideOffline", "startKiosk", "loadBoard",
                "shiftsLabel", "isOnShift", "filterStaff", "stateBadge", "renderBoard", "toast", "switchedOff") + r"""
async function main() {
  await setupDevice();
  const create = fetched.find(function (f) { return f.url === '/api/kiosk/devices'; });
  const logout = fetched.find(function (f) { return f.url === '/api/auth/logout'; });
  const after = fetched.filter(function (f) { return f.url.indexOf('/api/clock/') === 0; });
  console.log(JSON.stringify({
    create: create, logout: logout, after: after.map(function (f) { return f.auth; }),
    kiosk: localStorage.getItem('riq_kiosk_token'), jwt: localStorage.getItem('jwt_token'),
    refresh: localStorage.getItem('jwt_refresh_token'), manager: managerToken, venue: el('venue-name').textContent,
    err: el("setup-err").textContent,
  }));
}
""")
    assert data["create"]["auth"] == "Bearer MANAGER" and data["create"]["body"]["venue_id"] == "wem"
    assert data["logout"]["body"] == {"refresh_token": "REFRESH"}
    assert data["kiosk"] == "DEVICE"
    assert data["jwt"] is None and data["refresh"] is None and data["manager"] == ""
    assert data["after"] and set(data["after"]) == {"Bearer DEVICE"}
    assert data["venue"] == "The Wembley Hotel" and data["err"] == ""


@needs_node
def test_only_a_managers_sign_in_sets_the_clock_up():
    import base64

    def tok(role):
        body = base64.urlsafe_b64encode(json.dumps({"sub": "u", "role": role}).encode()).decode().rstrip("=")
        return "h." + body + ".s"
    data = _run(_kiosk_js("isManagerToken") + r"""
async function main() {
  console.log(JSON.stringify({ m: isManagerToken('""" + tok("manager") + r"""'),
    o: isManagerToken('""" + tok("owner") + r"""'), s: isManagerToken('""" + tok("staff") + r"""'),
    junk: isManagerToken('nope'), none: isManagerToken('') }));
}
""")
    assert data == {"m": True, "o": True, "s": False, "junk": False, "none": False}


@needs_node
def test_the_board_searches_by_name_filters_rostered_now_and_lists_split_shifts():
    data = _run(NO_TIMERS + r"""
var staff = [
  { employee_id: 'e1', name: 'Riley Restaurant', state: 'off', rostered_now: true,
    shifts: [{ start: '11:00', end: '15:00' }, { start: '17:00', end: '22:00' }] },
  { employee_id: 'e2', name: 'Sam Sportsbar', state: 'on', rostered_now: false, shifts: [] },
  { employee_id: 'e3', name: 'Casey <img src=x onerror=alert(1)>', state: 'off', rostered_now: false, stale: false, shifts: [] },
  { employee_id: 'e4', name: 'Dana Duty', state: 'on', stale: true, rostered_now: false, shifts: [] },
], query = '', filterMode = 'all';
""" + _kiosk_js("esc", "shiftsLabel", "isOnShift", "filterStaff", "stateBadge", "renderBoard") + r"""
async function main() {
  const names = function (l) { return l.map(function (s) { return s.employee_id; }); };
  renderBoard();
  const grid = el('staff-grid');
  console.log(JSON.stringify({
    search: names(filterStaff(staff, 'RIL', 'all')),
    now: names(filterStaff(staff, '', 'now')),
    on: names(filterStaff(staff, '', 'on')),
    split: shiftsLabel(staff[0]),
    none: shiftsLabel(staff[1]),
    cards: grid.children.map(function (c) { return c.innerHTML; }),
  }));
}
""")
    assert data["search"] == ["e1"]
    assert data["now"] == ["e1", "e2", "e4"]
    assert data["on"] == ["e2", "e4"]
    assert data["split"] == "Rostered 11:00–15:00 · 17:00–22:00"
    assert data["none"] == "Not rostered today"
    assert "<img" not in "".join(data["cards"]) and "&lt;img" in data["cards"][2]
    assert "STILL CLOCKED IN" in data["cards"][3]


@needs_node
def test_the_kiosk_card_offers_breaks_and_asks_a_stale_punch_for_its_finish_time():
    data = _run(NO_TIMERS + r"""
global.punched = [];
global.punch = function (action, extra) { punched.push({ action: action, extra: extra || null }); };
global.closeModal = function () {};
var modalTimer = null, MODAL_IDLE_MS = 45000;
""" + _kiosk_js("holdModal", "cardSub", "addAction", "modalError", "localHHMM", "showActions") + r"""
function labels() { return el('m-actions').children.map(function (b) { return b.textContent; }); }
function reset() { el('m-actions').children = []; }
async function main() {
  const out = {};
  reset(); showActions({ state: 'off', shifts: [] }); out.off = labels();
  reset(); showActions({ state: 'on', clock_in_label: 'Wed 5:00 pm' }); out.on = labels();
  reset(); showActions({ state: 'break' }); out.brk = labels();
  reset(); showActions({ state: 'on', stale: true, clock_in_label: 'Tue 5:02 pm' });
  out.stale = labels(); out.finish = el('finish-wrap').style.display; out.sub = el('m-sub').textContent;
  el('finish-time').value = '';
  el('m-actions').children[0].onclick();
  out.err = el('m-err').textContent;
  el('finish-time').value = '23:30';
  el('m-actions').children[0].onclick();
  out.punched = punched;
  console.log(JSON.stringify(out));
}
""")
    assert data["off"] == ["Clock in"]
    assert data["on"] == ["Start meal break (unpaid)", "Clock out"]
    assert data["brk"] == ["End meal break", "Clock out"]
    assert data["stale"][0] == "Save my finish time" and data["finish"] == "block"
    assert data["sub"] == "Still clocked in from Tue 5:02 pm"
    assert "Enter the time you finished" in data["err"]
    assert data["punched"] == [{"action": "out", "extra": {"finished_at": "23:30"}}]


@needs_node
def test_timesheets_page_edits_open_punches_and_approves_clean_ones_in_one_go():
    src = _main("dashboard.html")
    data = _run(r"""
global.state = { currentVenue: 'wem' };
global.posted = [];
global.fetchAPI = async function (url, opts) {
  posted.push({ url: url, body: opts && opts.body ? JSON.parse(opts.body) : null });
  return { approved: 1, approved_ids: ['t-clean'], skipped: [{ id: 't-flag', employee: 'Riley', reason: 'PIN not verified' }] };
};
global.loadTimesheets = function () { log.push('reload'); };
var tsSelected = new Set();
var tsRows = [
  { id: 't-open', employee_name: 'Sam', work_date: '2026-10-13', clock_in: '2026-10-13T09:00:00', clock_out: null,
    status: 'open', open_minutes: 23 * 60, break_minutes: 0, flag_labels: [], review_reasons: [], pin_verified: true },
  { id: 't-clean', employee_name: 'Casey', work_date: '2026-10-14', clock_in: '2026-10-14T03:00:00', clock_out: '2026-10-14T09:00:00',
    status: 'closed', clean: true, break_minutes: 30, break_source: 'auto', worked_minutes: 330, variance_minutes: 4,
    flag_labels: ['meal break auto-deducted (rostered)'], review_reasons: [], pin_verified: true },
  { id: 't-flag', employee_name: 'Riley <b>R</b>', work_date: '2026-10-14', clock_in: '2026-10-14T03:00:00', clock_out: '2026-10-14T09:00:00',
    status: 'closed', clean: false, break_minutes: 0, worked_minutes: 360, variance_minutes: 40,
    flag_labels: ['no break recorded'], review_reasons: ['+40 min against the roster', 'PIN not verified', 'no break recorded'], pin_verified: false },
  { id: 't-done', employee_name: 'Dana', work_date: '2026-10-12', clock_in: '2026-10-12T03:00:00', clock_out: '2026-10-12T09:00:00',
    status: 'approved', break_minutes: 30, worked_minutes: 330, variance_minutes: 0, adjustment_note: 'Forgot "out"', pin_verified: true },
];
""" + _fns(src, "tsMinutesLabel", "tsTimeLabel", "tsBreakLabel", "tsChip", "renderTimesheets", "tsUpdateSelection",
           "tsPick", "tsToggleAll", "tsBatchApprove", "approveAllClean", "approveSelected") + r"""
async function main() {
  renderTimesheets();
  const html = el('ts-rows').innerHTML;
  const cleanLabel = el('ts-approve-clean').textContent;
  await approveAllClean();
  tsPick(2, true);
  const selLabel = el('ts-approve-selected').textContent;
  await approveSelected();
  tsToggleAll(true);
  console.log(JSON.stringify({ html: html, cleanLabel: cleanLabel, selLabel: selLabel, posted: posted,
    toasts: toasts, all: Array.from(tsSelected) }));
}
""")
    html = data["html"]
    assert "openEditTimesheet(0)" in html and "OPEN 23H — CHECK" in html       # open punch: Edit
    assert "approveTimesheet(0)" not in html
    assert html.count("tsPick(") == 2                                        # only rows awaiting approval
    assert "auto 30m (rostered)" in html and "CLEAN" in html
    assert "+40 min against the roster" in html and "Riley &lt;b&gt;R&lt;/b&gt;" in html
    assert 'Forgot &quot;out&quot;' in html
    assert data["cleanLabel"] == "Approve all clean (1)"
    assert data["posted"][0] == {"url": "/api/clock/timesheets/approve",
                                 "body": {"venue_id": "wem", "ids": ["t-clean"], "only_clean": True}}
    assert data["selLabel"] == "Approve selected (1)"
    assert data["posted"][1]["body"] == {"venue_id": "wem", "ids": ["t-flag"], "only_clean": False}
    assert "left for a look" in data["toasts"][0]["m"] and "PIN not verified" in data["toasts"][0]["m"]
    assert sorted(data["all"]) == ["t-clean", "t-flag"]


@needs_node
def test_staff_rows_set_a_kiosk_pin_and_generated_pins_print_escaped():
    src = _main("dashboard.html")
    assert "openKioskPin(" in _fn(src, "renderStaffTable")
    data = _run(r"""
global.state = { currentVenue: 'wem', employees: [{ id: 'e1', name: 'Riley Restaurant' }] };
global.posted = [];
global.opened = [];
global.openModal = function (id) { opened.push(id); };
global.closeModal = function () {};
global.fetchAPI = async function (url, opts) {
  posted.push({ url: url, body: opts && opts.body ? JSON.parse(opts.body) : null });
  if (url === '/api/kiosk/pins/generate') return { count: 1, venue_name: 'The Wembley Hotel',
    pins: [{ employee_id: 'e9', name: 'Xavier <b>Kelly</b>', pin: '4821' }] };
  return { employee: 'Riley Restaurant' };
};
var _kioskPinEmp = null, _kioskPinSheet = null;
""" + _fns(src, "openKioskPin", "saveKioskPin", "kioskPinTable", "generateKioskPins") + r"""
async function main() {
  openKioskPin('e1');
  const who = el('kp-who').textContent;
  el('kp-pin').value = '12a';
  await saveKioskPin();
  el('kp-pin').value = '4821';
  await saveKioskPin();
  await generateKioskPins();
  console.log(JSON.stringify({ who: who, posted: posted, toasts: toasts, opened: opened, sheet: el('kps-list').innerHTML }));
}
""")
    assert data["who"] == "Riley Restaurant"
    assert data["posted"][0] == {"url": "/api/clock/pin", "body": {"venue_id": "wem", "employee_id": "e1", "pin": "4821"}}
    assert "4 to 6 digits" in data["toasts"][0]["m"]
    assert data["posted"][1]["url"] == "/api/kiosk/pins/generate"
    assert "Xavier &lt;b&gt;Kelly&lt;/b&gt;" in data["sheet"] and "4821" in data["sheet"]
    assert data["opened"] == ["modal-kiosk-pin", "modal-kiosk-pins"]


@needs_node
def test_settings_lists_time_clocks_and_their_rules():
    src = _main("dashboard.html")
    data = _run(r"""
global.state = { currentVenue: 'wem', venues: [{ id: 'wem', name: 'The Wembley Hotel' }] };
global.fetchAPI = async function (url) {
  if (url.indexOf('/api/kiosk/devices') === 0) return { devices: [
    { id: 'kd-1', name: 'Front bar <iPad>', active: true, created_at: '2026-10-01T01:00:00', last_used_at: null },
    { id: 'kd-0', name: 'Old tablet', active: false, created_at: '2026-09-01T01:00:00', revoked_at: '2026-09-30T01:00:00' } ] };
  return { venue_id: 'wem', auto_meal_break: true, allow_self_clock: false };
};
""" + _fns(src, "_timeClockVenueName", "_whenLabel", "loadTimeClockSettings", "renderKioskDevices") + r"""
async function main() {
  await loadTimeClockSettings();
  console.log(JSON.stringify({ btn: el('tc-setup-btn').textContent, list: el('tc-devices').innerHTML,
    autoBreak: el('tc-auto-break').checked, selfClock: el('tc-self-clock').checked }));
}
""")
    assert data["btn"] == "Set up this device as The Wembley Hotel time clock"
    assert "Front bar &lt;iPad&gt;" in data["list"] and data["list"].count("Switch off") == 1
    assert data["autoBreak"] is True and data["selfClock"] is False


@needs_node
def test_staff_set_their_own_pin_in_the_staff_app():
    src = _main("my.html")
    data = _run(r"""
global.calls = [];
global.api = async function (path, opts) { calls.push({ path: path, body: opts && opts.body }); return {}; };
global.toast = function (m, k) { toasts.push({ m: m, k: k }); };
global.loadMyClock = function () {};
global.loadHours = function () {};
var myClock = { venue_id: 'wem', employee_id: 'e0' };
""" + _fns(src, "renderMyClock", "saveMyPin", "selfPunch") + r"""
async function main() {
  renderMyClock({ linked: true, has_pin: false, self_clock: false, state: 'off' });
  const plain = el('clock-card').innerHTML;
  el('my-pin').value = '12';
  await saveMyPin();
  el('my-pin').value = '2468';
  await saveMyPin();
  renderMyClock({ linked: true, has_pin: true, self_clock: true, state: 'on' });
  const self = el('clock-card').innerHTML;
  await selfPunch('break/start');
  console.log(JSON.stringify({ plain: plain, self: self, calls: calls, toasts: toasts }));
}
""")
    assert "Set my PIN" in data["plain"] and "Clock in" not in data["plain"]
    assert "Change my PIN" in data["self"] and "Start break" in data["self"] and "Clock out" in data["self"]
    assert data["calls"][0] == {"path": "/api/me/clock-pin", "body": {"pin": "2468"}}
    assert data["calls"][1] == {"path": "/api/clock/break/start", "body": {"venue_id": "wem", "employee_id": "e0"}}
    assert data["toasts"][0]["k"] == "err"
