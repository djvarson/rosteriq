"""
Kiosk PINs and whose record a login is (review F 0, 4, 43).

* Nobody sets a PIN anyone would try first (1234, 0000, 2580, 7890...),
  themselves or through a manager. A time clock that sees eight wrong PINs on
  more than one card inside ten minutes pauses PIN entry for a minute,
  doubling while it keeps happening (429 + Retry-After), records a security
  event and tells the venue's managers once. One person fumbling their own
  PIN never pauses the device: that is the card's own lock.
* A login's own record at a venue is the ONE record there carrying its
  email. /employees/bulk refuses an email another record (or another row of
  the batch) already uses, as POST /employees does.
* The kiosk PIN and phone clocking on /my reach every venue the login holds,
  staff-only ones included: a manager at the Wembley who works shifts at
  Apothecary sets one PIN for both and clocks herself in at either, and /my
  shows one clock card per venue.
"""

import re
import uuid
from contextlib import contextmanager
from datetime import datetime, timedelta

import pytest

from rosteriq.database import MemoryStore, PostgresStore, get_db
from rosteriq.services import kiosk
from rosteriq.tests.kiosk_support import kiosk_headers
from rosteriq.tests.test_wembley_a6 import _main, _run, needs_node
from rosteriq.tests.test_wembley_b5 import _fns
from rosteriq.tests.test_wembley_b6 import _emp, _link, _login, _person, _promote, _venue, _world

PINS = ["4826", "5937", "6148", "7259", "8361", "9472"]


def _msg(r):
    body = r.json()
    return (body.get("error") or {}).get("message") or body.get("detail") or ""


def _staff_with_pins(w, n=6):
    """n people at the Wembley, each with a kiosk PIN their manager set."""
    ids = []
    for i in range(n):
        eid = _emp(w.c, w.ben, w.vw, f"{w.vw}-s{i}", f"Staff {i}")
        r = w.c.post("/api/clock/pin", json={"venue_id": w.vw, "employee_id": eid, "pin": PINS[i]},
                     headers=w.ben)
        assert r.status_code == 200, r.text
        ids.append(eid)
    return ids


def _events(vid, action, category):
    return [e for e in get_db().list_events(venue_id=vid, action_prefix=action, limit=1000)
            if e["action"] == action and (e.get("details") or {}).get("category") == category]


# ---------------------------------------------------------------------------
# [0] Weak PINs are refused; a time clock can't be used to spray PINs
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("pin", ["1234", "4321", "0000", "1111", "999999", "1212", "2580",
                                 "0123", "3456", "7890", "8901", "9876", "123456", "654321"])
def test_weak_pins(pin):
    assert kiosk.weak_pin(pin)


@pytest.mark.parametrize("pin", ["4826", "1357", "2468", "97531", "1235", "8642", "102938"])
def test_ordinary_pins_are_fine(pin):
    assert not kiosk.weak_pin(pin)


def test_generated_pins_are_never_weak():
    assert not any(kiosk.weak_pin(kiosk.generate_pin()) for _ in range(3000))


def test_staff_and_managers_cannot_set_a_pin_anyone_would_guess():
    w = _world()
    c = w.c
    for pin in ("1234", "0000", "1111", "2580", "7890"):
        r = c.post("/api/me/clock-pin", json={"pin": pin}, headers=w.meca)
        assert r.status_code == 422 and "too easy to guess" in _msg(r), (pin, r.text)
        r = c.post("/api/clock/pin", json={"venue_id": w.vw, "employee_id": w.mw, "pin": pin},
                   headers=w.ben)
        assert r.status_code == 422 and "too easy to guess" in _msg(r), (pin, r.text)
    db = get_db()
    assert not db.get_timeclock_pin(w.vw, w.mw) and not db.get_timeclock_pin(w.va, w.ma)
    assert c.post("/api/me/clock-pin", json={"pin": "4826"}, headers=w.meca).status_code == 200
    assert c.post("/api/clock/pin", json={"venue_id": w.vw, "employee_id": w.mw, "pin": "5937"},
                  headers=w.ben).status_code == 200


def test_a_time_clock_pauses_pin_entry_when_someone_works_through_the_cards():
    w = _world()
    c, db = w.c, get_db()
    s = _staff_with_pins(w)
    k = kiosk_headers(c, w.ben, w.vw, "Bar iPad")
    device_id = db.list_kiosk_devices(w.vw)[0]["id"]
    check = lambda emp, pin: c.post("/api/clock/pin/check",  # noqa: E731
                                    json={"venue_id": w.vw, "employee_id": emp, "pin": pin}, headers=k)
    guesses = ["0000", "1111", "1212", "2580"]

    # Four guesses on one card stay under its own lock; three on the next...
    for g in guesses:
        assert check(s[0], g).status_code == 403
    for g in guesses[:3]:
        assert check(s[1], g).status_code == 403
    # ...and the eighth wrong PIN on this device pauses PIN entry.
    paused = check(s[1], guesses[3])
    assert paused.status_code == 429, paused.text
    assert "paused" in _msg(paused) and "60 seconds" in _msg(paused)
    assert 55 <= int(paused.headers["retry-after"]) <= 60

    # Nobody's PIN is checked while it is paused, not even a right one.
    blocked = check(s[2], PINS[2])
    assert blocked.status_code == 429 and int(blocked.headers["retry-after"]) <= 60
    punch = c.post("/api/clock/in", json={"venue_id": w.vw, "employee_id": s[2], "pin": PINS[2]},
                   headers=k)
    assert punch.status_code == 429
    assert not db.get_open_timesheet(w.vw, s[2])
    assert db.get_kiosk_device(device_id)["pin_failures"] == 0      # paused tries aren't counted
    # Someone with no PIN still clocks in (unverified, as before).
    nopin = c.post("/api/clock/in", json={"venue_id": w.vw, "employee_id": w.mw}, headers=k)
    assert nopin.status_code == 200 and nopin.json()["pin_verified"] is False

    # A security event, and the managers' notice in Activity
    assert len(_events(w.vw, "timeclock.kiosk_pin_paused", "security")) == 1
    notices = _events(w.vw, "timeclock.kiosk_pin_paused", "audit")
    assert len(notices) == 1 and notices[0]["resource_id"] == device_id
    assert notices[0]["details"]["device"] == "Bar iPad"

    # The pause ends: a right PIN on a fresh card works again.
    db._kiosk_devices[device_id]["pin_cooldown_until"] = datetime.utcnow() - timedelta(seconds=1)
    ok = check(s[2], PINS[2])
    assert ok.status_code == 200 and ok.json()["pin_set"] is True

    # Keep going and the next pause is twice as long; managers aren't told twice.
    for g in guesses:
        check(s[3], g)
    for g in guesses[:3]:
        assert check(s[4], g).status_code == 403
    again = check(s[4], guesses[3])
    assert again.status_code == 429 and "2 minutes" in _msg(again)
    assert 115 <= int(again.headers["retry-after"]) <= 120
    assert len(_events(w.vw, "timeclock.kiosk_pin_paused", "security")) == 2
    assert len(_events(w.vw, "timeclock.kiosk_pin_paused", "audit")) == 1


def test_one_person_fumbling_their_own_pin_never_pauses_the_time_clock():
    w = _world()
    c, db = w.c, get_db()
    s = _staff_with_pins(w, 2)
    k = kiosk_headers(c, w.ben, w.vw)
    check = lambda emp, pin: c.post("/api/clock/pin/check",  # noqa: E731
                                    json={"venue_id": w.vw, "employee_id": emp, "pin": pin}, headers=k)
    for _ in range(4):
        assert check(s[0], "1357").status_code == 403
    locked = check(s[0], "1357")
    assert locked.status_code == 429 and "locked for 15 minutes" in _msg(locked)
    c.post("/api/clock/pin", json={"venue_id": w.vw, "employee_id": s[0], "pin": PINS[0]}, headers=w.ben)
    for _ in range(4):
        assert check(s[0], "1357").status_code == 403
    assert check(s[1], PINS[1]).status_code == 200
    assert check(s[0], PINS[0]).status_code == 200
    device = db.list_kiosk_devices(w.vw)[0]
    assert device["pin_failures"] == 9 and device["pin_window_card"] == s[0]
    assert not device.get("pin_cooldown_until")
    assert not _events(w.vw, "timeclock.kiosk_pin_paused", "security")


def test_a_broken_device_counter_leaves_the_cards_own_lock(monkeypatch):
    w = _world()
    c = w.c
    s = _staff_with_pins(w, 1)
    k = kiosk_headers(c, w.ben, w.vw)

    def broken(*a, **kw):
        raise RuntimeError("column pin_failures does not exist")
    monkeypatch.setattr(type(get_db()), "note_kiosk_pin_failure", broken)
    check = lambda pin: c.post("/api/clock/pin/check",  # noqa: E731
                               json={"venue_id": w.vw, "employee_id": s[0], "pin": pin}, headers=k)
    assert check("1357").status_code == 403
    assert check(PINS[0]).status_code == 200


def test_memory_store_pin_pause_counts_windows_cards_and_doubles():
    s = MemoryStore()
    s.save_kiosk_device({"id": "kd", "venue_id": "v", "name": "Bar iPad", "token_hash": "h"})
    note = lambda emp: s.note_kiosk_pin_failure("kd", emp, 8, 10, 60, 600)  # noqa: E731
    now = datetime.utcnow

    for _ in range(20):                                   # one card: its own lock's job
        row = note("a")
    assert not row.get("pin_cooldown_until") and row["pin_window_card"] == "a"
    s._kiosk_devices["kd"]["pin_window_started"] = now() - timedelta(minutes=11)
    row = note("a")
    assert row["pin_failures"] == 1                       # the window ran out: a new count

    for _ in range(6):
        row = note("b")
    assert row["pin_failures"] == 7 and row["pin_window_card"] is None
    assert not row.get("pin_cooldown_until")
    row = note("c")
    secs = (row["pin_cooldown_until"] - now()).total_seconds()
    assert 55 <= secs <= 60 and row["pin_cooldowns"] == 1
    assert row["pin_failures"] == 0 and row["pin_window_started"] is None

    def pause_again():
        s._kiosk_devices["kd"]["pin_cooldown_until"] = now() - timedelta(minutes=1)
        for emp in "abababab":
            r = note(emp)
        return r

    row = pause_again()
    assert row["pin_cooldowns"] == 2 and 115 <= (row["pin_cooldown_until"] - now()).total_seconds() <= 120
    s._kiosk_devices["kd"]["pin_cooldowns"] = 9
    row = pause_again()
    assert row["pin_cooldowns"] == 10 and 595 <= (row["pin_cooldown_until"] - now()).total_seconds() <= 600
    # A quiet spell longer than the window starts the doubling over.
    s._kiosk_devices["kd"]["pin_cooldown_until"] = now() - timedelta(minutes=11)
    for emp in "abababab":
        row = note(emp)
    assert row["pin_cooldowns"] == 1 and (row["pin_cooldown_until"] - now()).total_seconds() <= 60
    assert s.note_kiosk_pin_failure("ghost", "a", 8, 10, 60, 600) is None


class _Cur:
    def __init__(self, log, row):
        self.log, self.row = log, row

    def execute(self, sql, params=None):
        self.log.append((" ".join(sql.split()), params))

    def fetchone(self):
        return self.row

    def fetchall(self):
        return [self.row] if self.row else []


class _PG:
    _TABLE_DDL = PostgresStore._TABLE_DDL
    _ensure_table = PostgresStore._ensure_table

    def __init__(self, row=None):
        self.log, self.row = [], row

    @contextmanager
    def _cursor(self):
        yield _Cur(self.log, self.row)


def test_postgres_pin_pause_is_one_update_on_the_shared_connection():
    row = {"id": "kd-1", "name": "Bar iPad", "pin_failures": 0, "pin_cooldowns": 1,
           "pin_cooldown_until": datetime(2026, 10, 10, 9, 1)}
    pg = _PG(row=row)
    out = PostgresStore.note_kiosk_pin_failure(pg, "kd-1", "e1", 8, 10, 60, 600)
    assert out == row
    stmts = [(s, p) for s, p in pg.log if not s.startswith("CREATE TABLE")]
    assert len(stmts) == 1, pg.log
    sql, params = stmts[0]
    assert sql.startswith("UPDATE kiosk_devices SET") and sql.endswith("WHERE id = %(id)s RETURNING *")
    for word in ("BEGIN", "COMMIT", "ROLLBACK", "SELECT"):
        assert word not in sql.upper().split(), sql
    assert "%s" not in sql
    assert set(re.findall(r"%\((\w+)\)s", sql)) == set(params)
    assert params == {"window": 10, "emp": "e1", "max": 8, "pause": 60, "cap": 600, "id": "kd-1"}
    for col in ("pin_failures", "pin_window_started", "pin_window_card", "pin_cooldowns",
                "pin_cooldown_until"):
        assert f" {col} = CASE WHEN" in sql, col
    assert sql.count("(") == sql.count(")")
    assert "make_interval(secs => LEAST(%(cap)s, %(pause)s * power(2," in sql
    assert PostgresStore.note_kiosk_pin_failure(_PG(), "ghost", "e1", 8, 10, 60, 600) is None


def test_postgres_kiosk_devices_carry_the_pause_columns_on_new_and_existing_tables():
    import inspect
    ddl = PostgresStore._TABLE_DDL["kiosk_devices"]
    ensure = inspect.getsource(PostgresStore._ensure_core_schema)
    pg = _PG()
    PostgresStore.save_kiosk_device(pg, {"id": "kd-1", "venue_id": "v", "name": "x", "token_hash": "h"})
    insert, params = [(s, p) for s, p in pg.log if s.startswith("INSERT INTO kiosk_devices")][0]
    assert insert.count("%s") == len(params)
    for col in ("pin_failures", "pin_window_started", "pin_window_card", "pin_cooldowns",
                "pin_cooldown_until"):
        assert col in ddl, col
        assert f"ALTER TABLE IF EXISTS kiosk_devices ADD COLUMN IF NOT EXISTS {col} " in ensure, col
        assert col in insert.split("VALUES")[0], col
        # counters are only changed by their own one-statement update
        assert f"{col}=EXCLUDED" not in insert
    for n in ("note_kiosk_pin_failure",):
        assert n in MemoryStore.__dict__ and n in PostgresStore.__dict__


# ---------------------------------------------------------------------------
# [4] A login's own record is the one record with its email
# ---------------------------------------------------------------------------

def test_own_record_needs_to_be_the_only_one_with_the_email():
    w = _world()
    db = get_db()
    jess = _emp(w.c, w.ben, w.vw, f"{w.vw}-jess", "Jess", email=f"parent_{w.tag}@x.com")
    tom = _emp(w.c, w.ben, w.vw, f"{w.vw}-tom", "Tom")
    acct = {"id": "u-parent", "email": f"Parent_{w.tag}@x.com"}
    assert kiosk.own_employee_ids(db, acct, w.vw) == {jess}
    rec = db.get_employee(tom)
    rec.email = f"parent_{w.tag}@x.com"        # data that predates the rule
    db.save_employee(rec)
    assert kiosk.own_employee_ids(db, acct, w.vw) == set()


def test_a_shared_email_sets_no_pin_and_clocks_nobody_in():
    w = _world()
    c, db = w.c, get_db()
    email = f"parent_{w.tag}@x.com"
    jess = _emp(c, w.ben, w.vw, f"{w.vw}-jess", "Jess", email=email)
    tom = _emp(c, w.ben, w.vw, f"{w.vw}-tom", "Tom")
    h = _login(c, email)
    assert _link(c, w.ben, jess, h).status_code == 200
    rec = db.get_employee(tom)
    rec.email = email.upper()
    db.save_employee(rec)

    assert c.post("/api/me/clock-pin", json={"pin": "4826"}, headers=h).status_code == 409
    assert not db.get_timeclock_pin(w.vw, jess) and not db.get_timeclock_pin(w.vw, tom)
    assert c.get(f"/api/me/clock?venue_id={w.vw}", headers=h).json() == {"linked": False}
    c.put("/api/kiosk/settings", json={"venue_id": w.vw, "allow_self_clock": True}, headers=w.ben)
    for emp in (tom, jess):
        r = c.post("/api/clock/in", json={"venue_id": w.vw, "employee_id": emp}, headers=h)
        assert r.status_code == 403 and "only clock yourself" in _msg(r)
    assert not db.get_open_timesheet(w.vw, tom)


def _bulk_row(vid, eid, name, email=None):
    row = {"id": eid, "venue_id": vid, "name": name, "employment_type": "casual",
           "award_level": "level_2", "state": "wa", "hourly_base_rate": "30.00",
           "skills": ["bar"], "created_at": "2026-07-01T00:00:00",
           "updated_at": "2026-07-01T00:00:00"}
    if email:
        row["email"] = email
    return row


def test_bulk_create_refuses_an_email_someone_else_already_uses():
    w = _world()
    c, db = w.c, get_db()
    tag = uuid.uuid4().hex[:6]
    parent = f"parent_{tag}@x.com"
    same_batch = [_bulk_row(w.vw, f"{w.vw}-jess", "Jess", parent),
                  _bulk_row(w.vw, f"{w.vw}-tom", "Tom", parent.upper())]
    r = c.post("/employees/bulk", json=same_batch, headers=w.ben)
    assert r.status_code == 409, r.text
    assert "Row 2 (Tom)" in _msg(r) and "row 1 (Jess)" in _msg(r)
    assert db.get_employee(f"{w.vw}-jess") is None and db.get_employee(f"{w.vw}-tom") is None

    # Clashing with a record already on the staff list (Meca's)
    r = c.post("/employees/bulk", json=[_bulk_row(w.vw, f"{w.vw}-ok", "Okay", f"ok_{tag}@x.com"),
                                        _bulk_row(w.vw, f"{w.vw}-twin", "Twin", w.meca_email)],
               headers=w.ben)
    assert r.status_code == 409 and "Row 2 (Twin)" in _msg(r) and "Meca Rossi" in _msg(r)
    assert db.get_employee(f"{w.vw}-ok") is None

    # The same email at two different venues is one person working both
    r = c.post("/employees/bulk", json=[_bulk_row(w.vw, f"{w.vw}-jess", "Jess", parent),
                                        _bulk_row(w.va, f"{w.va}-jess", "Jess", parent),
                                        _bulk_row(w.vw, f"{w.vw}-tom", "Tom")], headers=w.ben)
    assert r.status_code == 200, r.text
    # Re-sending a record with its own email is an update, not a clash
    r = c.post("/employees/bulk", json=[_bulk_row(w.vw, f"{w.vw}-jess", "Jess S", parent)],
               headers=w.ben)
    assert r.status_code == 200, r.text


# ---------------------------------------------------------------------------
# [43] /my's PIN and phone clock reach a manager's staff-only venue
# ---------------------------------------------------------------------------

def test_a_manager_sets_one_pin_and_clocks_herself_in_at_her_staff_only_venue():
    w = _world()
    c, db = w.c, get_db()
    riley = _person(w, w.vw, "Riley")
    _promote(w, riley.id)
    ra = _emp(c, w.ben, w.va, f"{w.va}-riley", "Riley", email=riley.email)
    assert _link(c, w.ben, ra, riley.h).status_code == 200
    acct = db.get_user_by_email(riley.email)
    assert acct["venue_ids"] == [w.vw] and acct["staff_venue_ids"] == [w.va]

    r = c.post("/api/me/clock-pin", json={"pin": "4826"}, headers=riley.h)
    assert r.status_code == 200 and r.json()["venues"] == 2
    assert db.get_timeclock_pin(w.vw, riley.id) and db.get_timeclock_pin(w.va, ra)
    mine = c.get(f"/api/me/clock?venue_id={w.va}", headers=riley.h).json()
    assert mine["linked"] is True and mine["employee_id"] == ra and mine["has_pin"] is True

    ka = kiosk_headers(c, w.ben, w.va, "Apothecary iPad")
    ok = c.post("/api/clock/pin/check", json={"venue_id": w.va, "employee_id": ra, "pin": "4826"},
                headers=ka)
    assert ok.status_code == 200 and ok.json()["pin_set"] is True

    # Phone clocking at Apothecary once it allows it: herself only
    c.put("/api/kiosk/settings", json={"venue_id": w.va, "allow_self_clock": True}, headers=w.ben)
    r = c.post("/api/clock/in", json={"venue_id": w.va, "employee_id": ra}, headers=riley.h)
    assert r.status_code == 200, r.text
    assert db.get_open_timesheet(w.va, ra)["source"] == "self"
    r = c.post("/api/clock/in", json={"venue_id": w.va, "employee_id": w.ma}, headers=riley.h)
    assert r.status_code == 403 and "only clock yourself" in _msg(r)
    # Still the Wembley's rule at the Wembley
    r = c.post("/api/clock/in", json={"venue_id": w.vw, "employee_id": riley.id}, headers=riley.h)
    assert r.status_code == 403 and "switched off" in _msg(r)

    # Deactivated mid-shift, she can still clock herself out
    rec = db.get_employee(ra)
    rec.active = False
    db.save_employee(rec)
    r = c.post("/api/clock/out", json={"venue_id": w.va, "employee_id": ra}, headers=riley.h)
    assert r.status_code == 200, r.text

    # A venue she doesn't hold is refused before its rules are read
    other_h = _login(c, f"other_{w.tag}@x.com")
    vo = f"oth-{w.tag}"
    _venue(c, other_h, vo, "Unrelated Pub")
    mo = _emp(c, other_h, vo, f"{vo}-riley", "Riley", email=riley.email)
    c.put("/api/kiosk/settings", json={"venue_id": vo, "allow_self_clock": True}, headers=other_h)
    r = c.post("/api/clock/in", json={"venue_id": vo, "employee_id": mo}, headers=riley.h)
    assert r.status_code == 403 and "do not have access" in _msg(r)
    assert c.get(f"/api/me/clock?venue_id={vo}", headers=riley.h).json() == {"linked": False}
    assert not db.get_timeclock_pin(vo, mo)


@needs_node
def test_my_shows_a_clock_card_for_each_venue_and_punches_at_the_right_one():
    src = _main("my.html")
    data = _run(r"""
global.calls = [];
global.toast = function (m, k) { toasts.push({ m: m, k: k }); };
global.loadHours = function () {};
global.myVenues = [{ venue_id: 'wem', venue_name: 'The Wembley Hotel', employee_id: 'w-meca' },
                   { venue_id: 'apo', venue_name: 'Apo<b>thecary', employee_id: 'a-meca' }];
global.api = async function (path, opts) {
  calls.push({ path: path, body: opts && opts.body });
  if (path === '/api/me/clock?venue_id=wem')
    return { linked: true, venue_id: 'wem', employee_id: 'w-meca', has_pin: true, self_clock: false, state: 'off' };
  if (path === '/api/me/clock?venue_id=apo')
    return { linked: true, venue_id: 'apo', employee_id: 'a-meca', has_pin: false, self_clock: true, state: 'off' };
  return {};
};
var myClocks = [];
""" + _fns(src, "loadMyClock", "clockVenueName", "renderMyClock", "selfPunch") + r"""
async function main() {
  await loadMyClock();
  const html = el('clock-card').innerHTML;
  await selfPunch('in', 1);
  console.log(JSON.stringify({ html: html, calls: calls }));
}
""")
    paths = [c["path"] for c in data["calls"]]
    assert paths[:2] == ["/api/me/clock?venue_id=wem", "/api/me/clock?venue_id=apo"]
    html = data["html"]
    assert "The Wembley Hotel" in html and "Apo&lt;b&gt;thecary" in html and "<b>" not in html
    assert "only set at some of your venues" in html and "Set my PIN" in html
    assert html.count("Clock in from your phone") == 1 and "selfPunch('in', 1)" in html
    punch = [c for c in data["calls"] if c["path"] == "/api/clock/in"]
    assert punch == [{"path": "/api/clock/in", "body": {"venue_id": "apo", "employee_id": "a-meca"}}]
