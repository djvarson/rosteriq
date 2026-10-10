"""
Paste importers, second pass (synthetic data; the rates below are made-up
venue records, not award values).

Staff: shared emails counted over this venue's rows only, one person on two
rows, update-existing merging positions into trained areas and flagging
removals, a one-location export for another venue, visa expiry formats,
'k' and comma-decimal pay figures, implausible rates flagged, a long rate
cell parsed in linear time, unclosed quotes.

Roster: one day/month order per file, a re-paste as the truth for each
person on the dates it covers, implausible spans and 12-hour times.
"""

import asyncio
import time
import uuid
from datetime import date, datetime, time as dtime, timedelta
from decimal import Decimal

from fastapi.testclient import TestClient

from rosteriq.api import app
from rosteriq.cost_calculator import calculate_roster_cost
from rosteriq.database import get_db
from rosteriq.models import Roster, Shift, ShiftStatus, State
from rosteriq.routes import roster_import, staff_import

WEMBLEY = "The Wembley Hotel"
APOTHECARY = "Apothecary"
DOM = "wembley-synthetic.com.au"


def _owner(c):
    email = f"wf2{uuid.uuid4().hex[:8]}@{DOM}"
    c.post("/api/auth/register", json={"email": email, "password": "Passw0rd!234", "name": "Ben"})
    tok = c.post("/api/auth/login", json={"email": email, "password": "Passw0rd!234"}).json()["access_token"]
    return {"Authorization": f"Bearer {tok}"}


def _venue(c, h, name=WEMBLEY):
    vid = f"wf2-{uuid.uuid4().hex[:8]}"
    r = c.post("/venues", json={
        "id": vid, "name": name, "state": "wa", "max_labour_pct": 30,
        "tanda_org_id": "", "created_at": "2026-07-01T00:00:00",
    }, headers=h)
    assert r.status_code == 200, r.text
    return vid


def _world(name=WEMBLEY):
    c = TestClient(app)
    h = _owner(c)
    return c, h, _venue(c, h, name)


def _staff(c, h, vid, content, **extra):
    return c.post("/api/setup/import-staff", json={"venue_id": vid, "content": content, **extra}, headers=h)


def _roster(c, h, vid, content, **extra):
    return c.post("/api/setup/import-roster", json={"venue_id": vid, "content": content, **extra}, headers=h)


def _emps(vid):
    return {e.name: e for e in get_db().get_employees(vid)}


def _rows_by_line(out):
    return {r["line"]: r for r in out["rows"] if r.get("line")}


def _shifts(vid):
    names = {e.id: e.name for e in get_db().get_employees(vid)}
    return sorted((names[s.employee_id], s.date, s.start_time.strftime("%H:%M"), s.end_time.strftime("%H:%M"))
                  for r in get_db().list_rosters() if r.venue_id == vid for s in r.shifts)


# --- [4] shared emails --------------------------------------------------------

def test_shared_staff_listed_once_per_location_keep_their_email():
    c, h, vid = _world()
    content = ("First Name,Last Name,Email,Pay Rate,Main Location\n"
               f"Jo,Shared,jo@{DOM},30,{WEMBLEY}\n"
               f"Jo,Shared,jo@{DOM},30,{APOTHECARY}\n"
               f"Al,Only,al@{DOM},30,{WEMBLEY}\n")
    r = _staff(c, h, vid, content)
    assert r.status_code == 200, r.text
    out = r.json()
    assert _emps(vid)["Jo Shared"].email == f"jo@{DOM}"
    rows = _rows_by_line(out)
    assert rows[2]["status"] == "created" and "email_shared" not in rows[2]["flags"]
    assert rows[3]["status"] == "skipped" and rows[3]["reason"] == f"other location: {APOTHECARY}"
    assert "email_shared" not in out["flags_summary"]


def test_one_person_on_two_rows_is_one_person_with_both_areas():
    c, h, vid = _world()
    content = ("Name,Email,Pay Rate,Training\n"
               f"Jo Shared,jo@{DOM},30,Front Bar\n"
               f"jo  shared,JO@{DOM},30,Restaurant\n")
    out = _staff(c, h, vid, content).json()
    emps = get_db().get_employees(vid)
    assert len(emps) == 1
    assert emps[0].email == f"jo@{DOM}"
    assert emps[0].skills == ["front bar", "restaurant"]
    rows = _rows_by_line(out)
    assert rows[2]["status"] == "created" and "email_shared" not in rows[2]["flags"]
    assert rows[3]["status"] == "skipped" and "same person as line 2" in rows[3]["reason"]
    assert out["created_count"] == 1

    # Two different people on one address are still a shared inbox
    c, h, vid = _world()
    out = _staff(c, h, vid, f"Name,Email\nA One,jobs@{DOM}\nB Two,jobs@{DOM}\n").json()
    assert out["flags_summary"]["email_shared"] == 2
    assert all(e.email is None for e in get_db().get_employees(vid))


# --- [5] update-existing and trained areas -------------------------------------------

_JO_TRAINED = ('First Name,Last Name,Email,Pay Rate,Training,Mobile\n'
               f'Jo,Smith,jo@{DOM},30,"Front Bar, Kitchen, Cellar, Duty Manager",0412345678\n')


def test_update_existing_merges_a_position_into_trained_areas():
    c, h, vid = _world()
    _staff(c, h, vid, _JO_TRAINED)
    out = _staff(c, h, vid, "First Name,Last Name,Email,Mobile,Department\n"
                            f"Jo,Smith,jo@{DOM},0412 999 888,Bar\n", update_existing=True).json()
    jo = _emps(vid)["Jo Smith"]
    assert jo.skills == ["front bar", "kitchen", "cellar", "duty manager", "bar"]
    row = _rows_by_line(out)[2]
    assert row["status"] == "updated" and set(row["changed"]) == {"phone", "skills"}
    assert "skills_removed" not in row["flags"]

    # A position already among their areas changes nothing
    out = _staff(c, h, vid, f"First Name,Last Name,Email,Job Title\nJo,Smith,jo@{DOM},Kitchen\n",
                 update_existing=True).json()
    assert _rows_by_line(out)[2]["reason"] == "already up to date"
    assert _emps(vid)["Jo Smith"].skills == ["front bar", "kitchen", "cellar", "duty manager", "bar"]


def test_update_existing_training_column_replaces_and_flags_what_it_removed():
    c, h, vid = _world()
    _staff(c, h, vid, _JO_TRAINED)
    out = _staff(c, h, vid, "First Name,Last Name,Email,Training\n"
                            f'Jo,Smith,jo@{DOM},"Front Bar; Restaurant; Duty Manager"\n',
                 update_existing=True).json()
    assert _emps(vid)["Jo Smith"].skills == ["front bar", "restaurant", "duty manager"]
    row = _rows_by_line(out)[2]
    assert row["status"] == "updated" and "skills" in row["changed"]
    assert "skills_removed" in row["flags"]
    note = " ".join(row["notes"])
    assert "kitchen" in note and "cellar" in note
    assert out["flags_summary"]["skills_removed"] == 1

    # A blank training cell never wipes the recorded areas
    out = _staff(c, h, vid, f"Name,Email,Training,Mobile\nJo Smith,jo@{DOM},,0400 000 111\n",
                 update_existing=True).json()
    assert _rows_by_line(out)[2]["changed"] == ["phone"]
    assert _emps(vid)["Jo Smith"].skills == ["front bar", "restaurant", "duty manager"]


# --- [6] a one-location export for another venue ------------------------------------

def _choice(r):
    body = r.json()
    return body["detail"], body["error"]


def test_single_location_export_for_another_venue_asks_which_location():
    c, h, vid = _world()
    content = ("First Name,Last Name,Email,Location\n"
               f"Anna,Apo,anna@{DOM},{APOTHECARY}\nBen,Apo,ben@{DOM},{APOTHECARY}\n")
    r = _staff(c, h, vid, content)
    assert r.status_code == 422, r.text
    detail, error = _choice(r)
    assert detail["code"] == "location_choice_needed" and detail["locations"] == [APOTHECARY]
    assert APOTHECARY in detail["message"] and WEMBLEY in detail["message"]
    assert error["message"] == detail["message"] and error["locations"] == [APOTHECARY]
    assert get_db().get_employees(vid) == []

    r = _staff(c, h, vid, content, location=APOTHECARY)           # the manager confirms
    assert r.status_code == 200 and r.json()["created_count"] == 2

    roster = f"Name,Date,Start,End,Location\nAnna Apo,12/10/2026,17:00,23:00,{APOTHECARY}\n"
    r = _roster(c, h, vid, roster)
    assert r.status_code == 422 and _choice(r)[0]["code"] == "location_choice_needed"
    assert get_db().get_roster(f"imported-{vid}-2026-10-12") is None
    r = _roster(c, h, vid, roster, location=APOTHECARY)
    assert r.status_code == 200 and r.json()["shifts_imported"] == 1

    # The venue's own location needs no confirmation
    c, h, vid = _world()
    r = _staff(c, h, vid, f"Name,Email,Location\nX Y,xy@{DOM},{WEMBLEY}\n")
    assert r.status_code == 200 and r.json()["created_count"] == 1


def test_ambiguous_multi_location_export_carries_the_picker_contract():
    c, h, vid = _world(name="Ben's Group")
    r = _staff(c, h, vid, f"Name,Email,Location\nA B,ab@{DOM},North\nC D,cd@{DOM},South\n")
    assert r.status_code == 422
    detail, _ = _choice(r)
    assert detail["code"] == "location_choice_needed" and detail["locations"] == ["North", "South"]


# --- [7] day/month order -----------------------------------------------------------

def _amy(c, h, vid):
    assert _staff(c, h, vid, f"Name,Email,Pay Rate\nAmy Lee,amy@{DOM},30\n").status_code == 200


def test_month_first_file_is_read_month_first_throughout():
    c, h, vid = _world()
    _amy(c, h, vid)
    rows = "\n".join(f"Amy Lee,10/{d}/2026,17:00,23:00" for d in range(5, 19))
    r = _roster(c, h, vid, "Employee,Date,Start,End\n" + rows + "\n")
    assert r.status_code == 200, r.text
    out = r.json()
    assert out["shifts_imported"] == 14 and out["weeks"] == 2
    assert out["date_order"] == "month/day"
    assert [s[1] for s in _shifts(vid)] == [date(2026, 10, d) for d in range(5, 19)]

    # A date carried in the Start cell follows the same order
    c, h, vid = _world()
    _amy(c, h, vid)
    out = _roster(c, h, vid, "Employee,Start,End\nAmy Lee,10/13/2026 17:00,10/13/2026 23:00\n"
                             "Amy Lee,10/05/2026 17:00,10/05/2026 23:00\n").json()
    assert [s[1] for s in _shifts(vid)] == [date(2026, 10, 5), date(2026, 10, 13)]


def test_day_first_and_month_first_dates_in_one_file_are_refused():
    c, h, vid = _world()
    _amy(c, h, vid)
    r = _roster(c, h, vid, "Employee,Date,Start,End\nAmy Lee,13/10/2026,17:00,23:00\n"
                           "Amy Lee,05/10/2026,17:00,23:00\nAmy Lee,10/14/2026,17:00,23:00\n")
    assert r.status_code == 422
    msg = r.json()["error"]["message"]
    assert "13/10/2026" in msg and "line 2" in msg
    assert "10/14/2026" in msg and "line 4" in msg
    assert _shifts(vid) == []


def test_ambiguous_dates_stay_day_first():
    c, h, vid = _world()
    _amy(c, h, vid)
    out = _roster(c, h, vid, "Employee,Date,Start,End\nAmy Lee,05/10/2026,17:00,23:00\n").json()
    assert out["date_order"] == "day/month"
    assert [s[1] for s in _shifts(vid)] == [date(2026, 10, 5)]


# --- [8] re-paste is the truth -------------------------------------------------------

def _jo_al(c, h, vid):
    r = _staff(c, h, vid, f"Name,Email,Pay Rate,Employment Type\nJo Smith,jo@{DOM},30,Casual\n"
                          f"Al Brown,al@{DOM},30,Casual\n")
    assert r.status_code == 200, r.text


def test_repaste_after_a_shift_moved_replaces_it():
    c, h, vid = _world()
    _jo_al(c, h, vid)
    _roster(c, h, vid, "Name,Date,Start,End\nJo Smith,09/10/2026,17:00,22:00\nAl Brown,10/10/2026,10:00,16:00\n")
    out = _roster(c, h, vid, "Name,Date,Start,End\nJo Smith,09/10/2026,18:00,23:00\n").json()
    assert _shifts(vid) == [("Al Brown", date(2026, 10, 10), "10:00", "16:00"),
                            ("Jo Smith", date(2026, 10, 9), "18:00", "23:00")]
    assert out["shifts_replaced"] == 1 and out["shifts_removed"] == 0
    row = _rows_by_line(out)[2]
    assert row["status"] == "replaced" and "17:00" in row["reason"] and "22:00" in row["reason"]
    wk = get_db().get_roster(f"imported-{vid}-2026-10-05")
    emps = {e.id: e for e in get_db().get_employees(vid)}
    assert wk.total_cost == calculate_roster_cost(wk, emps, State.wa)


def test_repaste_is_the_truth_for_each_person_on_the_dates_it_covers():
    c, h, vid = _world()
    _jo_al(c, h, vid)
    _roster(c, h, vid, "Name,Date,Start,End\nJo Smith,09/10/2026,17:00,22:00\n"
                       "Jo Smith,10/10/2026,17:00,22:00\nAl Brown,09/10/2026,10:00,16:00\n")
    jo_sat = [s for s in get_db().get_roster(f"imported-{vid}-2026-10-05").shifts
              if s.date == date(2026, 10, 10)][0].id
    # Deputy deleted Jo's Friday shift; Friday is still in the paste
    out = _roster(c, h, vid, "Name,Date,Start,End\nJo Smith,10/10/2026,17:00,22:00\n"
                             "Al Brown,09/10/2026,10:00,16:00\n").json()
    assert _shifts(vid) == [("Al Brown", date(2026, 10, 9), "10:00", "16:00"),
                            ("Jo Smith", date(2026, 10, 10), "17:00", "22:00")]
    assert out["shifts_removed"] == 1 and out["duplicates_skipped"] == 2
    assert out["removed"] == [{"name": "Jo Smith", "date": "2026-10-09", "start": "17:00", "end": "22:00"}]
    assert jo_sat in {s.id for s in get_db().get_roster(f"imported-{vid}-2026-10-05").shifts}

    # A paste covering other days, or not naming someone, leaves their shifts alone
    _roster(c, h, vid, "Name,Date,Start,End\nJo Smith,11/10/2026,12:00,18:00\n")
    assert len(_shifts(vid)) == 3

    # A row that couldn't be read never costs the person their existing shift
    out = _roster(c, h, vid, "Name,Date,Start,End\nJo Smith,10/10/2026,18:00,huh\n"
                             "Al Brown,10/10/2026,10:00,16:00\n").json()
    assert ("Jo Smith", date(2026, 10, 10), "17:00", "22:00") in _shifts(vid)
    assert out["shifts_removed"] == 0


def test_split_shifts_survive_a_repaste():
    c, h, vid = _world()
    _jo_al(c, h, vid)
    split = "Name,Date,Start,End\nJo Smith,09/10/2026,10:00,14:00\nJo Smith,09/10/2026,18:00,22:00\n"
    assert _roster(c, h, vid, split).json()["shifts_imported"] == 2
    again = _roster(c, h, vid, split).json()
    assert again["duplicates_skipped"] == 2 and again["shifts_replaced"] == 0
    lunch = [s.id for s in get_db().get_roster(f"imported-{vid}-2026-10-05").shifts if s.start_time == dtime(10)]
    out = _roster(c, h, vid, "Name,Date,Start,End\nJo Smith,09/10/2026,10:00,14:00\n"
                             "Jo Smith,09/10/2026,18:00,23:00\n").json()
    assert _shifts(vid) == [("Jo Smith", date(2026, 10, 9), "10:00", "14:00"),
                            ("Jo Smith", date(2026, 10, 9), "18:00", "23:00")]
    assert out["shifts_replaced"] == 1 and out["shifts_removed"] == 0
    assert lunch[0] in {s.id for s in get_db().get_roster(f"imported-{vid}-2026-10-05").shifts}


def test_import_never_touches_generated_or_published_rosters():
    c, h, vid = _world()
    _jo_al(c, h, vid)
    db = get_db()
    jo = _emps(vid)["Jo Smith"].id
    monday = date(2026, 10, 5)
    gen = Shift(id=f"{vid}-g1", employee_id=jo, date=date(2026, 10, 9), start_time=dtime(17),
                end_time=dtime(22), break_minutes=0, status=ShiftStatus.scheduled, role="bar")
    db.save_roster(Roster(id=f"{vid}-gen", venue_id=vid, week_start=monday, week_end=monday + timedelta(days=6),
                          shifts=[gen], total_cost=None, created_at=datetime.utcnow()))
    db.update_roster_state(f"{vid}-gen", "published", "seed", "test")
    _roster(c, h, vid, "Name,Date,Start,End\nJo Smith,09/10/2026,18:00,23:00\n")
    kept = db.get_roster(f"{vid}-gen")
    assert [(s.id, s.start_time) for s in kept.shifts] == [(f"{vid}-g1", dtime(17))]
    assert db.get_roster_state(f"{vid}-gen") == "published"

    # An imported week that has been published in RosterIQ is left as published
    rid = f"imported-{vid}-{monday.isoformat()}"
    db.update_roster_state(rid, "published", "seed", "test")
    out = _roster(c, h, vid, "Name,Date,Start,End\nJo Smith,09/10/2026,12:00,16:00\n").json()
    assert [(s.start_time, s.end_time) for s in db.get_roster(rid).shifts] == [(dtime(18), dtime(23))]
    assert out["shifts_imported"] == 0 and "published" in _rows_by_line(out)[2]["reason"]


# --- [9] long rate cell ------------------------------------------------------------

def test_reading_and_costing_a_paste_run_off_the_event_loop(monkeypatch):
    where = []
    for mod, fn in ((staff_import, "_read_staff_paste"), (roster_import, "_read_roster_paste"),
                    (roster_import, "_reconcile")):
        real = getattr(mod, fn)

        def spy(*a, _real=real, _fn=fn, **kw):
            try:
                asyncio.get_running_loop()
                where.append((_fn, "event loop"))
            except RuntimeError:
                where.append((_fn, "worker thread"))
            return _real(*a, **kw)
        monkeypatch.setattr(mod, fn, spy)
    c, h, vid = _world()
    _jo_al(c, h, vid)
    assert _roster(c, h, vid, "Name,Date,Start,End\nJo Smith,09/10/2026,17:00,22:00\n").json()["shifts_imported"] == 1
    assert where == [("_read_staff_paste", "worker thread"), ("_read_roster_paste", "worker thread"),
                     ("_reconcile", "worker thread")]


def test_a_long_rate_cell_returns_quickly():
    c, h, vid = _world()
    content = 'Name,Pay Rate\nJo Smith,"' + "7 " * 50_000 + '"\n'        # a 100 KB cell
    t0 = time.monotonic()
    r = _staff(c, h, vid, content)
    assert time.monotonic() - t0 < 3
    assert r.status_code == 200, r.text
    assert "rate_unclear" in r.json()["rows"][0]["flags"]
    assert _emps(vid)["Jo Smith"].hourly_base_rate == Decimal("31.50")
    # The level number is still never the rate
    c, h, vid = _world()
    _staff(c, h, vid, f"Name,Email,Pay Rate\nL Two,l2@{DOM},Level 2 28\nL Dash,ld@{DOM},Grade - 3 29.10\n")
    assert _emps(vid)["L Two"].hourly_base_rate == Decimal("28.00")
    assert _emps(vid)["L Dash"].hourly_base_rate == Decimal("29.10")


# --- [10] visa expiry ------------------------------------------------------------

def test_visa_expiry_reads_common_formats_and_flags_the_rest():
    c, h, vid = _world()
    cells = ["15-Mar-2027", "15/03/27", "46461", "2027-03-15 00:00:00", "15 March 2027",
             "15/03/2027 12:00 AM", "Mon 15/03/2027", "2027/03/15", "15.03.2027"]
    lines = ["Name,Email,Visa Status,Visa Expiry"]
    lines += [f"V {i},v{i}@{DOM},Student,{v}" for i, v in enumerate(cells)]
    lines.append(f"V Bad,vb@{DOM},Student,soon")
    out = _staff(c, h, vid, "\n".join(lines) + "\n").json()
    emps = _emps(vid)
    for i, v in enumerate(cells):
        assert emps[f"V {i}"].visa_expiry == date(2027, 3, 15), v
        assert "visa_expiry_unreadable" not in _rows_by_line(out)[i + 2]["flags"], v
    assert emps["V Bad"].visa_expiry is None
    bad = _rows_by_line(out)[len(cells) + 2]
    assert "visa_expiry_unreadable" in bad["flags"] and "'soon'" in " ".join(bad["notes"])
    assert out["flags_summary"]["visa_expiry_unreadable"] == 1


def test_visa_expiry_column_order_is_decided_once():
    c, h, vid = _world()
    out = _staff(c, h, vid, f"Name,Email,Visa Expiry\nA B,ab@{DOM},03/15/2027\nC D,cd@{DOM},04/01/2027\n").json()
    emps = _emps(vid)
    assert emps["A B"].visa_expiry == date(2027, 3, 15)
    assert emps["C D"].visa_expiry == date(2027, 4, 1)
    c, h, vid = _world()
    out = _staff(c, h, vid, f"Name,Email,Visa Expiry\nA B,ab@{DOM},03/15/2027\nC D,cd@{DOM},15/03/2027\n"
                            f"E F,ef@{DOM},04/01/2027\n").json()
    assert all(e.visa_expiry is None for e in get_db().get_employees(vid))
    assert out["flags_summary"]["visa_expiry_unreadable"] == 3


# --- [11] pay figures --------------------------------------------------------------

def test_k_suffix_and_comma_decimal_thousands():
    c, h, vid = _world()
    out = _staff(c, h, vid, f"Name,Email,Salary\nK One,k1@{DOM},$78k\nK Two,k2@{DOM},95K\n"
                            f"K Three,k3@{DOM},\"$78,000\"\n").json()
    emps = _emps(vid)
    assert emps["K One"].hourly_base_rate == Decimal("39.47")        # 78000 / 52 / 38
    assert emps["K Two"].hourly_base_rate == Decimal("48.08")        # 95000 / 52 / 38
    assert emps["K Three"].hourly_base_rate == Decimal("39.47")
    assert out["flags_summary"]["salaried"] == 3

    c, h, vid = _world()
    out = _staff(c, h, vid, f"Name;Email;Weekly Wage\nE One;e1@{DOM};1.250,00\nE Two;e2@{DOM};1250\n").json()
    emps = _emps(vid)
    assert emps["E One"].hourly_base_rate == Decimal("32.89")        # 1250 / 38
    assert emps["E Two"].hourly_base_rate == Decimal("32.89")
    assert out["flags_summary"]["salaried"] == 2


def test_figures_that_cant_be_the_columns_unit_or_hourly_pay_are_flagged():
    c, h, vid = _world()
    out = _staff(c, h, vid, f"Name,Email,Salary\nS One,s1@{DOM},$120\nS Two,s2@{DOM},31.50\n").json()
    rows = _rows_by_line(out)
    for line in (2, 3):
        assert "rate_unclear" in rows[line]["flags"], rows[line]
    assert "year" in " ".join(rows[3]["notes"])
    assert _emps(vid)["S Two"].hourly_base_rate == Decimal("31.50")

    c, h, vid = _world()
    out = _staff(c, h, vid, f"Name,Email,Pay Rate\nLow Rate,lo@{DOM},3.50\nHigh Rate,hi@{DOM},$120\n"
                            f"Fine Rate,ok@{DOM},$31.58\n").json()
    rows = _rows_by_line(out)
    assert "rate_unclear" in rows[2]["flags"] and "rate_unclear" in rows[3]["flags"]
    assert "rate_unclear" not in rows[4]["flags"]

    # Updating from a figure that needs review keeps the rate on record
    out = _staff(c, h, vid, f"Name,Email,Pay Rate\nFine Rate,ok@{DOM},$320\n", update_existing=True).json()
    assert _emps(vid)["Fine Rate"].hourly_base_rate == Decimal("31.58")
    out = _staff(c, h, vid, f"Name,Email,Pay Rate\nFine Rate,ok@{DOM},$101\n", update_existing=True).json()
    assert _emps(vid)["Fine Rate"].hourly_base_rate == Decimal("31.58")
    assert "rate_unclear" in _rows_by_line(out)[2]["flags"]


# --- [13] unclosed quotes ----------------------------------------------------------

def _seventy(c, h, vid):
    rows = ["Name,Email,Pay Rate"] + [f"Staff{i} Person,s{i}@{DOM},30" for i in range(70)]
    assert _staff(c, h, vid, "\n".join(rows) + "\n").json()["created_count"] == 70


def test_an_unclosed_quote_past_the_field_limit_is_a_422_naming_the_line():
    c, h, vid = _world()
    _seventy(c, h, vid)
    lines = ["Name,Date,Start,End,Area,Notes",
             'Staff0 Person,12/10/2026,17:00,23:00,Bar,"Covering for Sam']
    lines += [f"Staff{i % 70} Person,{12 + i % 7}/10/2026,17:00,23:00,Bar,ok" for i in range(3500)]
    r = _roster(c, h, vid, "\n".join(lines) + "\n")
    assert r.status_code == 422, r.text[:300]
    msg = r.json()["error"]["message"]
    assert "line 2" in msg.lower() and "quote" in msg.lower()
    assert _shifts(vid) == []


def test_an_unclosed_quote_that_swallows_lines_is_reported_not_imported():
    c, h, vid = _world()
    _seventy(c, h, vid)
    lines = ["Name,Date,Start,End,Area,Notes",
             'Staff0 Person,12/10/2026,17:00,23:00,Bar,"Covering for Sam']
    lines += [f"Staff{i % 70} Person,{12 + i % 7}/10/2026,17:00,23:00,Bar,ok" for i in range(1500)]
    out = _roster(c, h, vid, "\n".join(lines) + "\n").json()
    assert out["shifts_imported"] == 0
    row = _rows_by_line(out)[2]
    assert row["status"] == "skipped" and row["reason"].startswith("unclosed quote at line 2")
    assert _shifts(vid) == []

    c, h, vid = _world()
    rows = ["Name,Email,Pay Rate,Notes"]
    rows += [f"Staff{i} Person,s{i}@{DOM},30," + ('"Prefers bar' if i == 3 else "ok") for i in range(70)]
    out = _staff(c, h, vid, "\n".join(rows) + "\n").json()
    assert out["created_count"] == 3
    row = _rows_by_line(out)[5]
    assert row["status"] == "skipped" and row["reason"].startswith("unclosed quote at line 5")

    # A note over several lines, short or long, is a real cell
    c, h, vid = _world()
    long_note = "Opening shift:\n" + "\n".join(f"- check the {x} and sign the sheet" for x in
                                               ("cellar", "tills, floats", "kegs", "deliveries, stock",
                                                "fridges", "glass washer", "lights"))
    assert len(long_note) > 200
    out = _staff(c, h, vid, f'Name,Email,Notes\nJo Smith,jo@{DOM},"Allergic to nuts\nPrefers bar"\n'
                            f'Al Brown,al@{DOM},"{long_note}"\n').json()
    assert out["created_count"] == 2
    out = _roster(c, h, vid, f'Name,Date,Start,End,Notes\nJo Smith,12/10/2026,17:00,23:00,"{long_note}"\n').json()
    assert out["shifts_imported"] == 1


# --- [16] implausible spans -----------------------------------------------------

def test_implausible_spans_are_skipped_and_ambiguous_12_hour_times_flagged():
    c, h, vid = _world()
    _jo_al(c, h, vid)
    out = _roster(c, h, vid, "Name,Date,Start,End,Break\n"
                             "Jo Smith,13/10/2026,9:00,5:00,30\n"
                             "Jo Smith,14/10/2026,9:00,9:00,0\n"
                             "Jo Smith,15/10/2026,9:00,17:00,30\n"
                             "Jo Smith,16/10/2026,11:00,3:00,30\n"
                             "Jo Smith,17/10/2026,18:00,2:00,30\n").json()
    assert out["shifts_imported"] == 3
    rows = _rows_by_line(out)
    assert rows[2]["status"] == "skipped"
    assert "20-hour" in rows[2]["reason"] and "ambiguous 12-hour time" in rows[2]["reason"]
    assert rows[3]["status"] == "skipped" and "09:00" in rows[3]["reason"]
    assert [(f["line"], f["flag"]) for f in out["flagged"]] == [(5, "ambiguous_12h")]
    assert "ambiguous 12-hour time" in out["flagged"][0]["detail"]
    assert [s[1] for s in _shifts(vid)] == [date(2026, 10, 15), date(2026, 10, 16), date(2026, 10, 17)]

    # With am/pm in the file there is nothing to guess
    c, h, vid = _world()
    _jo_al(c, h, vid)
    out = _roster(c, h, vid, "Name,Date,Start,End\nJo Smith,13/10/2026,11:00 AM,3:00 AM\n"
                             "Jo Smith,14/10/2026,9:00 AM,5:00 PM\n").json()
    assert out["shifts_imported"] == 2 and out["flagged"] == []
