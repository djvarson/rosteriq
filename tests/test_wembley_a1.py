"""
Paste importers against Wembley-shaped Deputy exports (synthetic data; the
rates below are made-up venue records, not award values).

Staff import: salaries/weekly pay converted to a flagged hourly planning
figure, first money token only, defaulted rates flagged, First+Last names
never doubled, a BOM on the first header, widened header aliases, skills
split + lower-cased (Position included), AU mobiles normalised, junk/shared
emails, award levels, a two-location export, and update-existing re-imports.

Roster import: re-paste dedupe, HH:MM:SS / combined date-time / 28-Sep-2026 /
weekday-prefixed dates, First+Last names, meal-break formats, two staff with
the same name, location filtering, and imported weeks costed.
"""

import csv
import inspect
import io
import uuid
from datetime import date, time
from decimal import Decimal

from fastapi.testclient import TestClient

from rosteriq.api import app
from rosteriq.cost_calculator import calculate_roster_cost, calculate_shift_cost_breakdown
from rosteriq.database import PostgresStore, get_db
from rosteriq.models import Roster, State

WEMBLEY = "The Wembley Hotel"
APOTHECARY = "Apothecary Murdoch Square"


def _owner(c):
    email = f"wa1{uuid.uuid4().hex[:8]}@wembley-synthetic.com.au"
    c.post("/api/auth/register", json={"email": email, "password": "Passw0rd!234", "name": "Ben"})
    tok = c.post("/api/auth/login", json={"email": email, "password": "Passw0rd!234"}).json()["access_token"]
    return {"Authorization": f"Bearer {tok}"}


def _venue(c, h, name=WEMBLEY):
    vid = f"wa1-{uuid.uuid4().hex[:8]}"
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
    r = c.post("/api/setup/import-staff", json={"venue_id": vid, "content": content, **extra}, headers=h)
    return r


def _roster(c, h, vid, content, **extra):
    return c.post("/api/setup/import-roster", json={"venue_id": vid, "content": content, **extra}, headers=h)


def _emps(vid):
    return {e.name: e for e in get_db().get_employees(vid)}


def _rows_by_name(out):
    return {r["name"]: r for r in out["rows"] if r.get("name")}


# --- Fixtures ----------------------------------------------------------------

_PEOPLE = [
    # first, last, email, mobile, basis, access, location, position, rate, training
    ("Ben", "Ingram", "ben.ingram@wembley-synthetic.com.au", "412 345 001", "Full Time",
     "Location Manager", WEMBLEY, "Venue Manager", "$78,000.00 p.a.",
     "Restaurant, Front Bar, Sports Bar, Bottle Shop, Functions"),
    ("Sarah", "Delaney", "sarah.delaney@wembley-synthetic.com.au", "0412 345 002", "Full Time",
     "Location Manager", WEMBLEY, "Assistant Venue Manager", "$1,250.00 /wk",
     "Front Bar; Sports Bar; Functions"),
    ("Tom", "Okafor", "tom.okafor@wembley-synthetic.com.au", "+61 412 345 003", "Full Time",
     "Location Manager", WEMBLEY, "Assistant Venue Manager", "$1,250.00",
     "Front Bar, Restaurant"),
    ("Meca", "Tran", "meca.tran@wembley-synthetic.com.au", "412345004", "Casual",
     "Supervisor", WEMBLEY, "Duty Manager", "$38.90", "Front Bar | Restaurant"),
    ("Ella", "Rhodes", "ella.rhodes@wembley-synthetic.com.au", "0412 345 005", "Casual",
     "Employee", WEMBLEY, "Floor Runner", "$22.11", "Restaurant"),
    ("Kai", "Nguyen", "kai.nguyen@wembley-synthetic.com.au", "0412 345 006", "Casual",
     "Employee", WEMBLEY, "Bar Attendant", "24.95", "Front Bar; Sports Bar"),
    ("Leah", "Fontaine", "leah.fontaine@wembley-synthetic.com.au", "", "Part Time",
     "Supervisor", WEMBLEY, "Restaurant Manager", "33.50", "Restaurant/Functions"),
]


def deputy_people_csv(people=_PEOPLE, bom=True, location_values=None):
    """Deputy 'People' export shape: Display Name + First/Last, quoted cells,
    Excel BOM + CRLF, Employment Basis, Base Pay Rate, Training, Mobile."""
    out = io.StringIO()
    w = csv.writer(out, quoting=csv.QUOTE_ALL, lineterminator="\r\n")
    w.writerow(["Display Name", "First Name", "Last Name", "Email", "Mobile", "Date of Birth",
                "Start Date", "Employment Basis", "Access Level", "Main Location", "Position",
                "Base Pay Rate", "Training"])
    for i, (f, l, em, mob, basis, access, loc, pos, rate, training) in enumerate(people):
        if location_values:
            loc = location_values[i]
        w.writerow([f"{f} {l}", f, l, em, mob, "01/01/1990", "01/02/2024", basis, access,
                    loc, pos, rate, training])
    return (BOM_CHAR if bom else "") + out.getvalue()


BOM_CHAR = "\ufeff"


def excel_tsv():
    """Pasted straight out of Excel: tabs, an 'Employee' column, award levels."""
    return "\n".join([
        "Employee\tEmail Address\tPhone Number\tHourly Rate\tArea\tEmployment Status\tAward Level",
        "Jordan Pike\tjordan.pike@wembley-synthetic.com.au\t0412 111 001\t$42.15\tFront Bar, Sports Bar\tCasual\tLevel 5",
        "Nina Calder\tnina.calder@wembley-synthetic.com.au\t412111002\t$36.12\tRestaurant\tCasual\tL3",
        "Max Kelly\tmax.kelly@wembley-synthetic.com.au\t0412 111 003\t23.40\tBottle Shop\tCasual\tIntroductory",
        "Ivy Shaw\tivy.shaw@wembley-synthetic.com.au\t0412 111 004\t25.10\tSports Bar\tCasual\t2",
        "Finn Reid\tfinn.reid@wembley-synthetic.com.au\t0412 111 005\t31.58\tKitchen\tFull Time\tCook",
    ])


# --- Staff: rates -----------------------------------------------------------

def test_salary_and_weekly_rates_become_flagged_planning_figures():
    c, h, vid = _world()
    r = _staff(c, h, vid, deputy_people_csv())
    assert r.status_code == 200, r.text
    out = r.json()
    emps = _emps(vid)
    # $78,000 a year -> 78000 / 52 / 38 = $39.47/h, never $78,000/h
    assert emps["Ben Ingram"].hourly_base_rate == Decimal("39.47")
    # $1,250 a week -> 1250 / 38 = $32.89/h
    assert emps["Sarah Delaney"].hourly_base_rate == Decimal("32.89")
    rows = _rows_by_name(out)
    assert "rate_converted" in rows["Ben Ingram"]["flags"]
    assert rows["Ben Ingram"]["rate_original"] == "$78,000.00 p.a."
    note = " ".join(rows["Ben Ingram"]["notes"]).lower()
    assert "planning" in note and "advisor" in note and "$78,000.00 p.a." in note
    # A bare $1,250 with no unit is not guessed at: default + flag
    assert emps["Tom Okafor"].hourly_base_rate == Decimal("31.50")
    assert "rate_unclear" in rows["Tom Okafor"]["flags"]
    # Junior rates the venue recorded are kept exactly
    assert emps["Ella Rhodes"].hourly_base_rate == Decimal("22.11")
    assert emps["Kai Nguyen"].hourly_base_rate == Decimal("24.95")
    assert out["flags_summary"]["rate_converted"] == 2
    assert out["flags_summary"]["rate_unclear"] == 1
    # Nobody is stored above a plausible hourly figure
    assert all(e.hourly_base_rate < 150 for e in emps.values())


def test_first_money_token_and_odd_rate_text():
    c, h, vid = _world()
    content = (
        "Name,Email,Pay Rate\n"
        "Rate Word,rw@wembley-synthetic.com.au,Level 2 - $28.10\n"
        "Rate Range,rr@wembley-synthetic.com.au,$28.10 - $31.58\n"
        "Rate Euro,re@wembley-synthetic.com.au,\"31,58\"\n"
        "Rate Salary,rs@wembley-synthetic.com.au,\"$65,000 salary\"\n"
        "Rate Annum,ran@wembley-synthetic.com.au,52000 per annum\n"
        "Rate Weekly,rwk@wembley-synthetic.com.au,\"$1,140.00 weekly\"\n"
        "Rate Hourly,rh@wembley-synthetic.com.au,$29.80/hr\n"
        "Rate Blank,rb@wembley-synthetic.com.au,\n"
        "Rate Huge,rhg@wembley-synthetic.com.au,1250\n"
        "Rate Level,rl@wembley-synthetic.com.au,L2 28\n"
    )
    out = _staff(c, h, vid, content).json()
    emps = _emps(vid)
    rate = {n: e.hourly_base_rate for n, e in emps.items()}
    assert rate["Rate Word"] == Decimal("28.10")
    assert rate["Rate Range"] == Decimal("28.10")
    assert rate["Rate Euro"] == Decimal("31.58")
    assert rate["Rate Salary"] == Decimal("32.89")     # 65000 / 52 / 38
    assert rate["Rate Annum"] == Decimal("26.32")      # 52000 / 52 / 38
    assert rate["Rate Weekly"] == Decimal("30.00")     # 1140 / 38
    assert rate["Rate Hourly"] == Decimal("29.80")
    assert rate["Rate Blank"] == Decimal("31.50")
    assert rate["Rate Huge"] == Decimal("31.50")
    assert rate["Rate Level"] == Decimal("28.00")      # the level number is not the rate
    rows = _rows_by_name(out)
    assert "rate_defaulted" in rows["Rate Blank"]["flags"]
    assert "rate_unclear" in rows["Rate Huge"]["flags"]
    for n in ("Rate Salary", "Rate Annum", "Rate Weekly"):
        assert "rate_converted" in rows[n]["flags"], n
    assert not rows["Rate Hourly"]["flags"] or "rate_converted" not in rows["Rate Hourly"]["flags"]


def test_annual_salary_header_converts_and_unknown_rate_header_defaults_every_row():
    c, h, vid = _world()
    out = _staff(c, h, vid, "Name,Email,Annual Salary\nDean Murray,dean@wembley-synthetic.com.au,\"78,000\"\n").json()
    assert _emps(vid)["Dean Murray"].hourly_base_rate == Decimal("39.47")
    assert "rate_converted" in out["rows"][0]["flags"]

    c, h, vid = _world()
    out = _staff(c, h, vid, "Name,Email,Remuneration\nA One,a1@wembley-synthetic.com.au,28.00\n"
                            "B Two,b2@wembley-synthetic.com.au,29.00\n").json()
    assert out["flags_summary"]["rate_defaulted"] == 2
    assert all("rate_defaulted" in r["flags"] for r in out["rows"])
    assert "Remuneration" in out["unmapped_columns"]


def test_base_pay_rate_and_employment_basis_headers_are_recognised():
    c, h, vid = _world()
    content = ("First Name,Last Name,Email,Employment Basis,Base Pay Rate\n"
               "Priya,Shah,priya@wembley-synthetic.com.au,Full Time,33.90\n"
               "Ella,Rhodes,ella@wembley-synthetic.com.au,Casual,22.11\n")
    out = _staff(c, h, vid, content).json()
    emps = _emps(vid)
    assert emps["Priya Shah"].employment_type.value == "full_time"
    assert emps["Priya Shah"].hourly_base_rate == Decimal("33.90")
    assert emps["Ella Rhodes"].hourly_base_rate == Decimal("22.11")
    assert out["column_map"]["Base Pay Rate"] == "rate"
    assert out["column_map"]["Employment Basis"] == "type"
    assert "rate_defaulted" not in out["flags_summary"]


def test_other_rate_and_type_aliases():
    for rate_h, type_h in [("Base Hourly Rate", "Employment Status"), ("Ordinary Rate", "Contract Type"),
                           ("Rate (hourly)", "Employment Basis")]:
        c, h, vid = _world()
        out = _staff(c, h, vid, f"Name,Email,{rate_h},{type_h}\nX Y,xy@wembley-synthetic.com.au,27.40,Part Time\n").json()
        e = _emps(vid)["X Y"]
        assert e.hourly_base_rate == Decimal("27.40"), rate_h
        assert e.employment_type.value == "part_time", type_h
        assert out["unmapped_columns"] == []


# --- Staff: names, BOM, column report ------------------------------------------

def test_display_name_plus_first_last_never_doubles_surname_and_bom_is_stripped():
    c, h, vid = _world()
    out = _staff(c, h, vid, deputy_people_csv(bom=True)).json()
    names = set(_emps(vid))
    assert "Ben Ingram" in names and "Sarah Delaney" in names
    assert not any(n.endswith("Ingram Ingram") for n in names)
    assert out["created_count"] == len(_PEOPLE)
    # The BOM'd first header still maps (and is reported clean)
    assert out["column_map"].get("Display Name") == "name"
    assert out["column_map"]["First Name"] == "first_name"
    assert out["column_map"]["Last Name"] == "last_name"
    for col in ("Date of Birth", "Start Date", "Access Level"):
        assert col in out["unmapped_columns"]
    assert "Mobile" in out["column_map"] and "Training" in out["column_map"]


def test_name_column_plus_surname_appends_once():
    c, h, vid = _world()
    content = ("Name,Surname,Email\nBen,Ingram,ben@wembley-synthetic.com.au\n"
               "Sarah Delaney,Delaney,sarah@wembley-synthetic.com.au\n")
    _staff(c, h, vid, content)
    assert set(_emps(vid)) == {"Ben Ingram", "Sarah Delaney"}


# --- Staff: skills, phones, emails, levels ------------------------------------

def test_training_areas_split_lowercased_and_position_included():
    c, h, vid = _world()
    _staff(c, h, vid, deputy_people_csv())
    emps = _emps(vid)
    assert emps["Sarah Delaney"].skills == ["front bar", "sports bar", "functions", "assistant venue manager"]
    assert emps["Meca Tran"].skills == ["front bar", "restaurant", "duty manager"]
    assert emps["Leah Fontaine"].skills == ["restaurant/functions", "restaurant manager"]
    assert emps["Ben Ingram"].skills[:5] == ["restaurant", "front bar", "sports bar", "bottle shop", "functions"]


def test_skill_cells_dedupe_and_aliases():
    for hdr in ("Training", "Training Areas", "Areas", "Skills", "Locations Trained"):
        c, h, vid = _world()
        _staff(c, h, vid, f"Name,Email,{hdr}\nJack Kelly,jack@wembley-synthetic.com.au,"
                          "\"Front Bar; front bar | Bottle Shop, SPORTS  BAR\"\n")
        assert _emps(vid)["Jack Kelly"].skills == ["front bar", "bottle shop", "sports bar"], hdr


def test_mobiles_normalised_including_excel_dropped_zero():
    c, h, vid = _world()
    _staff(c, h, vid, deputy_people_csv())
    emps = _emps(vid)
    assert emps["Ben Ingram"].phone == "0412 345 001"      # Excel dropped the 0
    assert emps["Tom Okafor"].phone == "0412 345 003"      # +61 accepted
    assert emps["Meca Tran"].phone == "0412 345 004"
    assert emps["Leah Fontaine"].phone is None
    c, h, vid = _world()
    out = _staff(c, h, vid, excel_tsv()).json()
    assert _emps(vid)["Nina Calder"].phone == "0412 111 002"
    assert out["column_map"]["Phone Number"] == "phone"


def test_phone_aliases_and_unreadable_numbers():
    for hdr in ("Mobile", "Phone", "Mobile Phone", "Phone Number", "Contact Number", "Cell"):
        c, h, vid = _world()
        _staff(c, h, vid, f"Name,Email,{hdr}\nJack Kelly,jack@wembley-synthetic.com.au,0400 111 222\n")
        assert _emps(vid)["Jack Kelly"].phone == "0400 111 222", hdr
    c, h, vid = _world()
    out = _staff(c, h, vid, "Name,Email,Mobile\nA B,ab@wembley-synthetic.com.au,4.12346E+08\n"
                            "C D,cd@wembley-synthetic.com.au,(08) 9381 1234\n"
                            "E F,ef@wembley-synthetic.com.au,+44 7911 123456\n").json()
    emps = _emps(vid)
    assert emps["A B"].phone is None
    assert "phone_invalid" in _rows_by_name(out)["A B"]["flags"]
    assert emps["C D"].phone == "08 9381 1234"
    assert emps["E F"].phone == "+447911123456"


def test_junk_and_shared_emails():
    c, h, vid = _world()
    content = ("Name,Email,Hourly Rate\n"
               "Trial One,jobs@wembley-synthetic.com.au,25.27\n"
               "Trial Two,jobs@wembley-synthetic.com.au,25.27\n"
               "Trial Three,n/a,25.27\n"
               "Trial Four, Spaced@Example.COM ,25.27\n"
               "Trial Five,0412 345 678,25.27\n")
    out = _staff(c, h, vid, content).json()
    assert out["created_count"] == 5
    emps = _emps(vid)
    assert emps["Trial One"].email is None and emps["Trial Two"].email is None
    assert emps["Trial Three"].email is None and emps["Trial Five"].email is None
    assert emps["Trial Four"].email == "spaced@example.com"
    rows = _rows_by_name(out)
    assert "email_shared" in rows["Trial One"]["flags"] and "email_shared" in rows["Trial Two"]["flags"]
    assert "email_invalid" in rows["Trial Three"]["flags"]
    assert "email_invalid" in rows["Trial Five"]["flags"]
    assert out["flags_summary"]["email_shared"] == 2


def test_award_levels_mapped_or_flagged():
    c, h, vid = _world()
    out = _staff(c, h, vid, excel_tsv()).json()
    emps = _emps(vid)
    assert emps["Jordan Pike"].award_level.value == "level_5"
    assert emps["Nina Calder"].award_level.value == "level_3"
    assert emps["Max Kelly"].award_level.value == "level_1"     # Introductory, advisor to confirm
    assert emps["Ivy Shaw"].award_level.value == "level_2"
    rows = _rows_by_name(out)
    assert "level_interpreted" in rows["Max Kelly"]["flags"]
    assert "advisor" in " ".join(rows["Max Kelly"]["notes"]).lower()
    assert "level_unclear" in rows["Finn Reid"]["flags"]
    assert out["column_map"]["Award Level"] == "award_level"
    assert out["column_map"]["Employment Status"] == "type"
    # No award column at all: model default kept, every row flagged
    c, h, vid = _world()
    out = _staff(c, h, vid, deputy_people_csv()).json()
    assert out["flags_summary"]["level_not_recorded"] == len(_PEOPLE)
    for hdr, val, want in [("Classification", "Level 4", "level_4"), ("Level", "L6", "level_6"),
                           ("Pay Level", "3", "level_3")]:
        c, h, vid = _world()
        _staff(c, h, vid, f"Name,Email,{hdr}\nX Y,xy@wembley-synthetic.com.au,{val}\n")
        assert _emps(vid)["X Y"].award_level.value == want, hdr


def test_response_contract_rows_and_counts():
    c, h, vid = _world()
    out = _staff(c, h, vid, "Name,Email,Rate\nA B,ab@wembley-synthetic.com.au,30\n,x@wembley-synthetic.com.au,30\n").json()
    for k in ("status", "created_count", "skipped_count", "created", "skipped", "note",
              "updated_count", "updated", "rows", "flags_summary", "unmapped_columns",
              "column_map", "location"):
        assert k in out, k
    assert out["location"] is None
    created = [r for r in out["rows"] if r["status"] == "created"]
    skipped = [r for r in out["rows"] if r["status"] == "skipped"]
    assert len(created) == 1 and len(skipped) == 1
    assert created[0]["line"] == 2 and created[0]["email"] == "ab@wembley-synthetic.com.au"
    assert skipped[0]["line"] == 3 and skipped[0]["reason"] == "no name"
    assert isinstance(created[0]["flags"], list)


# --- Staff: two locations -----------------------------------------------------

def _two_location_people():
    locs = [WEMBLEY] * len(_PEOPLE)
    locs[4] = APOTHECARY                         # Ella works only at Apothecary
    locs[5] = f"{WEMBLEY}; {APOTHECARY}"         # Kai is shared
    return deputy_people_csv(location_values=locs)


def test_two_location_staff_export_keeps_only_this_venue():
    c, h, vid = _world()
    out = _staff(c, h, vid, _two_location_people()).json()
    names = set(_emps(vid))
    assert "Ella Rhodes" not in names
    assert "Kai Nguyen" in names                  # shared staff are kept
    assert out["location"] == {"column": "Main Location", "kept": WEMBLEY, "skipped_other": 1}
    ella = _rows_by_name(out)["Ella Rhodes"]
    assert ella["status"] == "skipped" and ella["reason"] == f"other location: {APOTHECARY}"


def test_location_named_in_body_and_ambiguous_export_asks():
    c, h, vid = _world(name="Apothecary at Murdoch Square")
    out = _staff(c, h, vid, _two_location_people()).json()
    assert set(_emps(vid)) == {"Ella Rhodes", "Kai Nguyen"}
    assert out["location"]["kept"] == APOTHECARY

    c, h, vid = _world(name="Ben's Group")
    r = _staff(c, h, vid, _two_location_people())
    assert r.status_code == 422 and APOTHECARY in str(r.json())
    assert get_db().get_employees(vid) == []
    r = _staff(c, h, vid, _two_location_people(), location=WEMBLEY)
    assert r.status_code == 200 and "Ella Rhodes" not in _emps(vid)


# --- Staff: re-import ---------------------------------------------------------

def test_reimport_skips_unless_update_existing_then_updates_in_this_venue_only():
    c, h, vid = _world()
    other = _venue(c, h, name=APOTHECARY)
    first = ("First Name,Last Name,Email,Mobile,Pay Rate,Training\n"
             "Meca,Tran,meca@wembley-synthetic.com.au,,38.90,Front Bar\n"
             "Kai,Nguyen,kai@wembley-synthetic.com.au,0412 345 006,24.95,Sports Bar\n")
    _staff(c, h, vid, first)
    _staff(c, h, other, first)                   # shared staff record at Apothecary
    second = ("First Name,Last Name,Email,Mobile,Pay Rate,Training,Award Level\n"
              "Meca,Tran,MECA@wembley-synthetic.com.au,0412 999 001,40.10,\"Front Bar; Restaurant\",Level 5\n"
              "Kai,Nguyen,kai@wembley-synthetic.com.au,0412 345 006,24.95,Sports Bar,\n"
              "New,Person,new@wembley-synthetic.com.au,,31.58,Bottle Shop,\n")
    out = _staff(c, h, vid, second).json()
    assert out["created_count"] == 1 and out["updated_count"] == 0
    meca_row = _rows_by_name(out)["Meca Tran"]
    assert meca_row["status"] == "skipped"
    assert meca_row["reason"] == "already exists — tick update existing to refresh their details"
    assert _emps(vid)["Meca Tran"].hourly_base_rate == Decimal("38.90")

    out = _staff(c, h, vid, second, update_existing=True).json()
    assert out["updated_count"] == 1 and out["created_count"] == 0
    meca_row = _rows_by_name(out)["Meca Tran"]
    assert meca_row["status"] == "updated"
    assert set(meca_row["changed"]) == {"rate", "phone", "skills", "level"}
    assert out["updated"][0]["name"] == "Meca Tran"
    kai_row = _rows_by_name(out)["Kai Nguyen"]
    assert kai_row["status"] == "skipped" and kai_row["reason"] == "already up to date"
    meca = _emps(vid)["Meca Tran"]
    assert meca.hourly_base_rate == Decimal("40.10")
    assert meca.phone == "0412 999 001"
    assert meca.skills == ["front bar", "restaurant"]
    assert meca.award_level.value == "level_5"
    # The same email at the other venue is untouched
    assert _emps(other)["Meca Tran"].hourly_base_rate == Decimal("38.90")
    # Nothing duplicated
    assert len(get_db().get_employees(vid)) == 3


def test_update_existing_fixes_doubled_names_but_never_writes_defaults_over_records():
    c, h, vid = _world()
    db = get_db()
    _staff(c, h, vid, "Name,Email,Pay Rate,Employment Type\n"
                      "Ben Ingram Ingram,ben@wembley-synthetic.com.au,39.47,Full Time\n")
    out = _staff(c, h, vid, "First Name,Last Name,Email\nBen,Ingram,ben@wembley-synthetic.com.au\n",
                 update_existing=True).json()
    assert _rows_by_name(out)["Ben Ingram"]["changed"] == ["name"]
    ben = db.get_employees(vid)[0]
    assert ben.name == "Ben Ingram"
    assert ben.hourly_base_rate == Decimal("39.47")          # no rate in file: kept
    assert ben.employment_type.value == "full_time"          # no type in file: kept


def test_postgres_upsert_persists_award_level():
    src = inspect.getsource(PostgresStore.save_employee)
    assert "award_level=EXCLUDED.award_level" in src


def test_import_still_requires_full_manager_of_the_venue():
    c, h, vid = _world()
    other = f"wa1{uuid.uuid4().hex[:8]}@x.com"
    c.post("/api/auth/register", json={"email": other, "password": "Passw0rd!234", "name": "S"})
    db = get_db()
    rec = db.get_user_by_email(other)
    rec["venue_ids"] = ["not-this-one"]; rec["role"] = "manager"; db.save_user(rec)
    sh = {"Authorization": f"Bearer {c.post('/api/auth/login', json={'email': other, 'password': 'Passw0rd!234'}).json()['access_token']}"}
    r = c.post("/api/setup/import-staff", json={"venue_id": vid, "content": "Name,Email\nX,x@x.com\n",
                                                "update_existing": True}, headers=sh)
    assert r.status_code == 403
    r = c.post("/api/setup/import-roster", json={"venue_id": vid, "content": "Name,Date,Start,End\nX,03/08/2026,9,17\n"},
               headers=sh)
    assert r.status_code == 403


# --- Roster import ------------------------------------------------------------

def _roster_world(name=WEMBLEY):
    c, h, vid = _world(name)
    r = _staff(c, h, vid, deputy_people_csv())
    assert r.status_code == 200, r.text
    return c, h, vid


def deputy_schedule_csv():
    """Deputy schedule export: DD/MM/YYYY, 12h times, Area, Meal Break as H:MM,
    plus Rest Break / Total Time / Cost columns we don't use."""
    out = io.StringIO()
    w = csv.writer(out, lineterminator="\r\n")
    w.writerow(["Date", "Employee", "Location", "Area", "Start Time", "End Time",
                "Meal Break", "Rest Break", "Total Time", "Cost"])
    w.writerow(["28/09/2026", "Meca Tran", WEMBLEY, "Front Bar", "5:00 PM", "11:30 PM", "0:30", "0:10", "6", ""])
    w.writerow(["29/09/2026", "Ella Rhodes", WEMBLEY, "Restaurant", "11:00 AM", "5:00 PM", "0:30", "0:10", "5.5", ""])
    w.writerow(["03/10/2026", "Kai Nguyen", WEMBLEY, "Sports Bar", "6:00 PM", "1:00 AM", "1:00", "0:10", "6", ""])
    w.writerow(["04/10/2026", "Ben Ingram", WEMBLEY, "Restaurant", "10:00 AM", "6:00 PM", "1:00", "", "7", ""])
    return out.getvalue()


def _week(vid, d="2026-09-28"):
    return get_db().get_roster(f"imported-{vid}-{d}")


def test_repaste_does_not_double_shifts():
    c, h, vid = _roster_world()
    first = _roster(c, h, vid, deputy_schedule_csv()).json()
    assert first["shifts_imported"] == 4 and first["duplicates_skipped"] == 0
    again = _roster(c, h, vid, deputy_schedule_csv()).json()
    assert again["shifts_imported"] == 0 and again["duplicates_skipped"] == 4
    assert {r["reason"] for r in again["rows"]} == {"already imported"}
    assert len(_week(vid).shifts) == 4
    # Duplicate lines inside one paste are also caught
    dup = ("Name,Date,Start,End\nMeca Tran,06/10/2026,17:00,23:00\nMeca Tran,06/10/2026,17:00,23:00\n")
    out = _roster(c, h, vid, dup).json()
    assert out["shifts_imported"] == 1 and out["duplicates_skipped"] == 1
    assert out["rows"][0]["line"] == 3 and out["rows"][0]["status"] == "skipped"


def test_deputy_schedule_breaks_and_unmapped_columns():
    c, h, vid = _roster_world()
    out = _roster(c, h, vid, deputy_schedule_csv()).json()
    assert out["skipped_count"] == 0
    shifts = {s.employee_id: s for s in _week(vid).shifts}
    emps = _emps(vid)
    assert shifts[emps["Kai Nguyen"].id].break_minutes == 60       # '1:00' is an hour, not 100 min
    assert shifts[emps["Meca Tran"].id].break_minutes == 30
    for col in ("Rest Break", "Total Time", "Cost"):
        assert col in out["unmapped_columns"]
    assert out["column_map"]["Meal Break"] == "break"
    assert out["location"] is None


def test_break_formats():
    c, h, vid = _roster_world()
    cases = [("0:30", 30), ("1:00", 60), ("00:30:00", 30), ("0.5", 30), ("1h", 60),
             ("30m", 30), ("30 min", 30), ("45", 45), ("1.5", 90), ("1h 30m", 90)]
    lines = ["Employee,Date,Start,End,Meal Break"]
    for i, (b, _) in enumerate(cases):
        lines.append(f"Meca Tran,{5 + i:02d}/10/2026,17:00,23:30,{b}")
    lines.append("Meca Tran,20/10/2026,10:00,22:00,150")
    out = _roster(c, h, vid, "\n".join(lines)).json()
    assert out["shifts_imported"] == len(cases) + 1
    got = {}
    for r in get_db().list_rosters():
        if r.venue_id == vid:
            for s in r.shifts:
                got[s.date.day] = s.break_minutes
    for i, (b, want) in enumerate(cases):
        assert got[5 + i] == want, (b, got[5 + i])
    assert got[20] == 150                                            # flagged, not capped
    assert any(f["flag"] == "break_over_120" and f["line"] == len(cases) + 2 for f in out["flagged"])


def test_break_minutes_header_reads_small_numbers_as_minutes():
    c, h, vid = _roster_world()
    _roster(c, h, vid, "First Name\tLast Name\tDate\tStart\tFinish\tArea\tBreak (mins)\n"
                       "Meca\tTran\t28/09/2026\t17:00\t23:00\tFront Bar\t5\n")
    assert _week(vid).shifts[0].break_minutes == 5


def test_time_and_date_formats():
    c, h, vid = _roster_world()
    meca = _emps(vid)["Meca Tran"].id
    variants = [
        ("Employee Name,Date,Start,End\nMeca Tran,Mon 28/09/2026,17:00:00,23:00:00\n", date(2026, 9, 28)),
        ("Employee,Date,Start,End\nMeca Tran,29-Sep-2026,17:00,23:00\n", date(2026, 9, 29)),
        ("Employee,Date,Start,End\nMeca Tran,30-Sep-26,17:00,23:00\n", date(2026, 9, 30)),
        ("Employee,Date,Start,End\nMeca Tran,Thursday 01/10/2026,5:00 PM,11:00 PM\n", date(2026, 10, 1)),
        ("Employee,Start,End\nMeca Tran,02/10/2026 17:00,02/10/2026 23:00\n", date(2026, 10, 2)),
        ("Employee,Date,Start,End\nMeca Tran,03/10/2026 00:00:00,03/10/2026 17:00,04/10/2026 01:00\n", date(2026, 10, 3)),
    ]
    for content, d in variants:
        out = _roster(c, h, vid, content).json()
        assert out["shifts_imported"] == 1, (content, out)
    got = {s.date: s for r in get_db().list_rosters() if r.venue_id == vid for s in r.shifts}
    for _, d in variants:
        assert d in got and got[d].employee_id == meca, d
        assert got[d].start_time == time(17, 0), d
    assert got[date(2026, 10, 3)].end_time == time(1, 0)


def test_first_last_name_columns_match_staff():
    c, h, vid = _roster_world()
    r = _roster(c, h, vid, "First Name\tLast Name\tDate\tStart\tFinish\tArea\tBreak (mins)\n"
                           "Meca\tTran\t28/09/2026\t17:00\t23:00\tFront Bar\t30\n"
                           "Ella\tRhodes\t29/09/2026\t11:00\t17:00\tRestaurant\t30\n")
    assert r.status_code == 200, r.text
    assert r.json()["shifts_imported"] == 2


def test_two_staff_with_same_name_and_no_email_column_skips():
    c, h, vid = _world()
    _staff(c, h, vid, "Name,Email\nJack Smith,jack.smith@wembley-synthetic.com.au\n"
                      "Jack Smith,jack.smith2@wembley-synthetic.com.au\nMeca Tran,meca@wembley-synthetic.com.au\n")
    assert len(get_db().get_employees(vid)) == 3
    out = _roster(c, h, vid, "Employee,Date,Start,End\nJack Smith,28/09/2026,17:00,23:00\n"
                             "Meca Tran,28/09/2026,17:00,23:00\n").json()
    assert out["shifts_imported"] == 1
    reason = out["rows"][0]["reason"]
    assert "two staff named Jack Smith" in reason and "Email" in reason
    # With an email column the right Jack gets the shift
    out = _roster(c, h, vid, "Employee,Email,Date,Start,End\n"
                             "Jack Smith,jack.smith2@wembley-synthetic.com.au,29/09/2026,17:00,23:00\n").json()
    assert out["shifts_imported"] == 1
    jack2 = [e for e in get_db().get_employees(vid) if e.email == "jack.smith2@wembley-synthetic.com.au"][0]
    assert [s for s in _week(vid).shifts if s.date == date(2026, 9, 29)][0].employee_id == jack2.id


def test_two_location_schedule_keeps_only_this_venue():
    c, h, vid = _roster_world()
    content = ("Date,Employee,Location,Area,Start Time,End Time\n"
               f"03/10/2026,Meca Tran,{APOTHECARY},Bar,5:00 PM,11:00 PM\n"
               f"03/10/2026,Meca Tran,{WEMBLEY},Front Bar,11:00 AM,3:00 PM\n"
               f"04/10/2026,Zara Quinn,{APOTHECARY},Bar,5:00 PM,11:00 PM\n")
    out = _roster(c, h, vid, content).json()
    assert out["shifts_imported"] == 1
    assert out["location"] == {"column": "Location", "kept": WEMBLEY, "skipped_other": 2}
    reasons = [r["reason"] for r in out["rows"]]
    assert reasons == [f"other location: {APOTHECARY}"] * 2
    assert [(s.start_time, s.role) for s in _week(vid).shifts] == [(time(11, 0), "Front Bar")]


def test_imported_weeks_are_costed_from_recorded_rates():
    c, h, vid = _roster_world()
    out = _roster(c, h, vid, deputy_schedule_csv()).json()
    wk = _week(vid)
    assert wk.total_cost is not None and wk.total_cost > 0
    assert all(s.cost is not None and s.cost > 0 for s in wk.shifts)
    emps = {e.id: e for e in get_db().get_employees(vid)}
    assert wk.total_cost == calculate_roster_cost(wk, emps, State.wa)
    ella = [s for s in wk.shifts if emps[s.employee_id].name == "Ella Rhodes"][0]
    assert ella.cost == calculate_shift_cost_breakdown(emps[ella.employee_id], ella, State.wa).total_cost
    # Her cost reflects the junior rate the venue recorded, not a default
    assert emps[ella.employee_id].hourly_base_rate == Decimal("22.11")
    assert out["week_costs"][0]["total_cost"] == str(wk.total_cost)


def test_roster_response_contract():
    c, h, vid = _roster_world()
    out = _roster(c, h, vid, "Name,Date,Start,End\nNobody,28/09/2026,9:00,17:00\n").json()
    for k in ("status", "shifts_imported", "weeks", "skipped_count", "skipped",
              "duplicates_skipped", "rows", "unmapped_columns", "column_map", "location"):
        assert k in out, k
    row = out["rows"][0]
    assert row == {"line": 2, "name": "Nobody", "date": "2026-09-28", "status": "skipped",
                   "reason": "no matching staff member"}
