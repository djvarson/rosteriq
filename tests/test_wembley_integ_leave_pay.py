"""
Rostered pay prep for leave a person works part of, with both rules at once:
a day of the leave marked as one they work (working_days) is priced as a
normal shift, and the rest of the leave is reckoned as ONE leave, not once
per stretch between working days, so its hours are never applied twice.
"""

import uuid
from datetime import date, datetime, time as dtime
from decimal import Decimal

from rosteriq.database import get_db
from rosteriq.tests.test_wembley_g6 import WEEK, _batch, _roster, _world
from rosteriq.tests.test_wembley_h3 import _hours_by_type


def test_leave_split_by_a_working_day_is_reckoned_once():
    w = _world()
    wed, thu, fri, sat = (date(2026, 10, 14), date(2026, 10, 15),
                          date(2026, 10, 16), date(2026, 10, 17))
    assert WEEK <= wed
    _roster(w.vid, [(f"sh-sam-{d:%a}", w.sam, d, dtime(16), dtime(23), 30)
                    for d in (wed, thu, fri, sat)])
    # Off Wed to Sat, but works Friday; 13h of leave over the other three
    # 6.5h shifts, so one of them was worked
    get_db().save_leave_request({
        "id": f"lv-{uuid.uuid4().hex[:6]}", "venue_id": w.vid, "employee_id": w.sam,
        "start_date": wed, "end_date": sat, "leave_type": "personal", "status": "approved",
        "paid_hours": 13, "working_days": [fri.isoformat()], "created_at": datetime(2026, 10, 1)})
    r = w.c.post("/api/payroll/prepare", json={"venue_id": w.vid, "period_start": str(WEEK),
                                               "period_end": str(sat), "state": "wa"}, headers=w.mgr)
    line = next(e for e in _batch(r)["employees"] if e["employee_id"] == w.sam)
    assert sum(Decimal(h) for h in _hours_by_type(line).values()) == Decimal("13.00")
    assert [(le["leave_type"], le["hours"]) for le in line["leave_entries"]] == [("personal", "13.00")]
    notes = [e for e in r.json()["validation_errors"] if e.startswith("Sam Sportsbar:")]
    assert any("holds 13h of the 19.5h rostered on Wed 14 Oct to Sat 17 Oct, so the other 6.5h "
               "is priced from the roster (Wed 14 Oct 16:00-23:00)" in n for n in notes), notes
