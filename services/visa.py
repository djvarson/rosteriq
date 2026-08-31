"""
Work-rights watching: visa expiry alerts and fortnightly hour-cap flags.

Integrity rule: RosterIQ never asserts what a visa legally allows. The
manager records the expiry date and hour cap from their own VEVO check;
this module only compares rosters against what was recorded. No legal
limit is hard-coded anywhere.
"""

from datetime import date, timedelta
from typing import Iterable, List, Optional

EXPIRY_WARNING_DAYS = 28


def visa_alerts(employees: Iterable, today: Optional[date] = None) -> List[dict]:
    """Expired and soon-to-expire visas, most urgent first.

    An expired visa is a stop-the-presses problem for the venue; one
    expiring inside EXPIRY_WARNING_DAYS is a book-the-VEVO-check nudge.
    """
    today = today or date.today()
    alerts = []
    for emp in employees:
        expiry = getattr(emp, "visa_expiry", None)
        if not expiry:
            continue
        days = (expiry - today).days
        if days < 0:
            alerts.append({
                "employee_id": emp.id, "name": emp.name,
                "visa_status": getattr(emp, "visa_status", None),
                "expiry": expiry.isoformat(), "kind": "expired",
                "days": days,
            })
        elif days <= EXPIRY_WARNING_DAYS:
            alerts.append({
                "employee_id": emp.id, "name": emp.name,
                "visa_status": getattr(emp, "visa_status", None),
                "expiry": expiry.isoformat(), "kind": "expiring",
                "days": days,
            })
    alerts.sort(key=lambda a: a["days"])
    return alerts


def _hours_by_employee(shifts: Iterable) -> dict:
    hours: dict = {}
    for s in shifts or []:
        try:
            h = float(s.duration_hours)
        except Exception:
            continue
        emp_id = str(getattr(s, "employee_id", "") or "")
        if emp_id:
            hours[emp_id] = hours.get(emp_id, 0.0) + h
    return hours


def fortnight_cap_flags(roster, employees: Iterable, prev_roster=None) -> List[dict]:
    """Employees rostered past their RECORDED fortnight cap.

    The fortnight is this roster's week plus the previous stored week
    (when one exists — with none, this week alone is checked against the
    cap, which can only under-flag, never cry wolf). Returns one flag per
    capped employee who exceeds their recorded limit, worst first.
    """
    capped = {str(e.id): e for e in employees
              if getattr(e, "visa_work_limit_fortnight", None)}
    if not capped:
        return []
    this_week = _hours_by_employee(getattr(roster, "shifts", []))
    prev_week = _hours_by_employee(getattr(prev_roster, "shifts", [])) if prev_roster else {}
    flags = []
    for emp_id, emp in capped.items():
        total = round(this_week.get(emp_id, 0.0) + prev_week.get(emp_id, 0.0), 2)
        cap = float(emp.visa_work_limit_fortnight)
        if total > cap:
            flags.append({
                "employee_id": emp_id, "name": emp.name,
                "cap_hours_fortnight": cap,
                "rostered_fortnight_hours": total,
                "this_week_hours": round(this_week.get(emp_id, 0.0), 2),
                "prev_week_hours": round(prev_week.get(emp_id, 0.0), 2),
                "over_by_hours": round(total - cap, 2),
                "message": (f"{emp.name} is rostered {total:g}h across the fortnight "
                            f"but their recorded visa cap is {cap:g}h — trim "
                            f"{total - cap:g}h or confirm their work rights."),
            })
    flags.sort(key=lambda f: -f["over_by_hours"])
    return flags


def preserve_recorded_work_rights(db, emp):
    """Integration syncs (Deputy/MYOB) rebuild Employee objects from the
    external system, which knows nothing about visas. Carry the manager's
    recorded fields across so a re-sync never silently erases a VEVO record.
    A sync that DOES set visa fields wins (external truth beats stale local).
    """
    if emp.visa_status or emp.visa_expiry or emp.visa_work_limit_fortnight:
        return emp
    try:
        prior = db.get_employee(emp.id)
    except Exception:
        prior = None
    if prior is not None:
        emp.visa_status = getattr(prior, "visa_status", None)
        emp.visa_expiry = getattr(prior, "visa_expiry", None)
        emp.visa_work_limit_fortnight = getattr(prior, "visa_work_limit_fortnight", None)
    return emp
