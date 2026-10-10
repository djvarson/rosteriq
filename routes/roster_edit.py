"""
Managers edit a roster by hand, and copy a week forward.

    POST   /rosters/{id}/shifts                 add a shift
    PUT    /rosters/{id}/shifts/{shift_id}      move, reassign, retime, change area or break
    DELETE /rosters/{id}/shifts/{shift_id}      remove a shift
    POST   /rosters/{id}/copy-to?week_start=    clone into another week as a new draft
    GET    /rosters/{id}/changes                changes staff haven't been told about
    POST   /rosters/{id}/notify-changes         tell the affected staff

Shift edits are gated on the shift's area (enforce_section_manager), so a
department manager edits only their own sections; copying a week is
venue-wide. Edits to a published roster keep it published and staff see them
at once; the manager re-notifies from the "changed since publish" list.
Changing who, when or where a shift is, or deleting it, withdraws any cover
request still open on it.
"""

import datetime as dt
import logging
import uuid
from datetime import date, timedelta
from typing import Optional

from fastapi import APIRouter, HTTPException, Query
from fastapi.encoders import jsonable_encoder
from pydantic import BaseModel, Field

from rosteriq.database import get_db
from rosteriq.middleware.tenant import (
    enforce_section_manager, enforce_venue_manager, enforce_venue_manager_any,
    get_tenant_context_optional, load_roster_in_scope, scoped_employee_map,
)
from rosteriq.models import Shift, ShiftStatus, normalise_area
from rosteriq.services import roster_edit as edit
from rosteriq.services.current_roster import (
    PUBLISHED, current_roster, is_imported, roster_state, supersede_week_drafts, week_monday,
)
from rosteriq.services.events import audit

logger = logging.getLogger(__name__)

router = APIRouter(tags=["roster-edit"])


class ShiftCreate(BaseModel):
    employee_id: str = Field(..., min_length=1, max_length=100)
    date: dt.date
    start_time: dt.time
    end_time: dt.time
    break_minutes: int = Field(0, ge=0, le=240)
    role: str = Field(..., min_length=1, max_length=40)


class ShiftUpdate(BaseModel):
    employee_id: Optional[str] = Field(None, min_length=1, max_length=100)
    date: Optional[dt.date] = None
    start_time: Optional[dt.time] = None
    end_time: Optional[dt.time] = None
    break_minutes: Optional[int] = Field(None, ge=0, le=240)
    role: Optional[str] = Field(None, min_length=1, max_length=40)


def _actor() -> Optional[str]:
    tenant = get_tenant_context_optional()
    return getattr(tenant, "user_id", None) if tenant else None


def _editable_roster(db, roster_id: str):
    roster = load_roster_in_scope(db, roster_id)
    enforce_venue_manager_any(roster.venue_id)
    state = roster_state(db, roster.id)
    if state in edit.LOCKED_STATES:
        raise HTTPException(409, f"This roster is {state} — edit the week's current roster instead.")
    return roster


def _find_shift(roster, shift_id: str):
    shift = next((s for s in roster.shifts if s.id == shift_id), None)
    if shift is None:
        raise HTTPException(404, "Shift not found")
    return shift


def _venue_employee(db, roster, employee_id: str):
    emp = db.get_employee(employee_id)
    if emp is None or getattr(emp, "venue_id", None) != roster.venue_id:
        raise HTTPException(400, "That person isn't on this venue's staff list.")
    if not edit.is_active(emp):
        raise HTTPException(409, f"{emp.name} is inactive and can't be rostered — "
                                 "reassign or delete the shift, or reactivate them on the Staff page.")
    return emp


def _valid_times(shift) -> None:
    if shift.start_time == shift.end_time:
        raise HTTPException(400, "A shift needs different start and end times.")
    if shift.break_minutes >= shift.duration_hours * 60:
        raise HTTPException(400, "The break is as long as the shift.")


def _in_week(roster, day: date) -> None:
    if not roster.week_start <= day <= roster.week_end:
        raise HTTPException(400, f"{day.isoformat()} isn't in this roster's week "
                                 f"({roster.week_start.isoformat()} to {roster.week_end.isoformat()}).")


def _area(name) -> str:
    area = normalise_area(name)
    if not area:
        raise HTTPException(400, "Choose the area this shift is in.")
    return area


def _busy():
    return HTTPException(409, "Someone else is saving this roster right now — try again.")


def _result(db, roster, shift, warnings, action) -> dict:
    pending = edit.pending_changes(db, roster)
    return {
        "action": action,
        "roster_id": roster.id,
        "shift": jsonable_encoder(shift) if shift is not None else None,
        "warnings": warnings,
        "total_cost": str(roster.total_cost) if roster.total_cost is not None else None,
        "state": roster_state(db, roster.id),
        "changed_since_publish": bool(pending),
        "pending_changes": len(pending),
        "roster": jsonable_encoder(roster),
    }


def _save_and_record(db, roster, action, before, after) -> None:
    db.save_roster(roster)
    staff_see_it = edit.staff_visible(db, roster)
    if staff_see_it:
        edit.record_change(db, roster, action, before, after, _actor())
    audit(f"roster.shift_{action}", roster.venue_id, "roster", roster.id,
          shift_id=(after or before or {}).get("shift_id"), before=before, after=after,
          week_start=roster.week_start.isoformat(),
          state=roster_state(db, roster.id), staff_visible=staff_see_it)


async def _broadcast(roster, summary: str) -> None:
    try:
        from rosteriq.services.ws_events import get_dispatcher
        await get_dispatcher().roster_updated(venue_id=roster.venue_id, roster_id=roster.id,
                                              summary=summary)
    except Exception as e:
        logger.warning(f"Roster update broadcast failed: {e}")


@router.post("/rosters/{roster_id}/shifts")
async def add_shift(roster_id: str, body: ShiftCreate):
    db = get_db()
    roster = _editable_roster(db, roster_id)
    role = _area(body.role)
    enforce_section_manager(roster.venue_id, role)
    try:
        with edit.roster_lock(db, roster.id):
            roster = load_roster_in_scope(db, roster_id).model_copy(deep=True)
            _in_week(roster, body.date)
            emp = _venue_employee(db, roster, body.employee_id)
            shift = Shift(id=f"shift-{uuid.uuid4().hex[:10]}", employee_id=emp.id,
                          date=body.date, start_time=body.start_time, end_time=body.end_time,
                          break_minutes=body.break_minutes, status=ShiftStatus.scheduled, role=role)
            _valid_times(shift)
            venue = db.get_venue(roster.venue_id)
            try:
                warnings = edit.check_shift(db, roster, venue, emp, shift)
            except edit.ShiftClash as clash:
                raise HTTPException(409, str(clash))
            state = edit.venue_state(venue)
            edit.cost_shift(shift, emp, state)
            roster.shifts = roster.shifts + [shift]
            edit.recost_total(roster, scoped_employee_map(db, roster.venue_id), state)
            _save_and_record(db, roster, "add", None, edit.snapshot(shift, emp))
    except edit.RosterBusy:
        raise _busy()
    return _result(db, roster, shift, warnings, "add")


def _gate_update(roster, current, body) -> str:
    """The area the updated shift will be in. Moving a shift between areas
    needs both of them."""
    enforce_section_manager(roster.venue_id, current.role)
    if "role" not in body.model_fields_set or body.role is None:
        return current.role
    role = _area(body.role)
    if role == normalise_area(current.role):
        return current.role
    enforce_section_manager(roster.venue_id, role)
    return role


@router.put("/rosters/{roster_id}/shifts/{shift_id}")
async def update_shift(roster_id: str, shift_id: str, body: ShiftUpdate):
    db = get_db()
    roster = _editable_roster(db, roster_id)
    _gate_update(roster, _find_shift(roster, shift_id), body)
    sent = body.model_fields_set
    try:
        with edit.roster_lock(db, roster.id):
            roster = load_roster_in_scope(db, roster_id).model_copy(deep=True)
            current = _find_shift(roster, shift_id)
            role = _gate_update(roster, current, body)
            before = edit.snapshot(current, db.get_employee(current.employee_id))
            fields = {
                "employee_id": body.employee_id if "employee_id" in sent and body.employee_id else current.employee_id,
                "date": body.date if "date" in sent and body.date else current.date,
                "start_time": body.start_time if "start_time" in sent and body.start_time else current.start_time,
                "end_time": body.end_time if "end_time" in sent and body.end_time else current.end_time,
                "break_minutes": body.break_minutes if "break_minutes" in sent and body.break_minutes is not None
                else current.break_minutes,
                "role": role,
            }
            _in_week(roster, fields["date"])
            # An open shift left open has nobody to check or price.
            emp = _venue_employee(db, roster, fields["employee_id"]) if fields["employee_id"] else None
            shift = Shift(id=current.id, status=current.status,
                          penalty_multiplier=current.penalty_multiplier, **fields)
            _valid_times(shift)
            venue = db.get_venue(roster.venue_id)
            state = edit.venue_state(venue)
            warnings = edit.area_warnings(venue, shift.role)
            if emp is not None:
                try:
                    warnings = edit.check_shift(db, roster, venue, emp, shift, exclude_id=current.id)
                except edit.ShiftClash as clash:
                    raise HTTPException(409, str(clash))
                edit.cost_shift(shift, emp, state)
            roster.shifts = [shift if s.id == current.id else s for s in roster.shifts]
            edit.recost_total(roster, scoped_employee_map(db, roster.venue_id), state)
            after = edit.snapshot(shift, emp)
            if {k: v for k, v in before.items() if k != "employee_name"} == \
                    {k: v for k, v in after.items() if k != "employee_name"}:
                return _result(db, roster, shift, warnings, "unchanged")
            _save_and_record(db, roster, "update", before, after)
            warnings = warnings + edit.settle_covers(db, roster.venue_id, current, shift, _actor())
    except edit.RosterBusy:
        raise _busy()
    return _result(db, roster, shift, warnings, "update")


@router.delete("/rosters/{roster_id}/shifts/{shift_id}")
async def delete_shift(roster_id: str, shift_id: str):
    db = get_db()
    roster = _editable_roster(db, roster_id)
    enforce_section_manager(roster.venue_id, _find_shift(roster, shift_id).role)
    try:
        with edit.roster_lock(db, roster.id):
            roster = load_roster_in_scope(db, roster_id).model_copy(deep=True)
            current = _find_shift(roster, shift_id)
            enforce_section_manager(roster.venue_id, current.role)
            before = edit.snapshot(current, db.get_employee(current.employee_id))
            roster.shifts = [s for s in roster.shifts if s.id != shift_id]
            venue = db.get_venue(roster.venue_id)
            edit.recost_total(roster, scoped_employee_map(db, roster.venue_id), edit.venue_state(venue))
            _save_and_record(db, roster, "delete", before, None)
            notices = edit.settle_covers(db, roster.venue_id, current, None, _actor())
    except edit.RosterBusy:
        raise _busy()
    return _result(db, roster, None, notices, "delete")


@router.post("/rosters/{roster_id}/copy-to")
async def copy_roster_to(roster_id: str,
                         week_start: Optional[date] = Query(None, description="Any day of the target week; default the next week")):
    db = get_db()
    source = load_roster_in_scope(db, roster_id)
    enforce_venue_manager(source.venue_id)
    target = week_monday(week_start) if week_start else week_monday(source.week_start) + timedelta(days=7)
    if target == week_monday(source.week_start):
        raise HTTPException(400, "Pick a different week to copy into.")
    live = current_roster(db, source.venue_id, target)
    if live is not None and (roster_state(db, live.id) == PUBLISHED or is_imported(live)):
        raise HTTPException(409, f"The week of {target.isoformat()} already has a roster staff can see — "
                                 "edit that one instead, or recall it first.")
    venue = db.get_venue(source.venue_id)
    roster, skipped, left_open = edit.copy_roster(db, source, target, venue)
    db.save_roster(roster)
    try:
        superseded = supersede_week_drafts(db, source.venue_id, target, roster.id,
                                           reason=f"Replaced by a copy of roster {source.id}",
                                           actor_id=_actor() or "system")
    except Exception as e:
        logger.warning(f"Could not supersede drafts for {source.venue_id} {target}: {e}")
        superseded = []
    employees = [e for e in (db.get_employees(source.venue_id) or [])]
    prev = current_roster(db, source.venue_id, target - timedelta(days=7))
    visa_flags = edit.visa_cap_flags(db, roster, employees, prev if prev and prev.id != roster.id else None)
    elsewhere = edit.cross_venue_warnings(db, roster, employees)
    audit("roster.copy", source.venue_id, "roster", roster.id, source_roster_id=source.id,
          week_start=target.isoformat(), shifts=len(roster.shifts), skipped=len(skipped),
          left_open=len(left_open), superseded=len(superseded))
    await _broadcast(roster, f"Copied the week of {source.week_start.isoformat()} into {target.isoformat()}")
    return {
        "roster": jsonable_encoder(roster),
        "roster_id": roster.id,
        "source_roster_id": source.id,
        "week_start": target.isoformat(),
        "copied": len(roster.shifts) - len(left_open),
        "skipped": skipped,
        "left_open": left_open,
        "superseded": superseded,
        "visa_flags": visa_flags,
        "cross_venue_warnings": elsewhere,
        "total_cost": str(roster.total_cost) if roster.total_cost is not None else None,
    }


@router.get("/rosters/{roster_id}/changes")
async def roster_changes(roster_id: str):
    db = get_db()
    roster = load_roster_in_scope(db, roster_id)
    enforce_venue_manager_any(roster.venue_id)
    pending = edit.pending_changes(db, roster)
    changed, cancelled = edit.affected_staff(pending)
    return {
        "roster_id": roster.id,
        "state": roster_state(db, roster.id),
        "staff_visible": edit.staff_visible(db, roster),
        "changed_since_publish": bool(pending),
        "pending": pending[-100:],
        "pending_count": len(pending),
        "affected_staff": len(changed) + len(cancelled),
    }


@router.post("/rosters/{roster_id}/notify-changes")
async def notify_roster_changes(roster_id: str):
    db = get_db()
    roster = load_roster_in_scope(db, roster_id)
    enforce_venue_manager_any(roster.venue_id)
    pending = edit.pending_changes(db, roster)
    tenant = get_tenant_context_optional()
    sections = tenant.managed_sections(roster.venue_id) if tenant is not None else None
    if sections is not None:
        mine = {normalise_area(s) for s in sections}
        pending = [c for c in pending
                   if all(normalise_area(r) in mine for r in edit.change_roles(c))]
    if not pending:
        return {"roster_id": roster.id, "notified_changes": 0, "reached": 0,
                "delivered": {"email": 0, "sms": 0, "push": 0, "in_app": 0},
                "message": "No changes waiting to be sent.",
                "changed_since_publish": bool(edit.pending_changes(db, roster))}
    changed, cancelled = edit.affected_staff(pending)
    from rosteriq.services.notification_hub import NotificationEventType, get_notification_hub
    hub = get_notification_hub()
    payload = {"roster_id": roster.id, "week_start": roster.week_start.isoformat(),
               "week_end": roster.week_end.isoformat(), "changes": len(pending)}
    totals = {"reached": 0, "delivered": {"email": 0, "sms": 0, "push": 0, "in_app": 0},
              "channels_available": None, "total_targets": 0}
    messages = []
    for event, targets in ((NotificationEventType.SHIFT_CHANGED, changed),
                           (NotificationEventType.SHIFT_CANCELLED, cancelled)):
        if not targets:
            continue
        summary = await hub.dispatch(event, roster.venue_id, payload, target_employee_ids=targets)
        if "error" in summary:
            raise HTTPException(502, "Sending failed — staff weren't notified. Try again, or share the /my link.")
        totals["reached"] += int(summary.get("reached", 0) or 0)
        totals["total_targets"] += int(summary.get("total_targets", 0) or 0)
        for k, v in (summary.get("delivered") or {}).items():
            totals["delivered"][k] = totals["delivered"].get(k, 0) + int(v or 0)
        totals["channels_available"] = summary.get("channels_available")
        if summary.get("message"):
            messages.append(summary["message"])
    try:
        with edit.roster_lock(db, roster.id):
            edit.mark_notified(db, roster, {c["id"] for c in pending}, _actor())
    except edit.RosterBusy:
        raise _busy()
    audit("roster.notify_changes", roster.venue_id, "roster", roster.id,
          changes=len(pending), staff_changed=len(changed), staff_cancelled=len(cancelled),
          reached=totals["reached"])
    return {"roster_id": roster.id, "notified_changes": len(pending),
            "staff_changed": len(changed), "staff_cancelled": len(cancelled),
            "message": " ".join(dict.fromkeys(messages)),
            "changed_since_publish": bool(edit.pending_changes(db, roster)),
            **totals}
