"""
Time clock set-up: devices, PINs and the venue's clock rules.

Manager routes (full venue manager):
    POST /api/kiosk/devices                 set up a time clock device (token shown once)
    GET  /api/kiosk/devices?venue_id=       the venue's devices (Settings)
    POST /api/kiosk/devices/{id}/revoke     switch a device off
    GET  /api/kiosk/settings?venue_id=      the venue's time clock rules
    PUT  /api/kiosk/settings                change them
    POST /api/kiosk/pins/generate           PINs for everyone without one (shown once)

Staff routes (their own record only):
    GET  /api/me/clock                      my clock state, PIN set?, may I punch here?
    POST /api/me/clock-pin                  set my own kiosk PIN
"""

import logging
from datetime import datetime, timedelta
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from rosteriq.database import get_db
from rosteriq.middleware.auth import get_current_user, UserContext
from rosteriq.middleware.tenant import enforce_venue_manager
from rosteriq.services import kiosk
from rosteriq.services.clock import venue_today
from rosteriq.services.events import audit

logger = logging.getLogger(__name__)

router = APIRouter(tags=["timeclock"])


class DeviceCreate(BaseModel):
    venue_id: str
    name: str = Field(default="", max_length=80)


class DeviceRevoke(BaseModel):
    venue_id: str


class SettingsUpdate(BaseModel):
    venue_id: str
    allow_self_clock: Optional[bool] = None
    auto_meal_break: Optional[bool] = None


class GeneratePins(BaseModel):
    venue_id: str
    employee_ids: Optional[List[str]] = Field(default=None, max_length=1000,
                                              description="Only these people (default: everyone without a PIN)")
    replace: bool = Field(default=False, description="Also replace PINs people already have")


class MyPin(BaseModel):
    pin: str = Field(..., min_length=4, max_length=6, pattern=r"^\d{4,6}$")


def _no_store(body: dict) -> JSONResponse:
    return JSONResponse(content=body, headers={"Cache-Control": "no-store"})


@router.post("/api/kiosk/devices")
async def create_device(body: DeviceCreate, user: UserContext = Depends(get_current_user)):
    enforce_venue_manager(body.venue_id)
    db = get_db()
    venue = db.get_venue(body.venue_id)
    if not venue:
        raise HTTPException(status_code=404, detail="Venue not found")
    name = body.name.strip() or f"{getattr(venue, 'name', '') or 'Venue'} time clock"
    device, token = kiosk.create_device(db, body.venue_id, name, created_by=user.user_id)
    audit("timeclock.kiosk_created", body.venue_id, "kiosk_device", device["id"], device=device["name"])
    return _no_store({"device": kiosk.public_device(device), "token": token,
                      "venue_name": getattr(venue, "name", "")})


@router.get("/api/kiosk/devices")
async def list_devices(venue_id: str = Query(...)) -> dict:
    enforce_venue_manager(venue_id)
    db = get_db()
    return {"venue_id": venue_id,
            "devices": [kiosk.public_device(d) for d in (db.list_kiosk_devices(venue_id) or [])]}


@router.post("/api/kiosk/devices/{device_id}/revoke")
async def revoke_device(device_id: str, body: DeviceRevoke,
                        user: UserContext = Depends(get_current_user)) -> dict:
    enforce_venue_manager(body.venue_id)
    db = get_db()
    device = db.get_kiosk_device(device_id)
    if not device or device.get("venue_id") != body.venue_id:
        raise HTTPException(status_code=404, detail="Time clock not found")
    if not device.get("revoked_at"):
        device = kiosk.revoke_device(db, device, by=user.user_id)
        audit("timeclock.kiosk_revoked", body.venue_id, "kiosk_device", device_id,
              device=device.get("name"))
    return {"status": "revoked", "device": kiosk.public_device(device)}


@router.get("/api/kiosk/settings")
async def get_settings(venue_id: str = Query(...)) -> dict:
    enforce_venue_manager(venue_id)
    return {"venue_id": venue_id, **kiosk.load_settings(get_db(), venue_id)}


@router.put("/api/kiosk/settings")
async def put_settings(body: SettingsUpdate) -> dict:
    enforce_venue_manager(body.venue_id)
    db = get_db()
    before = kiosk.load_settings(db, body.venue_id)
    after = kiosk.save_settings(db, body.venue_id, {"allow_self_clock": body.allow_self_clock,
                                                    "auto_meal_break": body.auto_meal_break})
    changed = {k: {"old": before[k], "new": after[k]} for k in after if before[k] != after[k]}
    if changed:
        audit("timeclock.settings", body.venue_id, "venue", body.venue_id, changed=changed)
    return {"venue_id": body.venue_id, **after}


@router.post("/api/kiosk/pins/generate")
async def generate_pins(body: GeneratePins):
    """A random PIN for each active person without one (or the listed people).
    The PINs are in this response only — print or hand them out now."""
    enforce_venue_manager(body.venue_id)
    db = get_db()
    have = db.list_timeclock_pin_employee_ids(body.venue_id) or set()
    wanted = set(body.employee_ids or [])
    out = []
    for emp in db.get_employees(body.venue_id) or []:
        if not getattr(emp, "active", True):
            continue
        if wanted and emp.id not in wanted:
            continue
        if emp.id in have and not body.replace:
            continue
        pin = kiosk.generate_pin()
        db.set_timeclock_pin(body.venue_id, emp.id, kiosk.hash_pin(body.venue_id, emp.id, pin))
        out.append({"employee_id": emp.id, "name": emp.name, "pin": pin})
    out.sort(key=lambda r: r["name"].lower())
    if out:
        audit("timeclock.pins_generated", body.venue_id, "venue", body.venue_id, count=len(out),
              employee_ids=[r["employee_id"] for r in out][:200])
    venue = db.get_venue(body.venue_id)
    return _no_store({"venue_id": body.venue_id, "venue_name": getattr(venue, "name", "") if venue else "",
                      "count": len(out), "pins": out})


def _my_records(db, user: UserContext) -> list:
    """(employee, venue_id) for every active record that is this login, in
    every venue it holds, staff-only ones included (services/kiosk.my_records)."""
    return kiosk.my_records(db, user.user_id)


@router.get("/api/me/clock")
async def my_clock(venue_id: Optional[str] = Query(None),
                   user: UserContext = Depends(get_current_user)) -> dict:
    """``stale``: the open punch is a forgotten clock-out, as on the kiosk
    board. A rostered one whose shift has ended is closed first; the rest
    need the time they finished (POST /api/clock/out with finished_at).
    ``stale_at`` (UTC) is when an open punch becomes one, for a page left open."""
    from rosteriq.routes.timeclock import _close_stale_punches, _fmt_local, _tz
    db = get_db()
    mine = _my_records(db, user)
    if venue_id:
        mine = [(e, v) for e, v in mine if v == venue_id]
    if not mine:
        return {"linked": False}
    emp, vid = mine[0]
    _close_stale_punches(db, vid)
    open_ts = db.get_open_timesheet(vid, emp.id)
    state, clock_in, stale_at, stale = "off", None, None, False
    if open_ts:
        state = "break" if open_ts.get("break_started_at") else "on"
        clock_in = kiosk.naive_utc(open_ts["clock_in"])
        stale_at = clock_in + timedelta(hours=kiosk.STALE_OPEN_HOURS)
        stale = datetime.utcnow() > stale_at
    else:
        today = venue_today(vid, db)
        if any(t.get("employee_id") == emp.id and t.get("clock_out")
               for t in (db.get_timesheets(vid, today, today) or [])):
            state = "done"
    return {
        "linked": True,
        "venue_id": vid,
        "employee_id": emp.id,
        "has_pin": bool(db.get_timeclock_pin(vid, emp.id)),
        "self_clock": kiosk.load_settings(db, vid)["allow_self_clock"],
        "state": state,
        "clock_in": clock_in.isoformat() if clock_in else None,
        "clock_in_label": _fmt_local(clock_in, _tz(vid, db)) if clock_in else None,
        "stale": stale,
        "stale_at": stale_at.isoformat(timespec="seconds") if stale_at else None,
    }


@router.post("/api/me/clock-pin")
async def set_my_pin(body: MyPin, user: UserContext = Depends(get_current_user)) -> dict:
    """Set my own kiosk PIN on every staff record that is me (one per venue).
    The records come only from venues this login holds."""
    db = get_db()
    mine = _my_records(db, user)
    if not mine:
        raise HTTPException(status_code=409, detail=(
            "Your login isn't linked to a staff record yet — enter your join code first."))
    if kiosk.weak_pin(body.pin):
        raise HTTPException(status_code=422, detail=kiosk.WEAK_PIN_MESSAGE)
    for emp, vid in mine:
        db.set_timeclock_pin(vid, emp.id, kiosk.hash_pin(vid, emp.id, body.pin))
        audit("timeclock.pin_set", vid, "employee", emp.id, employee=emp.name, by="self")
    return {"status": "pin_set", "venues": len(mine)}
