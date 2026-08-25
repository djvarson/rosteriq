"""
Communication hub — venue announcements with read receipts and optional SMS
fan-out. The first slice of making RosterIQ the venue's communication portal:
managers broadcast once, staff see it in /my (and by text if the venue has
SMS configured), and the manager can see exactly who has read it.

Manager routes:
    POST /api/announcements            -- publish (optional send_sms fan-out)
    GET  /api/announcements            -- venue's announcements + read counts
    POST /api/announcements/{id}/pin   -- toggle pinned
    GET  /api/sms/status               -- is SMS configured? (never leaks creds)

Staff routes (email-linked identity, same as the rest of the portal):
    GET  /api/me/announcements         -- my venue's feed + my unread count
    POST /api/me/announcements/{id}/read -- mark read (read receipt)

SMS honesty contract: the publish response reports exactly what happened per
staff member — sent / no_phone / failed / not_configured. We never claim a
text went out when it didn't.
"""

import asyncio
import logging
import uuid
from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field

from rosteriq.database import get_db
from rosteriq.middleware.auth import get_current_user, UserContext
from rosteriq.middleware.tenant import enforce_venue_access
from rosteriq.routes.staff_portal import _linked_employee, _no_link_response
from rosteriq.services.events import audit
from rosteriq.services.sms import get_sms_service

logger = logging.getLogger(__name__)

router = APIRouter(tags=["comms"])


class AnnouncementBody(BaseModel):
    venue_id: str
    title: str = Field(..., min_length=1, max_length=120)
    body: str = Field(..., min_length=1, max_length=2000)
    pinned: bool = False
    send_sms: bool = Field(default=False, description="Also text every staff member with a phone number")


class PinBody(BaseModel):
    venue_id: str
    pinned: bool


def _ann_payload(a: dict, staff_count: int) -> dict:
    read_by = list(a.get("read_by") or [])
    return {
        "id": a["id"],
        "title": a["title"],
        "body": a["body"],
        "author_name": a.get("author_name"),
        "pinned": bool(a.get("pinned")),
        "created_at": str(a.get("created_at")),
        "read_count": len(read_by),
        "staff_count": staff_count,
        "sms_result": a.get("sms_result"),
    }


# ---------------------------------------------------------------------------
# Manager-facing
# ---------------------------------------------------------------------------

def _require_manager(user: UserContext) -> None:
    """Announcements are the venue speaking to its staff — and with SMS on,
    each publish can cost real money. Staff have the team feed for their own
    voice; publishing (and pinning) is manager/owner only. This route only
    checked the venue, so any linked staff member could blast the venue."""
    if user.role not in ("manager", "owner") and not getattr(user, "is_owner", False):
        raise HTTPException(status_code=403, detail="Managers only")


@router.post("/api/announcements")
async def publish_announcement(body: AnnouncementBody,
                               user: UserContext = Depends(get_current_user)) -> dict:
    enforce_venue_access(body.venue_id)
    _require_manager(user)
    db = get_db()
    staff = db.get_employees(body.venue_id) or []

    sms_result = None
    if body.send_sms:
        sms = get_sms_service()
        if not sms.is_configured:
            sms_result = {
                "attempted": False,
                "reason": "SMS is not configured — add Twilio credentials in Railway "
                          "(TWILIO_ACCOUNT_SID / TWILIO_AUTH_TOKEN / TWILIO_FROM_NUMBER).",
                "sent": 0, "no_phone": 0, "failed": 0,
            }
        else:
            sent, no_phone, failed, rate_limited = [], [], [], []
            text = f"{body.title}: {body.body}"
            with_phone = []
            for emp in staff:
                phone = (getattr(emp, "phone", "") or "").strip()
                if phone:
                    with_phone.append((emp, phone))
                else:
                    no_phone.append(emp.name)

            # Bounded concurrency: sequential awaits held this request open
            # for minutes when the provider was slow (30 staff × a 15s
            # timeout each). Eight in flight keeps a full venue under ~10s
            # even in the worst case.
            _gate = asyncio.Semaphore(8)

            async def _one(emp, phone):
                async with _gate:
                    return emp, await sms.send_detailed(phone, text)

            results = await asyncio.gather(
                *(_one(emp, phone) for emp, phone in with_phone))
            for emp, (ok, reason) in results:
                if ok:
                    sent.append(emp.name)
                elif reason == "rate_limited":
                    # Not a failure: they got an SMS in the last few minutes.
                    # Reporting this as "failed" made managers re-send, which
                    # rate-limited MORE people.
                    rate_limited.append(emp.name)
                elif reason == "invalid_number":
                    no_phone.append(emp.name)
                else:
                    failed.append(emp.name)
            sms_result = {
                "attempted": True,
                "sent": len(sent), "no_phone": len(no_phone), "failed": len(failed),
                "rate_limited": len(rate_limited),
                "no_phone_names": no_phone, "failed_names": failed,
                "rate_limited_names": rate_limited,
            }

    ann = {
        "id": f"an-{uuid.uuid4().hex[:10]}",
        "venue_id": body.venue_id,
        "title": body.title,
        "body": body.body,
        "author_id": user.user_id,
        "author_name": user.email,
        "pinned": body.pinned,
        "sms_result": sms_result,
        "read_by": [],
        "created_at": datetime.utcnow(),
    }
    db.save_announcement(ann)
    logger.info(f"Announcement published at {body.venue_id}: {body.title!r}")
    audit("announcement.publish", body.venue_id, "announcement", ann["id"],
          title=body.title, pinned=body.pinned, staff_count=len(staff),
          sms_requested=body.send_sms,
          sms_attempted=bool(sms_result and sms_result.get("attempted")),
          sms_sent=(sms_result or {}).get("sent", 0),
          sms_failed=(sms_result or {}).get("failed", 0),
          sms_no_phone=(sms_result or {}).get("no_phone", 0))
    return {"status": "published", "announcement_id": ann["id"], "sms_result": sms_result}


@router.get("/api/sms/status")
async def sms_status(user: UserContext = Depends(get_current_user)) -> dict:
    """Is SMS live, and if not, exactly which variables light it up.

    Platform-level (env vars in Railway), so there is nothing per-venue to
    scope — but only managers/owners see it: staff have no send button, and
    the setup detail is operational metadata. Never returns a credential."""
    st = get_sms_service().status()
    if user.role in ("manager", "owner") or getattr(user, "is_owner", False):
        return st
    # Staff (the demo dashboard runs as one) get the cosmetic flag only —
    # never the from-number or the setup variable names.
    return {"configured": st["configured"], "provider": st.get("provider")}


@router.get("/api/announcements")
async def venue_announcements(venue_id: str = Query(...),
                              user: UserContext = Depends(get_current_user)) -> dict:
    enforce_venue_access(venue_id)
    db = get_db()
    staff_count = len(db.get_employees(venue_id) or [])
    rows = db.list_announcements(venue_id) or []
    payloads = [_ann_payload(a, staff_count) for a in rows]
    # The stored sms_result names exactly who has no phone number and whose
    # send failed — a per-person roster only managers should see. Staff who
    # can read this listing (the demo account is one) get the announcements
    # without the delivery detail.
    if user.role not in ("manager", "owner") and not getattr(user, "is_owner", False):
        for pl in payloads:
            pl["sms_result"] = None
    return {"venue_id": venue_id, "count": len(rows),
            "announcements": payloads}


@router.post("/api/announcements/{ann_id}/pin")
async def pin_announcement(ann_id: str, body: PinBody,
                           user: UserContext = Depends(get_current_user)) -> dict:
    enforce_venue_access(body.venue_id)
    _require_manager(user)
    db = get_db()
    ann = db.get_announcement(ann_id)
    if not ann or ann.get("venue_id") != body.venue_id:
        raise HTTPException(status_code=404, detail="Announcement not found")
    ann["pinned"] = body.pinned
    db.save_announcement(ann)
    audit("announcement.pin", body.venue_id, "announcement", ann_id,
          title=ann.get("title"), pinned=bool(body.pinned))
    return {"status": "pinned" if body.pinned else "unpinned", "announcement_id": ann_id}


# ---------------------------------------------------------------------------
# Staff-facing
# ---------------------------------------------------------------------------

@router.get("/api/me/announcements")
async def my_announcements(user: UserContext = Depends(get_current_user)) -> dict:
    db = get_db()
    emp, vid = _linked_employee(db, user)
    if not emp:
        return _no_link_response(user)
    rows = db.list_announcements(vid) or []
    feed = []
    unread = 0
    for a in rows:
        read = emp.id in (a.get("read_by") or [])
        if not read:
            unread += 1
        feed.append({
            "id": a["id"], "title": a["title"], "body": a["body"],
            "pinned": bool(a.get("pinned")), "created_at": str(a.get("created_at")),
            "read": read,
        })
    return {"linked": True, "count": len(feed), "unread": unread, "announcements": feed}


@router.post("/api/me/announcements/{ann_id}/read")
async def mark_announcement_read(ann_id: str,
                                 user: UserContext = Depends(get_current_user)) -> dict:
    db = get_db()
    emp, vid = _linked_employee(db, user)
    if not emp:
        raise HTTPException(status_code=409, detail=_no_link_response(user)["message"])
    ann = db.get_announcement(ann_id)
    if not ann or ann.get("venue_id") != vid:
        raise HTTPException(status_code=404, detail="Announcement not found")
    read_by = list(ann.get("read_by") or [])
    if emp.id not in read_by:
        read_by.append(emp.id)
        ann["read_by"] = read_by
        db.save_announcement(ann)
    return {"status": "read", "announcement_id": ann_id}
