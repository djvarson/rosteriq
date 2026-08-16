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

@router.post("/api/announcements")
async def publish_announcement(body: AnnouncementBody,
                               user: UserContext = Depends(get_current_user)) -> dict:
    enforce_venue_access(body.venue_id)
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
            sent, no_phone, failed = [], [], []
            text = f"{body.title}: {body.body}"
            for emp in staff:
                phone = (getattr(emp, "phone", "") or "").strip()
                if not phone:
                    no_phone.append(emp.name)
                    continue
                ok = await sms.send_sms(phone, text)
                (sent if ok else failed).append(emp.name)
            sms_result = {
                "attempted": True,
                "sent": len(sent), "no_phone": len(no_phone), "failed": len(failed),
                "no_phone_names": no_phone, "failed_names": failed,
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


@router.get("/api/announcements")
async def venue_announcements(venue_id: str = Query(...)) -> dict:
    enforce_venue_access(venue_id)
    db = get_db()
    staff_count = len(db.get_employees(venue_id) or [])
    rows = db.list_announcements(venue_id) or []
    return {"venue_id": venue_id, "count": len(rows),
            "announcements": [_ann_payload(a, staff_count) for a in rows]}


@router.post("/api/announcements/{ann_id}/pin")
async def pin_announcement(ann_id: str, body: PinBody,
                           user: UserContext = Depends(get_current_user)) -> dict:
    enforce_venue_access(body.venue_id)
    db = get_db()
    ann = db.get_announcement(ann_id)
    if not ann or ann.get("venue_id") != body.venue_id:
        raise HTTPException(status_code=404, detail="Announcement not found")
    ann["pinned"] = body.pinned
    db.save_announcement(ann)
    audit("announcement.pin", body.venue_id, "announcement", ann_id,
          title=ann.get("title"), pinned=bool(body.pinned))
    return {"status": "pinned" if body.pinned else "unpinned", "announcement_id": ann_id}


@router.get("/api/sms/status")
async def sms_status(venue_id: str = Query(...)) -> dict:
    """Whether SMS fan-out is available. Reports state only — never values."""
    enforce_venue_access(venue_id)
    return {"configured": get_sms_service().is_configured}


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
