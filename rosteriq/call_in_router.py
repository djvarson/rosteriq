"""FastAPI router for staff call-in SMS flow.

Endpoints:
- POST /api/v1/call-in → create and send call-in SMS
- GET /api/v1/call-in/{venue_id} → list all call-ins for venue
- POST /api/v1/call-in/webhook/inbound → handle inbound SMS replies
"""
import os
import re
from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, HTTPException

from rosteriq.call_in import (
    CallInRequest,
    CallInStatus,
    get_service,
    get_store,
)

router = APIRouter(prefix="/api/v1", tags=["call-in"])


# ---------------------------------------------------------------------------
# Request/Response models (simple dicts for now, can be Pydantic later)
# ---------------------------------------------------------------------------


def _validate_phone(phone: str) -> None:
    """Raise 400 if phone is invalid. Must start with + or 0, be 8+ digits."""
    if not phone:
        raise HTTPException(status_code=400, detail="phone is required")
    # Remove whitespace/hyphens for digit check
    digits = re.sub(r"[\s\-]", "", phone)
    if not re.match(r"^[\+0]\d{7,}$", digits):
        raise HTTPException(
            status_code=400,
            detail="phone must start with + or 0 and have 8+ digits",
        )


def _validate_shift_times(shift_start: datetime, shift_end: datetime) -> None:
    """Raise 400 if shift times are invalid."""
    if shift_end <= shift_start:
        raise HTTPException(
            status_code=400, detail="shift_end must be after shift_start"
        )


def _call_in_to_dict(req: CallInRequest) -> dict:
    """Convert CallInRequest dataclass to JSON-safe dict."""
    return {
        "request_id": req.request_id,
        "venue_id": req.venue_id,
        "employee_id": req.employee_id,
        "employee_name": req.employee_name,
        "phone": req.phone,
        "shift_start": req.shift_start.isoformat(),
        "shift_end": req.shift_end.isoformat(),
        "role": req.role,
        "status": req.status.value,
        "created_at": req.created_at.isoformat(),
        "updated_at": req.updated_at.isoformat(),
        "sent_at": req.sent_at.isoformat() if req.sent_at else None,
        "responded_at": req.responded_at.isoformat() if req.responded_at else None,
        "message_body": req.message_body,
        "response_text": req.response_text,
    }


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@router.post("/call-in")
async def create_call_in(body: dict) -> dict:
    """Create and send a call-in SMS.

    Body:
    {
        "venue_id": str,
        "employee_id": str,
        "employee_name": str,
        "phone": str,  # must start with + or 0, 8+ digits
        "shift_start": ISO-8601 datetime,
        "shift_end": ISO-8601 datetime,
        "role": str (optional),
        "venue_name": str (optional)
    }

    Returns the created CallInRequest.
    """
    # Extract and validate
    venue_id = body.get("venue_id", "").strip()
    employee_id = body.get("employee_id", "").strip()
    employee_name = body.get("employee_name", "").strip()
    phone = body.get("phone", "").strip()
    shift_start_str = body.get("shift_start")
    shift_end_str = body.get("shift_end")
    role = body.get("role")
    venue_name = body.get("venue_name")

    if not all([venue_id, employee_id, employee_name, phone, shift_start_str, shift_end_str]):
        raise HTTPException(status_code=400, detail="Missing required fields")

    _validate_phone(phone)

    # Parse datetimes
    try:
        shift_start = datetime.fromisoformat(shift_start_str)
        shift_end = datetime.fromisoformat(shift_end_str)
    except (ValueError, TypeError):
        raise HTTPException(status_code=400, detail="Invalid datetime format")

    _validate_shift_times(shift_start, shift_end)

    # Create and send
    service = get_service()
    try:
        req = await service.create_and_send(
            venue_id=venue_id,
            employee_id=employee_id,
            employee_name=employee_name,
            phone=phone,
            shift_start=shift_start,
            shift_end=shift_end,
            role=role,
            venue_name=venue_name,
        )
        return _call_in_to_dict(req)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to send SMS: {str(e)}")


@router.get("/call-in/{venue_id}")
async def list_call_ins(venue_id: str) -> dict:
    """List all call-in requests for a venue, sorted newest first."""
    store = get_store()
    reqs = store.list_for_venue(venue_id)
    return {
        "venue_id": venue_id,
        "count": len(reqs),
        "requests": [_call_in_to_dict(r) for r in reqs],
    }


@router.post("/call-in/webhook/inbound")
async def handle_inbound_sms(body: dict) -> dict:
    """Handle inbound SMS reply.

    Body:
    {
        "from": phone,  # the sender's phone number
        "body": text    # the message text
    }

    Returns 200 with the matched and updated request, or
    {matched: false} if no PENDING/SENT request found for that phone.
    """
    phone = body.get("from", "").strip()
    text = body.get("body", "").strip()

    if not phone or not text:
        raise HTTPException(status_code=400, detail="Missing 'from' or 'body'")

    service = get_service()
    req = service.handle_inbound(phone, text)

    if not req:
        return {"matched": False}

    return {
        "matched": True,
        "request": _call_in_to_dict(req),
    }
