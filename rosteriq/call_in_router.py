"""FastAPI router for staff call-in SMS flow.

Endpoints:
- POST /api/v1/call-in → create and send call-in SMS
- GET /api/v1/call-in/{venue_id} → list all call-ins for venue
- POST /api/v1/call-in/webhook/inbound → handle inbound SMS replies (deprecated, demo mode)
- POST /api/v1/call-in/webhook/twilio → Twilio production webhook (signature verified)
"""
import logging
import os
import re
from datetime import datetime, timezone
from typing import Optional
from urllib.parse import urlparse, urlencode

from fastapi import APIRouter, HTTPException, Request

from rosteriq.call_in import (
    CallInRequest,
    CallInStatus,
    get_service,
    get_store,
    verify_twilio_signature,
)

# Auth gating — fall back to no-op in demo/sandbox when auth stack unavailable
try:
    from rosteriq.auth import require_access, AccessLevel  # type: ignore
except Exception:  # pragma: no cover — demo/sandbox path
    require_access = None  # type: ignore
    AccessLevel = None  # type: ignore


async def _gate(request: Request, level_name: str) -> None:
    """Apply role gating if auth stack is present; no-op in demo."""
    if require_access is None or AccessLevel is None:
        return
    level = getattr(AccessLevel, level_name)
    await require_access(level)(request=request)


logger = logging.getLogger(__name__)

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
async def create_call_in(body: dict, request: Request) -> dict:
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
    await _gate(request, "L1_SUPERVISOR")
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
async def list_call_ins(venue_id: str, request: Request) -> dict:
    """List all call-in requests for a venue, sorted newest first."""
    await _gate(request, "L1_SUPERVISOR")
    store = get_store()
    reqs = store.list_for_venue(venue_id)
    return {
        "venue_id": venue_id,
        "count": len(reqs),
        "requests": [_call_in_to_dict(r) for r in reqs],
    }


@router.post("/call-in/webhook/inbound")
async def handle_inbound_sms(body: dict) -> dict:
    """Handle inbound SMS reply (DEPRECATED - demo mode only).

    This endpoint accepts plain JSON for testing/demo. Production uses
    /api/v1/call-in/webhook/twilio which verifies Twilio signatures.

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


@router.post("/call-in/webhook/twilio")
async def handle_twilio_webhook(request: Request) -> dict:
    """Handle inbound SMS from Twilio webhook (production).

    Twilio sends x-www-form-urlencoded POST with fields:
    - From: sender's phone number
    - Body: message text
    - MessageSid: unique message ID
    - To: our Twilio number
    etc.

    Signature is in X-Twilio-Signature header.

    Rejects requests > 1KB (Twilio bodies are small).
    Rejects invalid signatures (403).
    If TWILIO_AUTH_TOKEN not set, accepts in demo mode with WARNING log.

    Returns 200 {matched: true, request: ...} or {matched: false}.
    """
    # Rate-limit-ish: reject body > 1KB
    body_bytes = await request.body()
    if len(body_bytes) > 1024:
        raise HTTPException(status_code=400, detail="Request body too large")

    # Read form data
    try:
        form = await request.form()
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid form data: {str(e)}")

    # Extract Twilio signature
    signature_header = request.headers.get("X-Twilio-Signature", "")

    # Build form params dict (form returns a MultiDict-like, convert to dict)
    params = dict(form)

    # Get auth token from env
    auth_token = os.getenv("TWILIO_AUTH_TOKEN")

    # Verify signature
    if auth_token:
        # Production: build canonical URL from request
        url = str(request.url)
        if not verify_twilio_signature(signature_header, url, params, auth_token):
            raise HTTPException(status_code=403, detail="Invalid Twilio signature")
    else:
        # Demo mode: no auth token set, accept request but warn
        logger.warning(
            "TWILIO_AUTH_TOKEN not set; accepting webhook without signature "
            "verification (demo mode)"
        )

    # Extract From and Body from form params
    phone = params.get("From", "").strip()
    text = params.get("Body", "").strip()

    if not phone or not text:
        raise HTTPException(status_code=400, detail="Missing From or Body")

    # Delegate to service
    service = get_service()
    req = service.handle_inbound(phone, text)

    if not req:
        return {"matched": False}

    return {
        "matched": True,
        "request": _call_in_to_dict(req),
    }
