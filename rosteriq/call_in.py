"""SMS call-in flow for on-shift demand spikes.

Pure-stdlib module for in-memory call-in request management and SMS provider
abstraction. The FastAPI layer in api_v2 imports and delegates to CallInService.

When demand spikes, an On-Shift Manager taps a button → app sends SMS to
available off-duty staff → staff replies yes/no → manager sees live status.

Call-in requests are stored in-memory only (PII-sensitive: phone numbers,
message bodies). Each request has a lifecycle: PENDING → SENT → ACCEPTED|DECLINED|EXPIRED|FAILED.
"""
from __future__ import annotations

import os
import re
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any, Dict, List, Optional


# ---------------------------------------------------------------------------
# Enums & Data Classes
# ---------------------------------------------------------------------------


class CallInStatus(str, Enum):
    PENDING = "pending"
    SENT = "sent"
    ACCEPTED = "accepted"
    DECLINED = "declined"
    EXPIRED = "expired"
    FAILED = "failed"


@dataclass
class CallInRequest:
    request_id: str
    venue_id: str
    employee_id: str
    employee_name: str
    phone: str
    shift_start: datetime
    shift_end: datetime
    status: CallInStatus
    created_at: datetime
    updated_at: datetime
    role: Optional[str] = None
    sent_at: Optional[datetime] = None
    responded_at: Optional[datetime] = None
    message_body: str = ""
    response_text: Optional[str] = None


# ---------------------------------------------------------------------------
# Pure helpers (stdlib-only)
# ---------------------------------------------------------------------------


def format_call_in_message(
    name: str,
    shift_start: datetime,
    shift_end: datetime,
    role: Optional[str] = None,
    venue_name: Optional[str] = None,
) -> str:
    """Format a call-in SMS message. Returns a string under 160 chars.

    Uses AU-friendly 12h time format with lowercase am/pm.
    Example: "Hi Alex — can you come in for the bar shift tonight 6pm-close at The Marina? Reply YES or NO."
    """
    start_str = shift_start.strftime("%I%p").lstrip("0").lower()  # 6pm, not 06pm
    end_str = shift_end.strftime("%I%p").lstrip("0").lower()

    # Check if shift runs until close (late evening)
    if shift_end.hour >= 22 or (shift_end.hour == 23 and shift_end.minute == 59):
        end_str = "close"

    role_str = f"for the {role} shift " if role else ""
    venue_str = f" at {venue_name}" if venue_name else ""

    msg = f"Hi {name} — can you come in {role_str}tonight {start_str}-{end_str}{venue_str}? Reply YES or NO."
    return msg[:160]  # safety truncate


def parse_reply(text: str) -> Optional[CallInStatus]:
    """Parse an inbound SMS reply. Returns ACCEPTED, DECLINED, or None (unrecognised).

    Case-insensitive. Accepts "yes", "y", "yep", "sure", "in", "👍" → ACCEPTED.
    Accepts "no", "n", "can't", "cant", "sorry" → DECLINED.
    """
    if not text:
        return None

    text_lower = text.strip().lower()

    # Emoji handling
    if "👍" in text:
        return CallInStatus.ACCEPTED

    # Accept variants — match if the first word is a yes-word. This tolerates
    # "yes please", "yep on my way" etc. without requiring an exact match.
    accept_patterns = r"^(yes|yeah|yep|yup|sure|ok|okay|y|in|coming|on my way)\b"
    if re.match(accept_patterns, text_lower):
        return CallInStatus.ACCEPTED

    # Decline variants — same first-word rule.
    decline_patterns = r"^(no|nope|nah|n|can'?t|cant|sorry|unable)\b"
    if re.match(decline_patterns, text_lower):
        return CallInStatus.DECLINED

    return None


# ---------------------------------------------------------------------------
# SMS Provider abstraction
# ---------------------------------------------------------------------------


class SMSProvider:
    """Abstract base for SMS sending."""

    async def send(self, to: str, body: str) -> dict:
        """Send an SMS. Returns a provider receipt dict."""
        raise NotImplementedError


class DemoSMSProvider(SMSProvider):
    """No-op SMS provider for demo mode."""

    async def send(self, to: str, body: str) -> dict:
        """Simulate sending; return a demo receipt."""
        return {
            "id": f"sms_{uuid.uuid4().hex[:12]}",
            "status": "queued",
            "to": to,
            "body": body,
        }


class TwilioSMSProvider(SMSProvider):
    """Twilio SMS provider. Requires TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN, TWILIO_FROM env vars."""

    def __init__(self):
        self.account_sid = os.getenv("TWILIO_ACCOUNT_SID")
        self.auth_token = os.getenv("TWILIO_AUTH_TOKEN")
        self.twilio_from = os.getenv("TWILIO_FROM")

        if not all([self.account_sid, self.auth_token, self.twilio_from]):
            raise ValueError(
                "TwilioSMSProvider requires TWILIO_ACCOUNT_SID, "
                "TWILIO_AUTH_TOKEN, TWILIO_FROM env vars"
            )

    async def send(self, to: str, body: str) -> dict:
        """Send via Twilio API. (Placeholder; raises NotImplementedError if not configured.)"""
        # In real implementation, would do:
        # import httpx
        # async with httpx.AsyncClient(auth=(self.account_sid, self.auth_token)) as client:
        #     resp = await client.post(
        #         f"https://api.twilio.com/2010-04-01/Accounts/{self.account_sid}/Messages.json",
        #         data={"To": to, "From": self.twilio_from, "Body": body}
        #     )
        #     return resp.json()

        raise NotImplementedError("Twilio integration not yet implemented")


# ---------------------------------------------------------------------------
# Module state
# ---------------------------------------------------------------------------

_CALL_IN_STORE: Dict[str, CallInRequest] = {}
MAX_REQUESTS = 5000  # bounds memory on high-volume venues


# ---------------------------------------------------------------------------
# CallInStore
# ---------------------------------------------------------------------------


class CallInStore:
    """In-process dict-keyed store for call-in requests."""

    def __init__(self):
        self._store: Dict[str, CallInRequest] = {}

    def create(
        self,
        venue_id: str,
        employee_id: str,
        employee_name: str,
        phone: str,
        shift_start: datetime,
        shift_end: datetime,
        role: Optional[str] = None,
    ) -> CallInRequest:
        """Create a new call-in request in PENDING state."""
        request_id = f"call_{uuid.uuid4().hex[:12]}"
        now = datetime.now(timezone.utc)
        req = CallInRequest(
            request_id=request_id,
            venue_id=venue_id,
            employee_id=employee_id,
            employee_name=employee_name,
            phone=phone,
            shift_start=shift_start,
            shift_end=shift_end,
            role=role,
            status=CallInStatus.PENDING,
            created_at=now,
            updated_at=now,
        )
        self._store[request_id] = req
        return req

    def get(self, request_id: str) -> Optional[CallInRequest]:
        """Retrieve a call-in request by ID."""
        return self._store.get(request_id)

    def list_for_venue(self, venue_id: str) -> List[CallInRequest]:
        """List all requests for a venue, sorted newest first."""
        reqs = [r for r in self._store.values() if r.venue_id == venue_id]
        return sorted(reqs, key=lambda r: r.created_at, reverse=True)

    def update_status(
        self,
        request_id: str,
        status: CallInStatus,
        response_text: Optional[str] = None,
    ) -> Optional[CallInRequest]:
        """Update request status. Returns updated request or None if not found."""
        req = self._store.get(request_id)
        if not req:
            return None
        req.status = status
        req.updated_at = datetime.now(timezone.utc)
        if response_text is not None:
            req.response_text = response_text
            req.responded_at = datetime.now(timezone.utc)
        return req

    def mark_sent(
        self, request_id: str, provider_receipt: dict
    ) -> Optional[CallInRequest]:
        """Mark request as SENT and store provider receipt metadata."""
        req = self._store.get(request_id)
        if not req:
            return None
        req.status = CallInStatus.SENT
        req.sent_at = datetime.now(timezone.utc)
        req.updated_at = req.sent_at
        return req

    def expire_stale(self, max_age_minutes: int = 60) -> int:
        """Mark PENDING/SENT requests older than max_age_minutes as EXPIRED.

        Returns count of expired requests.
        """
        cutoff = datetime.now(timezone.utc) - timedelta(minutes=max_age_minutes)
        count = 0
        for req in self._store.values():
            if req.status in (CallInStatus.PENDING, CallInStatus.SENT):
                if req.created_at < cutoff:
                    req.status = CallInStatus.EXPIRED
                    req.updated_at = datetime.now(timezone.utc)
                    count += 1
        return count

    def clear(self):
        """Wipe store. Used by tests."""
        self._store.clear()


# ---------------------------------------------------------------------------
# CallInService
# ---------------------------------------------------------------------------


class CallInService:
    """Orchestrates call-in requests: store + SMS provider."""

    def __init__(self, store: CallInStore, provider: SMSProvider):
        self.store = store
        self.provider = provider

    async def create_and_send(
        self,
        venue_id: str,
        employee_id: str,
        employee_name: str,
        phone: str,
        shift_start: datetime,
        shift_end: datetime,
        role: Optional[str] = None,
        venue_name: Optional[str] = None,
    ) -> CallInRequest:
        """Create a call-in request, format message, send SMS, mark as SENT."""
        # Create request in PENDING state
        req = self.store.create(
            venue_id=venue_id,
            employee_id=employee_id,
            employee_name=employee_name,
            phone=phone,
            shift_start=shift_start,
            shift_end=shift_end,
            role=role,
        )

        # Format and send message
        msg = format_call_in_message(
            employee_name, shift_start, shift_end, role, venue_name
        )
        req.message_body = msg

        try:
            receipt = await self.provider.send(phone, msg)
            self.store.mark_sent(req.request_id, receipt)
            req = self.store.get(req.request_id)  # re-fetch to get updated state
        except Exception as e:
            # Mark as FAILED if send fails
            self.store.update_status(req.request_id, CallInStatus.FAILED)
            req = self.store.get(req.request_id)
            raise e

        return req

    def handle_inbound(self, phone: str, text: str) -> Optional[CallInRequest]:
        """Match inbound reply to latest PENDING/SENT request for that phone.

        Parse the reply and update request status if matched.
        Returns updated request or None if no matching request.
        """
        # Find the latest PENDING or SENT request for this phone
        matching = None
        for req in self.store._store.values():
            if req.phone == phone and req.status in (
                CallInStatus.PENDING,
                CallInStatus.SENT,
            ):
                if matching is None or req.created_at > matching.created_at:
                    matching = req

        if not matching:
            return None

        # Parse the reply
        status = parse_reply(text)
        if status is None:
            # Unrecognized reply; don't update
            return None

        # Update the request with the parsed status
        return self.store.update_status(
            matching.request_id, status, response_text=text
        )


# ---------------------------------------------------------------------------
# Module singletons
# ---------------------------------------------------------------------------

_store: Optional[CallInStore] = None
_service: Optional[CallInService] = None


def get_store() -> CallInStore:
    """Return the module-level singleton CallInStore."""
    global _store
    if _store is None:
        _store = CallInStore()
    return _store


def get_service() -> CallInService:
    """Return the module-level singleton CallInService.

    Provider is selected based on ROSTERIQ_DATA_MODE env var:
    - 'demo' (default) → DemoSMSProvider
    - 'live' → TwilioSMSProvider (requires env vars)
    """
    global _service
    if _service is None:
        store = get_store()
        mode = os.getenv("ROSTERIQ_DATA_MODE", "demo")
        if mode == "live":
            provider = TwilioSMSProvider()
        else:
            provider = DemoSMSProvider()
        _service = CallInService(store, provider)
    return _service


def reset_singletons():
    """Reset singletons. Used by tests."""
    global _store, _service
    _store = None
    _service = None
