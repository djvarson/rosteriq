"""
SMS — provider-abstracted, inert until credentials arrive.

The strategy is "Dale pastes creds into Railway and SMS lights up": nothing
here stores a credential, and with no credentials every send degrades to a
clean, honest no-op with a reason the UI can show. Two providers are
supported behind one interface, selected by environment:

    Twilio        TWILIO_ACCOUNT_SID / TWILIO_AUTH_TOKEN / TWILIO_FROM_NUMBER
    MessageMedia  MESSAGEMEDIA_API_KEY / MESSAGEMEDIA_API_SECRET
                  (optional MESSAGEMEDIA_FROM)

    SMS_PROVIDER=twilio|messagemedia forces one; unset picks whichever has
    credentials (Twilio wins a tie).

Both speak plain HTTPS via httpx with a real timeout — no vendor SDK, so
there is no "credentials present but package missing" failure mode, and a
hung provider socket cannot pin an executor thread (the old Twilio-SDK path
had no timeout at all).

Every send is recorded as an ``integration`` event (provider, outcome,
duration) with the number MASKED — phone numbers never appear whole in logs
or the event table. Rate limiting is per RECIPIENT (default one SMS per 5
minutes, ``SMS_RATE_LIMIT_SECONDS`` to change), keyed on the normalised
E.164 number so "0412 345 678" and "+61412345678" are the same person.

``send_sms()`` keeps its historical bool contract for existing callers;
``send_detailed()`` also names WHY nothing was sent ("not_configured",
"invalid_number", "rate_limited", "failed") so the announcements UI can
report "2 rate-limited" instead of a misleading "2 failed".
"""

from __future__ import annotations

import asyncio
import logging
import os
import time as _time
from typing import Any, Dict, Optional, Tuple

import httpx

logger = logging.getLogger(__name__)

SEND_TIMEOUT_S = 15.0
# Three concatenated GSM segments (160 single / 153 each concatenated). The
# old cap of 160 mangled any real announcement mid-sentence.
MAX_SMS_CHARS = 459


import re as _re


def scrub_digits(text: str) -> str:
    """Replace any 6+ digit run in provider error text — Twilio and
    MessageMedia both echo the recipient number inside error messages, which
    would defeat the masking everywhere the detail string travels."""
    return _re.sub(r"[+]?\d[\d\s\-()]{5,}\d", "[number]", text or "")


def mask_number(e164: str) -> str:
    """"+61412345678" -> "+61…678" — enough to recognise, never the number."""
    if not e164:
        return ""
    return f"{e164[:3]}…{e164[-3:]}" if len(e164) > 6 else "…"


def format_au_number(phone: str) -> str:
    """Best-effort E.164; "" when hopeless.

    Handles the formats people actually type: "0412 345 678", "+61 412…",
    the common "+61 (0)412…" (the trunk zero must be DROPPED after the
    country code — "+610412…" is not a number Twilio will deliver), and a
    foreign number given with its own +country-code, which is passed through
    rather than mangled into a fake +61.
    """
    raw = (phone or "").strip()
    if not raw:
        return ""
    digits = "".join(c for c in raw if c.isdigit())
    if len(digits) < 8:
        return ""
    # An explicit non-AU country code is respected, never rewritten to +61.
    if raw.startswith("+") and not digits.startswith("61"):
        return f"+{digits}" if 8 <= len(digits) <= 15 else ""
    if digits.startswith("61"):
        rest = digits[2:]
        if rest.startswith("0"):          # "+61 (0)412…" — drop the trunk zero
            rest = rest[1:]
        return f"+61{rest}" if len(rest) == 9 else ""
    if digits.startswith("0"):
        rest = digits[1:]
        return f"+61{rest}" if len(rest) == 9 else ""
    return f"+61{digits}" if len(digits) == 9 else ""


class SMSProvider:
    """One way to hand a message to a carrier."""

    name = "none"

    async def send(self, to_e164: str, body: str) -> Tuple[bool, str]:
        """(ok, detail) — detail is a provider message id or an error string."""
        raise NotImplementedError


class TwilioProvider(SMSProvider):
    name = "twilio"

    def __init__(self, account_sid: str, auth_token: str, from_number: str):
        self._sid = account_sid
        self._token = auth_token
        self._from = from_number

    async def send(self, to_e164: str, body: str) -> Tuple[bool, str]:
        url = f"https://api.twilio.com/2010-04-01/Accounts/{self._sid}/Messages.json"
        async with httpx.AsyncClient(timeout=SEND_TIMEOUT_S) as client:
            resp = await client.post(
                url,
                auth=(self._sid, self._token),
                data={"From": self._from, "To": to_e164, "Body": body},
            )
        if resp.status_code == 201:
            try:
                return True, str(resp.json().get("sid") or "sent")
            except Exception:  # noqa: BLE001 — a 201 is a send even if unparsable
                return True, "sent"
        try:
            detail = str(resp.json().get("message") or "")[:200]
        except Exception:  # noqa: BLE001
            detail = resp.text[:200]
        return False, f"HTTP {resp.status_code}: {scrub_digits(detail)}"


class MessageMediaProvider(SMSProvider):
    name = "messagemedia"

    def __init__(self, api_key: str, api_secret: str, from_number: str = ""):
        self._key = api_key
        self._secret = api_secret
        self._from = from_number

    async def send(self, to_e164: str, body: str) -> Tuple[bool, str]:
        message: Dict[str, Any] = {
            "content": body,
            "destination_number": to_e164,
            "format": "SMS",
        }
        if self._from:
            message["source_number"] = self._from
        async with httpx.AsyncClient(timeout=SEND_TIMEOUT_S) as client:
            resp = await client.post(
                "https://api.messagemedia.com/v1/messages",
                auth=(self._key, self._secret),
                json={"messages": [message]},
            )
        if resp.status_code == 202:
            try:
                msgs = resp.json().get("messages") or []
                return True, str((msgs[0] or {}).get("message_id") or "sent")
            except Exception:  # noqa: BLE001
                return True, "sent"
        return False, f"HTTP {resp.status_code}: {scrub_digits(resp.text[:200])}"


def _provider_from_env() -> Optional[SMSProvider]:
    """Build the configured provider, or None. Credentials are read here and
    live only inside the provider object — never logged, never returned."""
    twilio_ok = all(os.environ.get(k) for k in
                    ("TWILIO_ACCOUNT_SID", "TWILIO_AUTH_TOKEN", "TWILIO_FROM_NUMBER"))
    mm_ok = all(os.environ.get(k) for k in
                ("MESSAGEMEDIA_API_KEY", "MESSAGEMEDIA_API_SECRET"))

    choice = (os.environ.get("SMS_PROVIDER") or "").strip().lower()
    if choice == "twilio" and not twilio_ok:
        logger.warning("SMS_PROVIDER=twilio but TWILIO_* credentials are incomplete")
        return None
    if choice == "messagemedia" and not mm_ok:
        logger.warning("SMS_PROVIDER=messagemedia but MESSAGEMEDIA_* credentials are incomplete")
        return None
    if not choice:
        choice = "twilio" if twilio_ok else ("messagemedia" if mm_ok else "")

    if choice == "twilio":
        return TwilioProvider(os.environ["TWILIO_ACCOUNT_SID"],
                              os.environ["TWILIO_AUTH_TOKEN"],
                              os.environ["TWILIO_FROM_NUMBER"])
    if choice == "messagemedia":
        return MessageMediaProvider(os.environ["MESSAGEMEDIA_API_KEY"],
                                    os.environ["MESSAGEMEDIA_API_SECRET"],
                                    os.environ.get("MESSAGEMEDIA_FROM", ""))
    return None


class SMSService:
    """Provider-agnostic sending with rate limiting and honest skip reasons."""

    def __init__(self):
        self._provider = _provider_from_env()
        self._rate_window = float(os.environ.get("SMS_RATE_LIMIT_SECONDS", "300"))
        self._last_send: Dict[str, float] = {}      # E.164 -> monotonic ts

    # ------------------------------------------------------------------ state

    @property
    def is_configured(self) -> bool:
        return self._provider is not None

    def status(self) -> Dict[str, Any]:
        """What the Connections/Comms UI shows. Never contains a credential."""
        if self._provider is None:
            return {
                "configured": False,
                "provider": None,
                "setup": {
                    "twilio": ["TWILIO_ACCOUNT_SID", "TWILIO_AUTH_TOKEN", "TWILIO_FROM_NUMBER"],
                    "messagemedia": ["MESSAGEMEDIA_API_KEY", "MESSAGEMEDIA_API_SECRET"],
                    "where": "Railway → the service → Variables; redeploy applies them.",
                },
            }
        from_number = getattr(self._provider, "_from", "") or ""
        return {
            "configured": True,
            "provider": self._provider.name,
            "from": mask_number(from_number) if from_number else None,
            "rate_limit_seconds": self._rate_window,
        }

    # ------------------------------------------------------------------ sends

    def _rate_limited(self, e164: str) -> bool:
        now = _time.monotonic()
        last = self._last_send.get(e164)
        return last is not None and (now - last) < self._rate_window

    def _record_send(self, e164: str) -> None:
        """Mark the window only for a send that actually HAPPENED — recording
        on attempt meant one failed send (bad creds, provider blip) blocked
        the retry with a misleading "rate_limited" for five minutes."""
        now = _time.monotonic()
        self._last_send[e164] = now
        if len(self._last_send) > 5000:             # bound the tracker
            cutoff = now - self._rate_window
            self._last_send = {k: v for k, v in self._last_send.items() if v >= cutoff}

    async def send_detailed(self, to_number: str, message: str) -> Tuple[bool, str]:
        """(sent, reason). reason ∈ sent | not_configured | no_phone |
        invalid_number | rate_limited | failed."""
        if self._provider is None:
            return False, "not_configured"
        if not (to_number or "").strip():
            return False, "no_phone"
        e164 = format_au_number(to_number)
        if not e164:
            logger.warning(f"SMS skipped: unusable phone number ({mask_number(to_number)})")
            return False, "invalid_number"
        if self._rate_limited(e164):
            logger.info(f"SMS rate-limited for {mask_number(e164)}")
            return False, "rate_limited"

        body = message if len(message) <= MAX_SMS_CHARS else message[:MAX_SMS_CHARS - 1] + "…"
        t0 = _time.perf_counter()
        try:
            ok, detail = await self._provider.send(e164, body)
        except (httpx.TimeoutException, httpx.HTTPError) as e:
            ok, detail = False, f"{type(e).__name__}: {str(e)[:150]}"
        except Exception as e:  # noqa: BLE001 — a send must never crash the caller
            ok, detail = False, f"{type(e).__name__}: {str(e)[:150]}"
        ms = (_time.perf_counter() - t0) * 1000

        try:
            from rosteriq.services.events import integration
            integration(self._provider.name, "send_sms",
                        outcome="ok" if ok else "failed", duration_ms=ms,
                        to=mask_number(e164), chars=len(body),
                        **({} if ok else {"error": scrub_digits(detail)}))
        except Exception:  # noqa: BLE001 — recording must never break sending
            pass

        if ok:
            self._record_send(e164)
            logger.info(f"SMS sent via {self._provider.name} to {mask_number(e164)}")
            return True, "sent"
        logger.error(f"SMS failed via {self._provider.name} to {mask_number(e164)}: "
                     f"{scrub_digits(detail)}")
        return False, "failed"

    async def send_sms(self, to_number: str, message: str) -> bool:
        """Historical bool contract — see send_detailed for the reason."""
        ok, _ = await self.send_detailed(to_number, message)
        return ok

    # ------------------------------------------- convenience senders (legacy)

    async def send_shift_reminder(self, employee: Dict[str, Any], shift: Dict[str, Any]) -> bool:
        phone = employee.get("phone", "")
        if not phone:
            return False
        message = (f"Reminder: You have a shift at {shift.get('venue_name', 'your venue')} "
                   f"from {shift.get('start_time', '')} to {shift.get('end_time', '')} in 2 hours.")
        return await self.send_sms(phone, message)

    async def send_swap_notification(self, employee: Dict[str, Any], swap: Dict[str, Any]) -> bool:
        phone = employee.get("phone", "")
        if not phone:
            return False
        status_text = "approved" if (swap.get("status", "").lower() == "approved") else "rejected"
        return await self.send_sms(phone, f"Your shift swap request was {status_text}.")

    async def send_roster_published(self, employee: Dict[str, Any], venue_name: str,
                                    week_start: str) -> bool:
        phone = employee.get("phone", "")
        if not phone:
            return False
        return await self.send_sms(
            phone, f"New roster published for {venue_name} week of {week_start}.")

    async def send_urgent_alert(self, phone: str, message: str) -> bool:
        return await self.send_sms(phone, message)


_sms_service: Optional[SMSService] = None


def get_sms_service() -> SMSService:
    """Get or create the SMS service singleton."""
    global _sms_service
    if _sms_service is None:
        _sms_service = SMSService()
    return _sms_service


def reset_sms_service() -> None:
    """Forget the singleton — tests use this to re-read the environment."""
    global _sms_service
    _sms_service = None
