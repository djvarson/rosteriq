"""Email provider abstraction — send briefs via email.

Pure stdlib + lazy httpx. Follows the same pattern as call_in.py's SMS providers.

Implementations:
- DemoEmailProvider: logs to stdout, returns demo receipt
- SendGridEmailProvider: POST to SendGrid API with bearer auth

Env vars:
- ROSTERIQ_EMAIL_BACKEND: "demo" (default) or "sendgrid"
- SENDGRID_API_KEY: API key for SendGrid (required when backend=sendgrid)
- SENDGRID_FROM: From address (required when backend=sendgrid)
"""
from __future__ import annotations

import os
import uuid
from abc import ABC, abstractmethod
from typing import Optional, Dict, Any


class EmailProviderError(Exception):
    """Raised when email provider fails to send."""
    pass


class EmailProvider(ABC):
    """Abstract base for email sending."""

    @abstractmethod
    async def send(
        self,
        to: str,
        subject: str,
        body_text: str,
        body_html: Optional[str] = None,
    ) -> dict:
        """Send an email. Returns a provider receipt dict.

        Args:
            to: Recipient email address
            subject: Email subject line
            body_text: Plain text body
            body_html: Optional HTML body

        Returns:
            Receipt dict with at least id, status, to, subject keys.

        Raises:
            EmailProviderError on send failure.
        """
        raise NotImplementedError


class DemoEmailProvider(EmailProvider):
    """No-op email provider for demo mode."""

    async def send(
        self,
        to: str,
        subject: str,
        body_text: str,
        body_html: Optional[str] = None,
    ) -> dict:
        """Simulate sending; return a demo receipt."""
        return {
            "id": f"email_{uuid.uuid4().hex[:12]}",
            "status": "queued",
            "to": to,
            "subject": subject,
            "body_text_length": len(body_text or ""),
            "body_html_present": bool(body_html),
        }


class SendGridEmailProvider(EmailProvider):
    """SendGrid email provider.

    Reads credentials from env vars or accepts explicit kwargs.
    Kwargs win over env vars.

    Requires SENDGRID_API_KEY and SENDGRID_FROM.
    If require_credentials=True (default), raises ValueError if none configured.
    """

    def __init__(
        self,
        api_key: Optional[str] = None,
        sendgrid_from: Optional[str] = None,
        require_credentials: bool = True,
    ):
        self.api_key = api_key or os.getenv("SENDGRID_API_KEY")
        self.sendgrid_from = sendgrid_from or os.getenv("SENDGRID_FROM")

        if require_credentials and not (self.api_key and self.sendgrid_from):
            raise ValueError(
                "SendGridEmailProvider requires SENDGRID_API_KEY and "
                "SENDGRID_FROM env vars or explicit kwargs"
            )

    async def send(
        self,
        to: str,
        subject: str,
        body_text: str,
        body_html: Optional[str] = None,
    ) -> dict:
        """Send email via SendGrid API.

        Makes a POST to https://api.sendgrid.com/v3/mail/send with bearer auth
        and JSON payload.

        Returns a receipt dict:
        {
            "id": message ID,
            "status": "sent" or "queued",
            "to": recipient,
            "subject": subject line,
        }

        Raises EmailProviderError on non-2xx response.
        """
        import httpx

        # Build the SendGrid payload
        payload = {
            "personalizations": [
                {
                    "to": [{"email": to}],
                    "subject": subject,
                }
            ],
            "from": {"email": self.sendgrid_from},
            "content": [
                {
                    "type": "text/plain",
                    "value": body_text,
                }
            ],
        }

        # Add HTML content if provided
        if body_html:
            payload["content"].append(
                {
                    "type": "text/html",
                    "value": body_html,
                }
            )

        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }

        async with httpx.AsyncClient() as client:
            resp = await client.post(
                "https://api.sendgrid.com/v3/mail/send",
                json=payload,
                headers=headers,
            )

            if resp.status_code < 200 or resp.status_code >= 300:
                try:
                    error_data = resp.json()
                    error_msg = error_data.get("errors", [{}])[0].get(
                        "message", str(error_data)
                    )
                except Exception:
                    error_msg = resp.text

                raise EmailProviderError(
                    f"SendGrid API error ({resp.status_code}): {error_msg}"
                )

            # SendGrid returns 202 Accepted with no body on success
            return {
                "id": f"sendgrid_{uuid.uuid4().hex[:12]}",
                "status": "sent" if resp.status_code == 200 else "queued",
                "to": to,
                "subject": subject,
                "status_code": resp.status_code,
            }


# ---------------------------------------------------------------------------
# Module singletons
# ---------------------------------------------------------------------------

_provider: Optional[EmailProvider] = None


def get_email_provider() -> EmailProvider:
    """Return the module-level singleton EmailProvider.

    Provider is selected based on ROSTERIQ_EMAIL_BACKEND env var:
    - 'demo' (default) → DemoEmailProvider
    - 'sendgrid' → SendGridEmailProvider (requires env vars)

    Falls back to DemoEmailProvider if SendGrid config is missing.
    """
    global _provider
    if _provider is None:
        backend = os.getenv("ROSTERIQ_EMAIL_BACKEND", "demo").lower()
        if backend == "sendgrid":
            try:
                _provider = SendGridEmailProvider()
            except ValueError:
                # Fall back to demo if credentials missing
                _provider = DemoEmailProvider()
        else:
            _provider = DemoEmailProvider()
    return _provider


def reset_email_provider():
    """Reset singleton. Used by tests."""
    global _provider
    _provider = None
