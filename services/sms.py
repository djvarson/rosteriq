"""
RosterIQ SMS Notification Service

Handles SMS notifications via Twilio for shift reminders, swap updates,
roster publications, and urgent compliance alerts. Gracefully degrades when
Twilio is not configured.

Usage:
    from rosteriq.services.sms import get_sms_service
    sms = get_sms_service()
    await sms.send_shift_reminder(employee_dict, shift_dict)
"""

import os
import logging
import asyncio
from datetime import datetime, timedelta
from typing import Optional, Dict, Any
from time import time

logger = logging.getLogger(__name__)


class SMSService:
    """
    SMS notification service using Twilio.

    Handles rate limiting, phone number formatting, and graceful degradation
    when Twilio is not configured.
    """

    def __init__(self):
        """Initialize Twilio configuration from environment variables."""
        self.account_sid = os.environ.get("TWILIO_ACCOUNT_SID", "")
        self.auth_token = os.environ.get("TWILIO_AUTH_TOKEN", "")
        self.from_number = os.environ.get("TWILIO_FROM_NUMBER", "")

        self._twilio_client = None
        self._is_configured = bool(
            self.account_sid and self.auth_token and self.from_number
        )

        if self._is_configured:
            try:
                from twilio.rest import Client
                self._twilio_client = Client(self.account_sid, self.auth_token)
            except ImportError:
                logger.warning(
                    "Twilio credentials present but twilio-python not installed. "
                    "Install with: pip install twilio"
                )
                self._is_configured = False
            except Exception as e:
                logger.warning(f"Failed to initialize Twilio client: {e}")
                self._is_configured = False
        else:
            logger.debug(
                "Twilio not configured. SMS notifications will be skipped. "
                "Set TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN, TWILIO_FROM_NUMBER."
            )

        # Rate limiting: track (phone_number) -> last_send_time
        self._rate_limit_tracker: Dict[str, float] = {}
        self._rate_limit_window_seconds = 300  # 5 minutes

    @property
    def is_configured(self) -> bool:
        """True when Twilio creds are present and the client initialised."""
        return self._is_configured

    def _format_phone_number(self, phone: str) -> str:
        """
        Format phone number to E.164 format with +61 Australian prefix.

        Args:
            phone: Phone number in any format (e.g., "0412345678", "412345678", "+61412345678")

        Returns:
            Phone number in E.164 format (e.g., "+61412345678")
        """
        if not phone:
            return ""

        # Remove all non-digit characters
        digits = "".join(c for c in phone if c.isdigit())

        # Handle Australian numbers
        if digits.startswith("61"):
            return f"+{digits}"
        elif digits.startswith("0"):
            # Replace leading 0 with 61
            return f"+61{digits[1:]}"
        else:
            # Assume it's missing country code
            return f"+61{digits}"

    def _truncate_message(self, message: str, max_length: int = 160) -> str:
        """
        Truncate message if it exceeds max_length.

        Args:
            message: Message to truncate
            max_length: Maximum message length (default 160 for SMS)

        Returns:
            Truncated message with "..." if needed
        """
        if len(message) <= max_length:
            return message

        # Reserve 3 chars for "..."
        return message[:max_length - 3] + "..."

    def _is_within_rate_limit(self, phone: str) -> bool:
        """
        Check if a phone number is within rate limit.

        Allows 1 SMS per phone number per 5 minutes.

        Args:
            phone: Formatted phone number

        Returns:
            True if within limit, False if rate limited
        """
        now = time()
        last_send = self._rate_limit_tracker.get(phone, 0)

        if now - last_send < self._rate_limit_window_seconds:
            return False

        self._rate_limit_tracker[phone] = now
        return True

    def _should_send(self, phone: str) -> bool:
        """Check if SMS should be sent (configured + rate limit + number valid)."""
        if not self._is_configured:
            return False

        if not phone:
            return False

        if not self._is_within_rate_limit(phone):
            logger.debug(f"Rate limit exceeded for {phone}")
            return False

        return True

    async def send_sms(self, to_number: str, message: str) -> bool:
        """
        Send SMS message via Twilio.

        Args:
            to_number: Recipient phone number (any format)
            message: Message content

        Returns:
            True if sent successfully, False otherwise
        """
        if not self._should_send(to_number):
            return False

        try:
            formatted_number = self._format_phone_number(to_number)
            if not formatted_number:
                logger.warning(f"Invalid phone number format: {to_number}")
                return False

            truncated_message = self._truncate_message(message)

            # Run Twilio send in thread pool to avoid blocking
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(
                None,
                self._send_twilio,
                formatted_number,
                truncated_message,
            )

            logger.info(f"SMS sent to {formatted_number}")
            return True

        except Exception as e:
            logger.error(f"Failed to send SMS to {to_number}: {e}")
            return False

    def _send_twilio(self, to_number: str, message: str):
        """Send SMS via Twilio (blocking)."""
        if not self._twilio_client:
            raise RuntimeError("Twilio client not initialized")

        self._twilio_client.messages.create(
            body=message,
            from_=self.from_number,
            to=to_number,
        )

    async def send_shift_reminder(
        self,
        employee: Dict[str, Any],
        shift: Dict[str, Any],
    ) -> bool:
        """
        Send shift reminder 2 hours before start.

        Args:
            employee: Employee dict with 'phone' key
            shift: Shift dict with 'venue_name', 'start_time', 'end_time'

        Returns:
            True if sent successfully
        """
        phone = employee.get("phone", "")
        if not phone:
            logger.debug(f"No phone number for employee {employee.get('id')}")
            return False

        venue_name = shift.get("venue_name", "Unknown Venue")
        start_time = shift.get("start_time", "")
        end_time = shift.get("end_time", "")

        message = (
            f"Reminder: You have a shift at {venue_name} "
            f"from {start_time} to {end_time} in 2 hours."
        )

        return await self.send_sms(phone, message)

    async def send_swap_notification(
        self,
        employee: Dict[str, Any],
        swap: Dict[str, Any],
    ) -> bool:
        """
        Send shift swap status notification.

        Args:
            employee: Employee dict with 'phone' key
            swap: Swap dict with 'status' key (approved/rejected)

        Returns:
            True if sent successfully
        """
        phone = employee.get("phone", "")
        if not phone:
            logger.debug(f"No phone number for employee {employee.get('id')}")
            return False

        status = swap.get("status", "pending").lower()
        status_text = "approved" if status == "approved" else "rejected"

        message = f"Your shift swap request was {status_text}."

        return await self.send_sms(phone, message)

    async def send_roster_published(
        self,
        employee: Dict[str, Any],
        venue_name: str,
        week_start: str,
    ) -> bool:
        """
        Send roster published notification.

        Args:
            employee: Employee dict with 'phone' key
            venue_name: Name of venue
            week_start: Start date of week (ISO format)

        Returns:
            True if sent successfully
        """
        phone = employee.get("phone", "")
        if not phone:
            logger.debug(f"No phone number for employee {employee.get('id')}")
            return False

        message = f"New roster published for {venue_name} week of {week_start}."

        return await self.send_sms(phone, message)

    async def send_urgent_alert(
        self,
        phone: str,
        message: str,
    ) -> bool:
        """
        Send urgent compliance/alert SMS.

        Used for critical alerts like compliance violations that need
        immediate attention.

        Args:
            phone: Phone number to send to
            message: Alert message

        Returns:
            True if sent successfully
        """
        return await self.send_sms(phone, message)


# Singleton instance
_sms_service: Optional[SMSService] = None


def get_sms_service() -> SMSService:
    """Get or create the SMS service singleton."""
    global _sms_service
    if _sms_service is None:
        _sms_service = SMSService()
    return _sms_service
