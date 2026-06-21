"""
RosterIQ Message Template Service

Pre-built messaging templates for common roster events (shift changes, swaps,
approvals, alerts). Supports venue-specific customization and multi-channel
rendering (email, SMS, push). Uses {variable_name} syntax with AU formatting.

Usage:
    from rosteriq.services.message_templates import get_message_template_service
    svc = get_message_template_service()
    rendered = svc.render_template("ROSTER_PUBLISHED", {
        "employee_name": "John",
        "week_start": date(2026, 4, 27),
        "week_end": date(2026, 5, 3),
        "venue_name": "The Griffin",
        "shift_count": 4,
        "total_hours": 32,
    })
    await svc.send_from_template(
        "ROSTER_PUBLISHED",
        variables={...},
        recipients=["john@example.com"],
        channels=["email", "sms"]
    )
"""

import re
import logging
from dataclasses import dataclass, field
from datetime import datetime, date, time
from decimal import Decimal
from typing import Optional, List, Dict, Any
from enum import Enum

from rosteriq.database import get_db
from rosteriq.services.notification_hub import get_notification_hub

logger = logging.getLogger(__name__)


# ============================================================================
# Enums
# ============================================================================

class TemplateEventType(str, Enum):
    """Event types that trigger messages."""
    ROSTER_PUBLISHED = "ROSTER_PUBLISHED"
    SHIFT_CHANGED = "SHIFT_CHANGED"
    SHIFT_CANCELLED = "SHIFT_CANCELLED"
    SWAP_REQUEST = "SWAP_REQUEST"
    APPROVAL_NEEDED = "APPROVAL_NEEDED"
    OVERTIME_ALERT = "OVERTIME_ALERT"
    FATIGUE_WARNING = "FATIGUE_WARNING"
    SURGE_CALLIN = "SURGE_CALLIN"
    BREAK_REMINDER = "BREAK_REMINDER"
    WELCOME = "WELCOME"


# ============================================================================
# Data Classes
# ============================================================================

@dataclass
class MessageTemplate:
    """Template definition with multi-channel content."""
    id: str
    name: str
    event_type: str
    email_subject: str
    email_body: str
    sms_body: str
    push_title: str
    push_body: str
    variables_schema: List[str]
    is_customised: bool = False
    venue_id: Optional[str] = None


@dataclass
class RenderedMessage:
    """Rendered message across all channels."""
    email_subject: str
    email_body: str
    sms_text: str
    push_title: str
    push_body: str
    rendered_at: datetime = field(default_factory=datetime.utcnow)


@dataclass
class SendResult:
    """Result of sending a message."""
    success: bool
    message_id: Optional[str] = None
    channel_results: Dict[str, bool] = field(default_factory=dict)
    error: Optional[str] = None
    sent_count: int = 0
    failed_count: int = 0


# ============================================================================
# Default Templates
# ============================================================================

DEFAULT_TEMPLATES = {
    "ROSTER_PUBLISHED": MessageTemplate(
        id="ROSTER_PUBLISHED",
        name="Roster Published",
        event_type=TemplateEventType.ROSTER_PUBLISHED.value,
        email_subject="Your roster for {week_start} to {week_end} is live",
        email_body="""Hi {employee_name},

Your roster for {week_start} to {week_end} at {venue_name} has been published.

You have {shift_count} shifts totalling {total_hours} hours.

Log in to the RosterIQ app to view the full details.

Cheers,
The {venue_name} team""",
        sms_body="Hi {employee_name}, your roster for {week_start}-{week_end} at {venue_name} is live. {shift_count} shifts, {total_hours}hrs. Check the app.",
        push_title="Roster Published",
        push_body="{venue_name}: Your {week_start} roster is live ({shift_count} shifts, {total_hours}hrs)",
        variables_schema=[
            "employee_name", "week_start", "week_end", "venue_name",
            "shift_count", "total_hours"
        ]
    ),

    "SHIFT_CHANGED": MessageTemplate(
        id="SHIFT_CHANGED",
        name="Shift Changed",
        event_type=TemplateEventType.SHIFT_CHANGED.value,
        email_subject="Your shift on {date} has been updated",
        email_body="""Hi {employee_name},

Your shift on {date} at {venue_name} has been updated.

New time: {start_time} - {end_time}
Role: {role}

If you have any questions, please contact your manager.

Cheers,
The {venue_name} team""",
        sms_body="Hi {employee_name}, your shift on {date} at {venue_name} changed to {start_time}-{end_time} ({role}).",
        push_title="Shift Updated",
        push_body="{date} at {venue_name}: Now {start_time}-{end_time} ({role})",
        variables_schema=[
            "employee_name", "date", "venue_name", "start_time", "end_time", "role"
        ]
    ),

    "SHIFT_CANCELLED": MessageTemplate(
        id="SHIFT_CANCELLED",
        name="Shift Cancelled",
        event_type=TemplateEventType.SHIFT_CANCELLED.value,
        email_subject="Your shift on {date} at {venue_name} has been cancelled",
        email_body="""Hi {employee_name},

Unfortunately, your shift on {date} ({start_time}-{end_time}) at {venue_name} has been cancelled.

If you have any questions, please contact your manager.

Cheers,
The {venue_name} team""",
        sms_body="Hi {employee_name}, your shift on {date} ({start_time}-{end_time}) at {venue_name} has been cancelled.",
        push_title="Shift Cancelled",
        push_body="{venue_name}: Your {date} shift ({start_time}-{end_time}) cancelled",
        variables_schema=[
            "employee_name", "date", "start_time", "end_time", "venue_name"
        ]
    ),

    "SWAP_REQUEST": MessageTemplate(
        id="SWAP_REQUEST",
        name="Swap Request",
        event_type=TemplateEventType.SWAP_REQUEST.value,
        email_subject="{requester_name} wants to swap a shift with you",
        email_body="""Hi {employee_name},

{requester_name} has requested to swap their shift with you.

Their shift: {swap_date}
Your shift: {your_date}

Reply to the app to accept or decline.

Cheers,
RosterIQ""",
        sms_body="Hi {employee_name}, {requester_name} wants to swap their {swap_date} shift with your {your_date} shift. Reply YES or NO.",
        push_title="Shift Swap Request",
        push_body="{requester_name} wants to swap {swap_date} with your {your_date}",
        variables_schema=[
            "employee_name", "requester_name", "swap_date", "your_date"
        ]
    ),

    "APPROVAL_NEEDED": MessageTemplate(
        id="APPROVAL_NEEDED",
        name="Roster Approval Needed",
        event_type=TemplateEventType.APPROVAL_NEEDED.value,
        email_subject="Roster for {venue_name} (week of {week_start}) needs your approval",
        email_body="""Hi {manager_name},

The roster for {venue_name} for the week of {week_start} is ready for approval.

Summary:
- {shift_count} shifts
- Total cost: ${total_cost}
- Staff required: {staff_count}

Log in to review and approve.

Cheers,
RosterIQ""",
        sms_body="Hi {manager_name}, roster for {venue_name} (week {week_start}) ready for approval. {shift_count} shifts, ${total_cost}.",
        push_title="Roster Approval Required",
        push_body="{venue_name}: {shift_count} shifts ({week_start}) await approval, ${total_cost}",
        variables_schema=[
            "manager_name", "venue_name", "week_start", "shift_count",
            "total_cost", "staff_count"
        ]
    ),

    "OVERTIME_ALERT": MessageTemplate(
        id="OVERTIME_ALERT",
        name="Overtime Alert",
        event_type=TemplateEventType.OVERTIME_ALERT.value,
        email_subject="Overtime alert for {employee_name}",
        email_body="""Alert: {employee_name} is approaching overtime.

Current hours this week: {current_hours}
Maximum allowed: {max_hours}
Venue: {venue_name}

Consider reducing upcoming shifts or discussing with the employee.

Cheers,
RosterIQ""",
        sms_body="Alert: {employee_name} approaching overtime ({current_hours}/{max_hours}hrs) at {venue_name}.",
        push_title="Overtime Alert",
        push_body="{employee_name}: {current_hours}/{max_hours} hrs this week at {venue_name}",
        variables_schema=[
            "employee_name", "current_hours", "max_hours", "venue_name"
        ]
    ),

    "FATIGUE_WARNING": MessageTemplate(
        id="FATIGUE_WARNING",
        name="Fatigue Warning",
        event_type=TemplateEventType.FATIGUE_WARNING.value,
        email_subject="Fatigue risk warning for {employee_name}",
        email_body="""Warning: {employee_name} has a fatigue risk score of {risk_score}/100.

High fatigue risk can impact work quality and safety.

Consider reducing upcoming shifts or reviewing their availability.

Contact the employee to discuss if needed.

Cheers,
RosterIQ""",
        sms_body="Warning: {employee_name} fatigue risk {risk_score}/100. Consider reducing shifts.",
        push_title="Fatigue Risk Alert",
        push_body="{employee_name}: Fatigue risk {risk_score}/100",
        variables_schema=[
            "employee_name", "risk_score"
        ]
    ),

    "SURGE_CALLIN": MessageTemplate(
        id="SURGE_CALLIN",
        name="Surge Call-In",
        event_type=TemplateEventType.SURGE_CALLIN.value,
        email_subject="We need you at {venue_name}!",
        email_body="""Hi {employee_name},

We have a busy period coming up at {venue_name}!

We need {additional_staff} more staff from {start_time}.

Can you come in? Reply YES to confirm, or let us know if you're unavailable.

Cheers,
The {venue_name} team""",
        sms_body="Hi {employee_name}, busy period at {venue_name}! Need {additional_staff} staff from {start_time}. Can you come in? Reply YES.",
        push_title="Call-In Needed",
        push_body="{venue_name} needs you from {start_time} (Reply YES to confirm)",
        variables_schema=[
            "employee_name", "venue_name", "additional_staff", "start_time"
        ]
    ),

    "BREAK_REMINDER": MessageTemplate(
        id="BREAK_REMINDER",
        name="Break Reminder",
        event_type=TemplateEventType.BREAK_REMINDER.value,
        email_subject="Break reminder for {employee_name}",
        email_body="""Hi {employee_name},

You're due for a {break_minutes} minute break at {break_time}.

Make sure you take time to rest and recharge.

Cheers,
The {venue_name} team""",
        sms_body="Hi {employee_name}, you're due for a {break_minutes}min break at {break_time} at {venue_name}.",
        push_title="Break Due",
        push_body="{break_minutes}min break at {break_time}",
        variables_schema=[
            "employee_name", "break_minutes", "break_time", "venue_name"
        ]
    ),

    "WELCOME": MessageTemplate(
        id="WELCOME",
        name="Welcome to Venue",
        event_type=TemplateEventType.WELCOME.value,
        email_subject="Welcome to {venue_name}, {employee_name}!",
        email_body="""Hi {employee_name},

Welcome to {venue_name}!

Your onboarding checklist is ready in the RosterIQ app. Log in to get started:

1. Complete your profile
2. Set your availability
3. Review venue policies
4. Download the mobile app

If you have any questions, reach out to your manager.

Cheers,
The {venue_name} team""",
        sms_body="Welcome to {venue_name}, {employee_name}! Log in to RosterIQ to start your onboarding.",
        push_title="Welcome to {venue_name}",
        push_body="Your onboarding checklist is ready",
        variables_schema=[
            "employee_name", "venue_name"
        ]
    ),
}


# ============================================================================
# Service Class
# ============================================================================

class MessageTemplateService:
    """
    Manages message templates for roster events.

    Supports:
    - Pre-built templates for 10 common events
    - Venue-specific customization and overrides
    - Multi-channel rendering (email, SMS, push)
    - Variable substitution with AU formatting
    - Dispatch via notification hub
    """

    def __init__(self):
        """Initialize template service."""
        self._db = get_db()
        self._hub = get_notification_hub()
        self._templates = {k: v for k, v in DEFAULT_TEMPLATES.items()}

    # ========================================================================
    # Template Management
    # ========================================================================

    def get_template(
        self,
        template_id: str,
        venue_id: Optional[str] = None
    ) -> Optional[MessageTemplate]:
        """
        Get a template by ID.

        Checks venue-specific overrides first, falls back to default.

        Args:
            template_id: Template identifier (e.g., "ROSTER_PUBLISHED")
            venue_id: Optional venue ID for venue-specific template

        Returns:
            MessageTemplate or None if not found
        """
        if not template_id or template_id not in self._templates:
            return None

        # TODO: In production, check database for venue-specific overrides
        # For now, return default
        return self._templates[template_id]

    def list_templates(
        self,
        venue_id: Optional[str] = None
    ) -> List[MessageTemplate]:
        """
        List all available templates.

        Args:
            venue_id: Optional venue ID to include customization status

        Returns:
            List of MessageTemplate objects
        """
        templates = list(self._templates.values())

        # TODO: In production, enrich with venue customization status
        # For now, return defaults
        return templates

    def update_template(
        self,
        venue_id: str,
        template_id: str,
        customisations: Dict[str, str]
    ) -> Optional[MessageTemplate]:
        """
        Create or update a venue-specific template customization.

        Customizable fields: email_subject, email_body, sms_body,
        push_title, push_body.

        Args:
            venue_id: Venue ID for customization scope
            template_id: Template to customize
            customisations: Dict of field -> new_value updates

        Returns:
            Updated MessageTemplate or None if template not found
        """
        template = self.get_template(template_id)
        if not template:
            return None

        # Create customized copy
        customised = MessageTemplate(
            id=template.id,
            name=template.name,
            event_type=template.event_type,
            email_subject=customisations.get("email_subject", template.email_subject),
            email_body=customisations.get("email_body", template.email_body),
            sms_body=customisations.get("sms_body", template.sms_body),
            push_title=customisations.get("push_title", template.push_title),
            push_body=customisations.get("push_body", template.push_body),
            variables_schema=template.variables_schema,
            is_customised=True,
            venue_id=venue_id,
        )

        # TODO: Save to database
        return customised

    def reset_template(self, venue_id: str, template_id: str) -> bool:
        """
        Reset a venue-specific template to default.

        Args:
            venue_id: Venue ID
            template_id: Template to reset

        Returns:
            True if reset successful
        """
        # TODO: Delete venue customization from database
        return True

    # ========================================================================
    # Rendering
    # ========================================================================

    def _format_au_value(self, value: Any) -> str:
        """
        Format a value for Australian locale.

        Dates: DD/MM/YYYY
        Currency: $X,XXX.XX
        Times: HH:MM

        Args:
            value: Value to format

        Returns:
            Formatted string
        """
        if isinstance(value, date) and not isinstance(value, datetime):
            return value.strftime("%d/%m/%Y")
        elif isinstance(value, datetime):
            return value.strftime("%d/%m/%Y %H:%M")
        elif isinstance(value, time):
            return value.strftime("%H:%M")
        elif isinstance(value, Decimal):
            # Format currency
            return "${:,.2f}".format(value)
        elif isinstance(value, (int, float)) and isinstance(value, (int, float)):
            # Check if looks like currency (has decimal context)
            if isinstance(value, float):
                return "${:,.2f}".format(value)
            return str(value)
        return str(value)

    def _substitute_variables(
        self,
        text: str,
        variables: Dict[str, Any]
    ) -> str:
        """
        Substitute {variable_name} placeholders in text.

        Uses AU formatting for dates, currency, times.
        Raises ValueError if required variables missing.

        Args:
            text: Template text with {variable_name} placeholders
            variables: Dict of variable_name -> value

        Returns:
            Text with variables substituted
        """
        # Find all placeholders
        placeholders = set(re.findall(r'\{(\w+)\}', text))

        # Format all variables
        formatted_vars = {}
        for key, value in variables.items():
            formatted_vars[key] = self._format_au_value(value)

        # Check for missing required variables
        missing = placeholders - set(formatted_vars.keys())
        if missing:
            raise ValueError(f"Missing required variables: {', '.join(missing)}")

        # Substitute
        result = text
        for key, value in formatted_vars.items():
            result = result.replace(f"{{{key}}}", value)

        return result

    def render_template(
        self,
        template_id: str,
        variables: Dict[str, Any],
        venue_id: Optional[str] = None
    ) -> Optional[RenderedMessage]:
        """
        Render a template with variables for all channels.

        Validates all required variables present. Applies AU formatting.

        Args:
            template_id: Template to render
            variables: Variable values
            venue_id: Optional venue ID for customized template

        Returns:
            RenderedMessage or None if template not found

        Raises:
            ValueError: If required variables missing
        """
        template = self.get_template(template_id, venue_id)
        if not template:
            return None

        try:
            email_subject = self._substitute_variables(
                template.email_subject, variables
            )
            email_body = self._substitute_variables(
                template.email_body, variables
            )
            sms_text = self._substitute_variables(
                template.sms_body, variables
            )
            push_title = self._substitute_variables(
                template.push_title, variables
            )
            push_body = self._substitute_variables(
                template.push_body, variables
            )

            return RenderedMessage(
                email_subject=email_subject,
                email_body=email_body,
                sms_text=sms_text,
                push_title=push_title,
                push_body=push_body,
            )
        except ValueError as e:
            logger.error(f"Failed to render template {template_id}: {e}")
            raise

    # ========================================================================
    # Preview & Send
    # ========================================================================

    def preview(
        self,
        template_id: str,
        variables: Dict[str, Any],
        venue_id: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Preview a rendered template across all channels.

        Does not send any messages.

        Args:
            template_id: Template to preview
            variables: Variable values
            venue_id: Optional venue ID for customized template

        Returns:
            Dict with 'email', 'sms', 'push' keys or None if not found
        """
        rendered = self.render_template(template_id, variables, venue_id)
        if not rendered:
            return None

        return {
            "email": {
                "subject": rendered.email_subject,
                "body": rendered.email_body,
            },
            "sms": {
                "text": rendered.sms_text,
            },
            "push": {
                "title": rendered.push_title,
                "body": rendered.push_body,
            },
            "rendered_at": rendered.rendered_at.isoformat(),
        }

    async def send_from_template(
        self,
        template_id: str,
        variables: Dict[str, Any],
        recipients: List[str],
        channels: List[str],
        venue_id: Optional[str] = None
    ) -> SendResult:
        """
        Render and dispatch a message to recipients.

        Sends via notification hub respecting user preferences.

        Args:
            template_id: Template to send
            variables: Variable values
            recipients: List of recipient email/phone
            channels: List of channels ("email", "sms", "push", "websocket")
            venue_id: Optional venue ID for customized template

        Returns:
            SendResult with success status and per-channel results
        """
        rendered = self.render_template(template_id, variables, venue_id)
        if not rendered:
            return SendResult(
                success=False,
                error=f"Template not found: {template_id}"
            )

        # Dispatch is not yet wired to the notification hub. Previously this LOGGED
        # the message and returned success=True with sent_count=len(recipients) —
        # i.e. it claimed to have messaged staff while sending nothing. Until it's
        # wired, return an HONEST failure (success=False, nothing sent) so callers
        # don't believe recipients were notified.
        logger.warning(
            f"send_from_template({template_id}) not dispatched — notification hub "
            f"wiring pending; {len(recipients)} recipient(s) NOT messaged."
        )
        return SendResult(
            success=False,
            sent_count=0,
            failed_count=len(recipients),
            error="Template sending is not yet wired to the notification hub — no messages were sent.",
        )


# ============================================================================
# Singleton
# ============================================================================

_service_instance: Optional[MessageTemplateService] = None


def get_message_template_service() -> MessageTemplateService:
    """Get or create the message template service singleton."""
    global _service_instance
    if _service_instance is None:
        _service_instance = MessageTemplateService()
    return _service_instance
