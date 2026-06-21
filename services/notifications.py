"""
RosterIQ Notification Service

Handles email notifications for rosters, compliance alerts, certifications,
and variance alerts. Supports async sending via SMTP with configurable
environment variables.
"""

import os
import asyncio
import smtplib
from datetime import date, datetime
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from typing import Optional, List, Dict, Any
from abc import ABC, abstractmethod

from rosteriq.models import Employee, Roster, Shift, VenueConfig
from rosteriq.services.sms import get_sms_service
from rosteriq.services.notification_preferences import get_preferences_service


class NotificationService:
    """
    Email notification service for RosterIQ.

    Sends HTML emails via SMTP with RosterIQ branding and templating.
    Supports multiple notification types with async sending.
    """

    def __init__(self):
        """Initialize SMTP configuration from environment variables."""
        self.smtp_host = os.environ.get("SMTP_HOST", "smtp.gmail.com")
        self.smtp_port = int(os.environ.get("SMTP_PORT", "587"))
        self.smtp_user = os.environ.get("SMTP_USER", "")
        self.smtp_pass = os.environ.get("SMTP_PASS", "")
        self.from_email = os.environ.get("FROM_EMAIL", "noreply@rosteriq.com")
        # Base URL for links in email templates. Reuse the centralized
        # APP_URL config so dashboard/roster links point at the deployed app
        # rather than a developer's localhost. Falls back to APP_BASE_URL,
        # then a production-safe default.
        self.base_url = self._resolve_base_url()

    @staticmethod
    def _resolve_base_url() -> str:
        """Resolve the public base URL for links in emails.

        Order of preference:
          1. APP_BASE_URL env var (explicit override)
          2. The centralized AppConfig.app_url (reads APP_URL)
          3. Production-safe default https://app.rosteriq.io
        """
        explicit = os.environ.get("APP_BASE_URL")
        if explicit:
            return explicit.rstrip("/")
        try:
            from rosteriq.services.config import get_config

            app_url = get_config().app_url
            if app_url:
                return app_url.rstrip("/")
        except Exception:
            pass
        return "https://app.rosteriq.io"

    async def send_email(
        self,
        to_email: str,
        subject: str,
        html_content: str,
    ) -> bool:
        """
        Send HTML email asynchronously.

        Args:
            to_email: Recipient email address
            subject: Email subject line
            html_content: HTML email body

        Returns:
            True if sent successfully, False otherwise
        """
        if not to_email or not self.smtp_user or not self.smtp_pass:
            return False

        try:
            msg = MIMEMultipart("alternative")
            msg["Subject"] = subject
            msg["From"] = self.from_email
            msg["To"] = to_email

            msg.attach(MIMEText(html_content, "html"))

            # Run SMTP in thread pool to avoid blocking
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(
                None,
                self._send_smtp,
                msg,
            )
            return True
        except Exception as e:
            print(f"Email send failed for {to_email}: {e}")
            return False

    def _send_smtp(self, msg):
        """Send email via SMTP (blocking)."""
        with smtplib.SMTP(self.smtp_host, self.smtp_port) as server:
            server.starttls()
            server.login(self.smtp_user, self.smtp_pass)
            server.send_message(msg)

    async def send_daily_digest(
        self,
        venue_id: str,
        venue: VenueConfig,
        manager_email: str,
        roster_shifts: List[Shift],
        expected_covers: Dict[int, float],
        weather_forecast: Optional[str] = None,
        events: Optional[List[str]] = None,
        compliance_alerts: Optional[List[str]] = None,
    ) -> bool:
        """
        Send morning daily digest email.

        Includes today's roster, expected covers, weather, events, and alerts.

        Args:
            venue_id: Venue identifier
            venue: VenueConfig object
            manager_email: Manager's email address
            roster_shifts: List of shifts for today
            expected_covers: Dict mapping hour to expected covers
            weather_forecast: Optional weather description
            events: Optional list of local events
            compliance_alerts: Optional list of compliance issues

        Returns:
            True if sent successfully
        """
        today = date.today().isoformat()

        # Build shifts table
        shifts_html = self._build_shifts_table(roster_shifts)

        # Build metrics cards
        peak_hour = max(expected_covers.items(), key=lambda x: x[1])[0] if expected_covers else 0
        peak_covers = max(expected_covers.values()) if expected_covers else 0
        avg_covers = sum(expected_covers.values()) / len(expected_covers) if expected_covers else 0

        metrics_html = f"""
        <div style="display: grid; grid-template-columns: 1fr 1fr 1fr; gap: 20px; margin: 20px 0;">
            <div style="background: #f0f4ff; padding: 15px; border-radius: 8px; text-align: center;">
                <div style="font-size: 12px; color: #666; margin-bottom: 5px;">SHIFTS TODAY</div>
                <div style="font-size: 24px; font-weight: bold; color: #3366FF;">{len(roster_shifts)}</div>
            </div>
            <div style="background: #f0f4ff; padding: 15px; border-radius: 8px; text-align: center;">
                <div style="font-size: 12px; color: #666; margin-bottom: 5px;">PEAK HOUR</div>
                <div style="font-size: 24px; font-weight: bold; color: #3366FF;">{int(peak_covers)} covers @ {peak_hour}:00</div>
            </div>
            <div style="background: #f0f4ff; padding: 15px; border-radius: 8px; text-align: center;">
                <div style="font-size: 12px; color: #666; margin-bottom: 5px;">AVG COVERS</div>
                <div style="font-size: 24px; font-weight: bold; color: #3366FF;">{int(avg_covers)}</div>
            </div>
        </div>
        """

        # Build alerts section
        alerts_html = ""
        if compliance_alerts:
            alerts_html = f"""
            <div style="background: #fff3cd; border-left: 4px solid #ff9800; padding: 15px; margin: 20px 0; border-radius: 4px;">
                <div style="font-weight: bold; color: #ff9800; margin-bottom: 10px;">Compliance Alerts</div>
                <ul style="margin: 0; padding-left: 20px;">
                    {''.join(f'<li style="margin: 5px 0;">{alert}</li>' for alert in compliance_alerts)}
                </ul>
            </div>
            """

        # Build weather/events section
        context_html = ""
        if weather_forecast or events:
            context_html = f"""
            <div style="background: #e8f4f8; padding: 15px; margin: 20px 0; border-radius: 8px;">
                <div style="font-weight: bold; color: #0288d1; margin-bottom: 10px;">Today's Context</div>
                {f'<p style="margin: 5px 0;"><strong>Weather:</strong> {weather_forecast}</p>' if weather_forecast else ''}
                {f'<p style="margin: 5px 0;"><strong>Events:</strong> {", ".join(events)}</p>' if events else ''}
            </div>
            """

        html = self._wrap_template(
            title=f"Daily Digest - {venue.name}",
            date_str=today,
            body=f"""
            <h2 style="color: #333; margin-bottom: 20px;">Good morning! Here's today at {venue.name}</h2>

            {metrics_html}

            <h3 style="color: #333; margin-top: 30px; margin-bottom: 15px;">Today's Roster</h3>
            {shifts_html}

            {context_html}

            {alerts_html}

            <div style="background: #e8f5e9; padding: 15px; margin: 20px 0; border-radius: 8px; text-align: center;">
                <a href="{self.base_url}/dashboard/{venue_id}/overview" style="background: #3366FF; color: white; padding: 10px 20px; text-decoration: none; border-radius: 4px; display: inline-block;">
                    View Full Dashboard
                </a>
            </div>
            """,
        )

        return await self.send_email(manager_email, f"Daily Digest - {venue.name}", html)

    async def send_roster_published(
        self,
        venue_id: str,
        venue: VenueConfig,
        manager_email: str,
        roster: Roster,
        employees_lookup: Dict[str, Employee],
        total_cost: Optional[float] = None,
    ) -> bool:
        """
        Notify manager when AI roster is published.

        Args:
            venue_id: Venue identifier
            venue: VenueConfig object
            manager_email: Manager's email address
            roster: Generated Roster object
            employees_lookup: Dict of employee_id -> Employee
            total_cost: Optional total labour cost

        Returns:
            True if sent successfully
        """
        shifts_by_day = {}
        for shift in roster.shifts:
            day = shift.date.strftime("%A, %b %d")
            if day not in shifts_by_day:
                shifts_by_day[day] = []
            shifts_by_day[day].append(shift)

        week_html = "".join([
            f"""
            <div style="margin: 15px 0; padding: 10px; background: #f9f9f9; border-radius: 4px;">
                <div style="font-weight: bold; color: #333; margin-bottom: 8px;">{day}</div>
                <div style="font-size: 13px; color: #666;">
                    {len(shifts)} shifts • {sum(s.net_hours for s in shifts):.1f} hours
                    {f' • ${total_cost:.2f} cost' if total_cost else ''}
                </div>
            </div>
            """
            for day, shifts in shifts_by_day.items()
        ])

        cost_html = ""
        if total_cost:
            cost_html = f"""
            <div style="background: #f0f4ff; padding: 20px; border-radius: 8px; margin: 20px 0; text-align: center;">
                <div style="font-size: 12px; color: #666; margin-bottom: 10px;">ESTIMATED LABOUR COST</div>
                <div style="font-size: 32px; font-weight: bold; color: #3366FF;">${total_cost:.2f}</div>
            </div>
            """

        html = self._wrap_template(
            title=f"Roster Published - {venue.name}",
            date_str=roster.week_start.isoformat(),
            body=f"""
            <h2 style="color: #333; margin-bottom: 20px;">New Roster Published</h2>
            <p style="color: #666; margin-bottom: 20px;">
                Your AI-optimized roster for <strong>{roster.week_start.strftime('%b %d')} - {roster.week_end.strftime('%b %d, %Y')}</strong> is ready to review and publish to staff.
            </p>

            <div style="background: #e8f5e9; padding: 20px; border-radius: 8px; margin: 20px 0;">
                <div style="font-weight: bold; color: #2e7d32; margin-bottom: 10px;">Summary</div>
                <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 15px; font-size: 14px;">
                    <div><span style="color: #666;">Total Shifts:</span> <span style="font-weight: bold;">{len(roster.shifts)}</span></div>
                    <div><span style="color: #666;">Total Hours:</span> <span style="font-weight: bold;">{roster.total_hours:.1f}</span></div>
                    <div><span style="color: #666;">Staff Members:</span> <span style="font-weight: bold;">{len(roster.employees_used)}</span></div>
                </div>
            </div>

            {cost_html}

            <h3 style="color: #333; margin-top: 30px; margin-bottom: 15px;">Week Overview</h3>
            {week_html}

            <div style="background: #e8f5e9; padding: 15px; margin: 20px 0; border-radius: 8px; text-align: center;">
                <a href="{self.base_url}/dashboard/{venue_id}/overview" style="background: #3366FF; color: white; padding: 10px 20px; text-decoration: none; border-radius: 4px; display: inline-block; margin-right: 10px;">
                    Review Roster
                </a>
                <a href="{self.base_url}/rosters/{roster.id}" style="background: #666; color: white; padding: 10px 20px; text-decoration: none; border-radius: 4px; display: inline-block;">
                    View Details
                </a>
            </div>
            """,
        )

        return await self.send_email(manager_email, f"Roster Published - {venue.name}", html)

    async def send_compliance_alert(
        self,
        venue_id: str,
        venue: VenueConfig,
        manager_email: str,
        alert_type: str,
        details: Dict[str, Any],
    ) -> bool:
        """
        Send compliance alert (break violations, fatigue, etc).

        Args:
            venue_id: Venue identifier
            venue: VenueConfig object
            manager_email: Manager's email address
            alert_type: Type of alert (break_violation, fatigue_warning, etc)
            details: Additional alert details

        Returns:
            True if sent successfully
        """
        alert_title_map = {
            "break_violation": "Break Violation Alert",
            "fatigue_warning": "Fatigue Warning",
            "consecutive_days": "Consecutive Days Limit",
            "weekly_hours": "Weekly Hours Limit",
        }

        alert_title = alert_title_map.get(alert_type, "Compliance Alert")
        alert_desc_map = {
            "break_violation": "One or more shifts do not meet rest break requirements",
            "fatigue_warning": "Staff members approaching fatigue thresholds",
            "consecutive_days": "Some staff scheduled beyond consecutive day limits",
            "weekly_hours": "Labour costs approaching budget constraints",
        }
        alert_desc = alert_desc_map.get(alert_type, "Compliance issue detected")

        details_html = "".join([
            f'<p style="margin: 8px 0;"><strong>{k}:</strong> {v}</p>'
            for k, v in details.items()
        ])

        html = self._wrap_template(
            title=alert_title,
            date_str=date.today().isoformat(),
            body=f"""
            <h2 style="color: #d32f2f; margin-bottom: 20px;">{alert_title}</h2>

            <div style="background: #ffebee; border-left: 4px solid #d32f2f; padding: 15px; margin: 20px 0; border-radius: 4px;">
                <p style="color: #d32f2f; margin: 0;">{alert_desc}</p>
            </div>

            <div style="background: #f5f5f5; padding: 15px; border-radius: 4px; margin: 20px 0;">
                <div style="font-weight: bold; color: #333; margin-bottom: 10px;">Details</div>
                {details_html}
            </div>

            <p style="color: #666; margin: 20px 0;">
                Please review your roster configuration and adjust scheduling as needed to ensure compliance.
            </p>

            <div style="background: #e8f5e9; padding: 15px; margin: 20px 0; border-radius: 8px; text-align: center;">
                <a href="{self.base_url}/dashboard/{venue_id}/overview" style="background: #3366FF; color: white; padding: 10px 20px; text-decoration: none; border-radius: 4px; display: inline-block;">
                    Review Roster
                </a>
            </div>
            """,
        )

        return await self.send_email(manager_email, alert_title, html)

    async def send_certification_expiry(
        self,
        employee: Employee,
        manager_email: str,
        cert_type: str,
        days_until: int,
    ) -> bool:
        """
        Send certification expiry warning.

        Sent at 60, 30, and 7 days before expiry.

        Args:
            employee: Employee object
            manager_email: Manager's email address
            cert_type: Type of certification (RSA, First Aid, etc)
            days_until: Days until expiry

        Returns:
            True if sent successfully
        """
        urgency = "CRITICAL" if days_until <= 7 else "URGENT" if days_until <= 30 else "REMINDER"
        color = "#d32f2f" if days_until <= 7 else "#ff9800" if days_until <= 30 else "#3366FF"

        html = self._wrap_template(
            title=f"{cert_type} Expiry Warning",
            date_str=date.today().isoformat(),
            body=f"""
            <h2 style="color: {color}; margin-bottom: 20px;">{urgency}: Certification Expiring Soon</h2>

            <div style="background: {color}14; border-left: 4px solid {color}; padding: 15px; margin: 20px 0; border-radius: 4px;">
                <p style="color: {color}; margin: 0; font-weight: bold;">{cert_type} expires in {days_until} days</p>
            </div>

            <div style="background: #f5f5f5; padding: 15px; border-radius: 4px; margin: 20px 0;">
                <div style="font-weight: bold; color: #333; margin-bottom: 10px;">Employee</div>
                <p style="margin: 5px 0;"><strong>Name:</strong> {employee.name}</p>
                <p style="margin: 5px 0;"><strong>ID:</strong> {employee.id}</p>
                <p style="margin: 5px 0;"><strong>Certification:</strong> {cert_type}</p>
                <p style="margin: 5px 0;"><strong>Expires in:</strong> {days_until} days</p>
            </div>

            <p style="color: #666; margin: 20px 0;">
                {'This is a critical expiry notice. Please arrange renewal immediately.' if days_until <= 7 else 'Please schedule renewal to avoid scheduling conflicts.'}
            </p>
            """,
        )

        return await self.send_email(manager_email, f"{cert_type} Expiry - {days_until} Days", html)

    async def send_password_reset(
        self,
        email: str,
        name: str,
        reset_token: str,
        reset_url: str,
    ) -> bool:
        """
        Send password reset email.

        Args:
            email: User's email address
            name: User's name
            reset_token: Password reset token (for reference)
            reset_url: Full URL to reset password page (e.g., https://app.example.com/reset-password?token=...)

        Returns:
            True if sent successfully
        """
        html = self._wrap_template(
            title="Password Reset",
            date_str="",
            body=f"""
            <h2 style="color: #333; margin-bottom: 20px;">Reset Your Password</h2>

            <p style="color: #666; margin-bottom: 20px;">
                Hi {name},
            </p>

            <p style="color: #666; margin-bottom: 20px;">
                We received a request to reset your password. If you didn't make this request, you can safely ignore this email.
            </p>

            <div style="background: #e8f5e9; padding: 20px; margin: 30px 0; border-radius: 8px; text-align: center;">
                <a href="{reset_url}" style="background: #3366FF; color: white; padding: 12px 30px; text-decoration: none; border-radius: 4px; display: inline-block; font-weight: bold;">
                    Reset Password
                </a>
            </div>

            <p style="color: #666; margin: 20px 0; font-size: 13px;">
                Or copy and paste this link in your browser:
                <br>
                <code style="background: #f5f5f5; padding: 8px 12px; border-radius: 4px; display: inline-block; margin-top: 10px; word-break: break-all; font-size: 12px;">{reset_url}</code>
            </p>

            <div style="background: #fff3cd; border-left: 4px solid #ff9800; padding: 15px; margin: 20px 0; border-radius: 4px;">
                <p style="color: #856404; margin: 0; font-size: 13px;">
                    <strong>Security Note:</strong> This link will expire in 1 hour. For your security, never share this link with anyone.
                </p>
            </div>

            <p style="color: #999; margin-top: 30px; font-size: 12px;">
                If you have any questions, contact support at support@rosteriq.com
            </p>
            """,
        )

        return await self.send_email(email, "Reset Your RosterIQ Password", html)

    async def send_email_verification(
        self,
        email: str,
        name: str,
        verification_token: str,
        verification_url: str,
    ) -> bool:
        """
        Send email verification email.

        Args:
            email: User's email address
            name: User's name
            verification_token: Email verification token (for reference)
            verification_url: Full URL to verify email page (e.g., https://app.example.com/verify-email?token=...)

        Returns:
            True if sent successfully
        """
        html = self._wrap_template(
            title="Verify Your Email",
            date_str="",
            body=f"""
            <h2 style="color: #333; margin-bottom: 20px;">Welcome to RosterIQ!</h2>

            <p style="color: #666; margin-bottom: 20px;">
                Hi {name},
            </p>

            <p style="color: #666; margin-bottom: 20px;">
                Thank you for signing up. Please verify your email address to get started with RosterIQ.
            </p>

            <div style="background: #e8f5e9; padding: 20px; margin: 30px 0; border-radius: 8px; text-align: center;">
                <a href="{verification_url}" style="background: #3366FF; color: white; padding: 12px 30px; text-decoration: none; border-radius: 4px; display: inline-block; font-weight: bold;">
                    Verify Email
                </a>
            </div>

            <p style="color: #666; margin: 20px 0; font-size: 13px;">
                Or copy and paste this link in your browser:
                <br>
                <code style="background: #f5f5f5; padding: 8px 12px; border-radius: 4px; display: inline-block; margin-top: 10px; word-break: break-all; font-size: 12px;">{verification_url}</code>
            </p>

            <div style="background: #e3f2fd; border-left: 4px solid #2196f3; padding: 15px; margin: 20px 0; border-radius: 4px;">
                <p style="color: #1565c0; margin: 0; font-size: 13px;">
                    <strong>Expires in 24 hours:</strong> This link will expire in 24 hours, so please verify your email soon.
                </p>
            </div>

            <p style="color: #999; margin-top: 30px; font-size: 12px;">
                If you didn't sign up for RosterIQ, please ignore this email or contact support at support@rosteriq.com
            </p>
            """,
        )

        return await self.send_email(email, "Verify Your Email - RosterIQ", html)

    async def send_variance_alert(
        self,
        venue_id: str,
        venue: VenueConfig,
        manager_email: str,
        variance_type: str,
        variance_pct: float,
        details: Dict[str, Any],
    ) -> bool:
        """
        Send variance alert when actual differs from forecast by >20%.

        Args:
            venue_id: Venue identifier
            venue: VenueConfig object
            manager_email: Manager's email address
            variance_type: Type of variance (demand, staffing, cost, etc)
            variance_pct: Percentage variance (e.g., 25.5 for 25.5%)
            details: Additional details

        Returns:
            True if sent successfully
        """
        severity = "CRITICAL" if abs(variance_pct) > 50 else "HIGH" if abs(variance_pct) > 30 else "ALERT"
        color = "#d32f2f" if abs(variance_pct) > 50 else "#ff9800" if abs(variance_pct) > 30 else "#ff6f00"

        direction = "HIGHER" if variance_pct > 0 else "LOWER"

        details_html = "".join([
            f'<p style="margin: 8px 0;"><strong>{k}:</strong> {v}</p>'
            for k, v in details.items()
        ])

        html = self._wrap_template(
            title=f"Variance Alert - {variance_type}",
            date_str=date.today().isoformat(),
            body=f"""
            <h2 style="color: {color}; margin-bottom: 20px;">{severity}: {variance_type} Variance</h2>

            <div style="background: {color}14; border-left: 4px solid {color}; padding: 15px; margin: 20px 0; border-radius: 4px;">
                <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 20px;">
                    <div>
                        <div style="font-size: 12px; color: #666; margin-bottom: 5px;">VARIANCE</div>
                        <div style="font-size: 28px; font-weight: bold; color: {color};">{variance_pct:.1f}%</div>
                    </div>
                    <div>
                        <div style="font-size: 12px; color: #666; margin-bottom: 5px;">DIRECTION</div>
                        <div style="font-size: 18px; font-weight: bold; color: {color};">{direction}</div>
                    </div>
                </div>
            </div>

            <div style="background: #f5f5f5; padding: 15px; border-radius: 4px; margin: 20px 0;">
                <div style="font-weight: bold; color: #333; margin-bottom: 10px;">Details</div>
                {details_html}
            </div>

            <p style="color: #666; margin: 20px 0;">
                Please review your current staffing levels and consider adjustments to match demand.
            </p>

            <div style="background: #e8f5e9; padding: 15px; margin: 20px 0; border-radius: 8px; text-align: center;">
                <a href="{self.base_url}/dashboard/{venue_id}/live-pulse" style="background: #3366FF; color: white; padding: 10px 20px; text-decoration: none; border-radius: 4px; display: inline-block;">
                    View Live Pulse
                </a>
            </div>
            """,
        )

        return await self.send_email(
            manager_email,
            f"Variance Alert: {variance_type.title()} {abs(variance_pct):.0f}% {direction}",
            html,
        )

    async def send_with_preferences(
        self,
        user_id: str,
        email: str,
        notification_type: str,
        email_subject: str,
        html_content: str,
        sms_func: Optional[Any] = None,
    ) -> Dict[str, bool]:
        """
        Send notification respecting user preferences and quiet hours.

        Sends email and/or SMS based on user's notification preferences.
        Respects quiet hours (no SMS between 22:00-07:00 AEST).

        Args:
            user_id: User ID for preferences lookup
            email: User's email address
            notification_type: Type of notification (shift_reminder, roster_published, etc)
            email_subject: Email subject line
            html_content: HTML email content
            sms_func: Async function to call for SMS (receives user_id, returns bool)

        Returns:
            Dict with "email" and "sms" keys indicating success of each channel
        """
        results = {"email": False, "sms": False}

        prefs_svc = get_preferences_service()
        sms_svc = get_sms_service()

        # Send email if enabled
        if prefs_svc.should_notify(user_id, "email", notification_type):
            results["email"] = await self.send_email(email, email_subject, html_content)

        # Send SMS if enabled and not in quiet hours
        if (
            sms_func
            and prefs_svc.should_notify(user_id, "sms", notification_type)
            and not prefs_svc.is_in_quiet_hours(user_id)
        ):
            results["sms"] = await sms_func(user_id)

        return results

    def _build_shifts_table(self, shifts: List[Shift]) -> str:
        """Build HTML table of shifts."""
        if not shifts:
            return "<p style='color: #999;'>No shifts scheduled</p>"

        rows = "".join([
            f"""
            <tr style="border-bottom: 1px solid #ddd;">
                <td style="padding: 10px; font-weight: bold;">{shift.role}</td>
                <td style="padding: 10px;">{shift.start_time.strftime('%H:%M')} - {shift.end_time.strftime('%H:%M')}</td>
                <td style="padding: 10px; text-align: right;">{shift.net_hours:.1f}h</td>
            </tr>
            """
            for shift in sorted(shifts, key=lambda s: s.start_time)
        ])

        return f"""
        <table style="width: 100%; border-collapse: collapse; font-size: 13px;">
            <thead style="background: #f5f5f5;">
                <tr style="border-bottom: 2px solid #ddd;">
                    <th style="padding: 10px; text-align: left; color: #333;">Role</th>
                    <th style="padding: 10px; text-align: left; color: #333;">Time</th>
                    <th style="padding: 10px; text-align: right; color: #333;">Duration</th>
                </tr>
            </thead>
            <tbody>
                {rows}
            </tbody>
        </table>
        """

    def _wrap_template(self, title: str, date_str: str, body: str) -> str:
        """Wrap email body in branded template."""
        return f"""
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
        </head>
        <body style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif; color: #333; line-height: 1.6; margin: 0; padding: 0;">
            <div style="max-width: 600px; margin: 0 auto; padding: 20px;">
                <!-- Header -->
                <div style="background: #3366FF; padding: 30px; border-radius: 8px 8px 0 0; text-align: center; color: white;">
                    <div style="font-size: 24px; font-weight: bold; margin-bottom: 5px;">RosterIQ</div>
                    <div style="font-size: 12px; opacity: 0.9;">AI-Powered Predictive Rostering</div>
                </div>

                <!-- Content -->
                <div style="background: white; padding: 30px; border: 1px solid #e0e0e0;">
                    <div style="font-size: 12px; color: #999; margin-bottom: 20px;">{date_str}</div>

                    <h1 style="font-size: 20px; font-weight: bold; color: #333; margin: 0 0 20px 0;">{title}</h1>

                    {body}
                </div>

                <!-- Footer -->
                <div style="background: #f5f5f5; padding: 20px; border-radius: 0 0 8px 8px; border: 1px solid #e0e0e0; border-top: none; text-align: center; font-size: 12px; color: #666;">
                    <p style="margin: 0 0 10px 0;">
                        <a href="{self.base_url}" style="color: #3366FF; text-decoration: none;">RosterIQ Dashboard</a> |
                        <a href="{self.base_url}/docs" style="color: #3366FF; text-decoration: none;">API Docs</a>
                    </p>
                    <p style="margin: 0; opacity: 0.8;">
                        This is an automated message from RosterIQ. Do not reply to this email.
                    </p>
                </div>
            </div>
        </body>
        </html>
        """


# Singleton instance
_notification_service: Optional[NotificationService] = None


def get_notification_service() -> NotificationService:
    """Get or create the notification service singleton."""
    global _notification_service
    if _notification_service is None:
        _notification_service = NotificationService()
    return _notification_service
