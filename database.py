"""
Database layer for RosterIQ.

Provides a thin abstraction over PostgreSQL using psycopg2.
Falls back to in-memory storage when no DATABASE_URL is set.

Usage:
    from rosteriq.database import get_db
    db = get_db()
    db.save_venue(venue)
    venues = db.list_venues()
"""

import os
import json
import logging
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from typing import Optional

from rosteriq.models import (
    Employee, Shift, Roster, DemandForecast, VenueConfig, User,
    EmploymentType, ShiftStatus, AwardLevel, State,
)

logger = logging.getLogger(__name__)

DATABASE_URL = os.environ.get("DATABASE_URL")


# ============================================================================
# Abstract store interface
# ============================================================================

class BaseStore:
    """Interface that both in-memory and PostgreSQL stores implement."""

    def save_venue(self, venue: VenueConfig) -> None:
        raise NotImplementedError

    def list_venues(self) -> list[VenueConfig]:
        raise NotImplementedError

    def get_venue(self, venue_id: str) -> Optional[VenueConfig]:
        raise NotImplementedError

    def save_employee(self, employee: Employee) -> None:
        raise NotImplementedError

    def save_employees(self, employees: list[Employee]) -> None:
        for emp in employees:
            self.save_employee(emp)

    def list_employees(self) -> list[Employee]:
        raise NotImplementedError

    def get_employee(self, employee_id: str) -> Optional[Employee]:
        raise NotImplementedError

    def get_employees_dict(self) -> dict[str, Employee]:
        return {e.id: e for e in self.list_employees()}

    def add_forecasts(self, forecasts: list[DemandForecast]) -> None:
        raise NotImplementedError

    def get_forecasts(
        self,
        venue_id: Optional[str] = None,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> list[DemandForecast]:
        raise NotImplementedError

    def save_roster(self, roster: Roster) -> None:
        raise NotImplementedError

    def list_rosters(self) -> list[Roster]:
        raise NotImplementedError

    def get_roster(self, roster_id: str) -> Optional[Roster]:
        raise NotImplementedError

    def save_shift(self, shift: Shift) -> None:
        """Save a single shift (convenience method)."""
        raise NotImplementedError

    def is_webhook_processed(self, webhook_id: str) -> bool:
        """Check if a webhook has already been processed."""
        raise NotImplementedError

    def save_webhook_event(self, webhook_id: str, event_type: str, payload_hash: str) -> None:
        """Record a processed webhook event."""
        raise NotImplementedError

    def save_user(self, user: dict) -> None:
        raise NotImplementedError

    def get_user_by_email(self, email: str) -> Optional[dict]:
        raise NotImplementedError

    def get_user_by_id(self, user_id: str) -> Optional[dict]:
        raise NotImplementedError

    def list_users(self) -> list[dict]:
        raise NotImplementedError

    def save_refresh_token(self, token_hash: str, user_id: str, expires_at: datetime) -> None:
        raise NotImplementedError

    def get_refresh_token(self, token_hash: str) -> Optional[dict]:
        raise NotImplementedError

    def revoke_refresh_token(self, token_hash: str) -> None:
        raise NotImplementedError

    def record_login_attempt(self, email: str, ip_address: str, success: bool) -> None:
        raise NotImplementedError

    def check_login_rate_limit(self, ip_address: str, minutes: int = 1) -> int:
        """Return count of failed login attempts in last N minutes from this IP."""
        raise NotImplementedError

    def save_onboarding_state(self, state: dict) -> None:
        """Save onboarding state for a venue."""
        raise NotImplementedError

    def get_onboarding_state(self, venue_id: str) -> Optional[dict]:
        """Get onboarding state for a venue."""
        raise NotImplementedError

    def save_subscription(self, subscription: dict) -> None:
        """Save or update a subscription record."""
        raise NotImplementedError

    def get_subscription(self, venue_id: str) -> Optional[dict]:
        """Get subscription for a venue, or None if not found."""
        raise NotImplementedError

    def list_subscriptions(self) -> list[dict]:
        """List all subscriptions."""
        raise NotImplementedError

    def save_billing_event(self, event: dict) -> None:
        """Save a billing event record for audit trail."""
        raise NotImplementedError

    def save_plugin_install(self, install: dict) -> None:
        """Save or update a plugin installation record."""
        raise NotImplementedError

    def get_plugin_install(self, organisation_id: str) -> Optional[dict]:
        """Get plugin installation record by organisation ID."""
        raise NotImplementedError

    def list_plugin_installs(self) -> list[dict]:
        """List all plugin installations."""
        raise NotImplementedError

    def save_feed_config(self, venue_id: str, feed_name: str, config: dict) -> None:
        """Save or update feed configuration for a venue."""
        raise NotImplementedError

    def get_feed_config(self, venue_id: str, feed_name: str) -> Optional[dict]:
        """Get feed configuration for a venue."""
        raise NotImplementedError

    def list_feed_configs(self, venue_id: str) -> list[dict]:
        """List all feed configurations for a venue."""
        raise NotImplementedError

    def save_roster_template(self, template: dict) -> None:
        """Save or update a roster template."""
        raise NotImplementedError

    def get_roster_template(self, template_id: str) -> Optional[dict]:
        """Get a roster template by ID."""
        raise NotImplementedError

    def list_roster_templates(self, venue_id: str) -> list[dict]:
        """List all roster templates for a venue."""
        raise NotImplementedError

    def delete_roster_template(self, template_id: str) -> None:
        """Delete a roster template by ID."""
        raise NotImplementedError

    def save_password_reset_token(self, token_hash: str, user_id: str, expires_at: datetime) -> None:
        """Save a password reset token."""
        raise NotImplementedError

    def get_password_reset_token(self, token_hash: str) -> Optional[dict]:
        """Get a password reset token by hash."""
        raise NotImplementedError

    def delete_password_reset_token(self, token_hash: str) -> None:
        """Delete a password reset token."""
        raise NotImplementedError

    def save_email_verification_token(self, token_hash: str, user_id: str, expires_at: datetime) -> None:
        """Save an email verification token."""
        raise NotImplementedError

    def get_email_verification_token(self, token_hash: str) -> Optional[dict]:
        """Get an email verification token by hash."""
        raise NotImplementedError

    def delete_email_verification_token(self, token_hash: str) -> None:
        """Delete an email verification token."""
        raise NotImplementedError

    def save_shift_swap(self, swap: dict) -> None:
        """Save or update a shift swap record."""
        raise NotImplementedError

    def list_shift_swaps(self, venue_id: Optional[str] = None) -> list[dict]:
        """List shift swaps, optionally filtered by venue."""
        raise NotImplementedError

    def get_shift_swap(self, swap_id: str) -> Optional[dict]:
        """Get a shift swap record by ID."""
        raise NotImplementedError

    def save_webhook_subscription(self, subscription: dict) -> None:
        """Save or update a webhook subscription."""
        raise NotImplementedError

    def get_webhook_subscription(self, subscription_id: str) -> Optional[dict]:
        """Get a webhook subscription by ID."""
        raise NotImplementedError

    def list_webhook_subscriptions(self, venue_id: str) -> list[dict]:
        """List all webhook subscriptions for a venue."""
        raise NotImplementedError

    def delete_webhook_subscription(self, subscription_id: str) -> None:
        """Delete a webhook subscription."""
        raise NotImplementedError

    def save_webhook_delivery(self, delivery: dict) -> None:
        """Save a webhook delivery record."""
        raise NotImplementedError

    def list_webhook_deliveries(self, subscription_id: str, limit: int) -> list[dict]:
        """List webhook delivery records for a subscription."""
        raise NotImplementedError

    def get_webhook_delivery(self, delivery_id: str) -> Optional[dict]:
        """Get a webhook delivery record by ID."""
        raise NotImplementedError

    def list_pending_retries(self, before: datetime) -> list[dict]:
        """
        List pending webhook deliveries ready for retry.

        Args:
            before: Only return deliveries with next_retry_at <= before

        Returns:
            List of pending delivery dicts, ordered by next_retry_at (oldest first)
        """
        raise NotImplementedError

    def save_dead_letter(self, dead_letter: dict) -> None:
        """Save a dead letter delivery record."""
        raise NotImplementedError

    def list_dead_letters(self, venue_id: Optional[str] = None, limit: int = 50) -> list[dict]:
        """
        List dead letter queue entries.

        Args:
            venue_id: Optional filter by venue ID
            limit: Max results to return

        Returns:
            List of dead letter delivery dicts
        """
        raise NotImplementedError

    def delete_dead_letter(self, delivery_id: str) -> None:
        """Delete a dead letter entry."""
        raise NotImplementedError

    def purge_dead_letters(self, before: datetime) -> int:
        """
        Delete dead letter entries older than the specified date.

        Args:
            before: Delete entries with dead_lettered_at < before

        Returns:
            Number of entries deleted
        """
        raise NotImplementedError

    def save_notification_preferences(self, user_id: str, prefs: dict) -> None:
        """Save or update notification preferences for a user."""
        raise NotImplementedError

    def get_notification_preferences(self, user_id: str) -> Optional[dict]:
        """Get notification preferences for a user."""
        raise NotImplementedError

    # --- Data Retention & Privacy (Australian Privacy Act 1988) ---

    def purge_old_webhook_events(self, before_date: datetime) -> int:
        """Delete webhook events older than the specified date. Returns count deleted."""
        raise NotImplementedError

    def purge_old_login_attempts(self, before_date: datetime) -> int:
        """Delete login attempts older than the specified date. Returns count deleted."""
        raise NotImplementedError

    def purge_revoked_tokens(self, before_date: datetime) -> int:
        """Delete revoked refresh tokens older than the specified date. Returns count deleted."""
        raise NotImplementedError

    def save_consent(self, user_id: str, consent_type: str, granted: bool, timestamp: datetime) -> None:
        """Record a user's privacy consent (e.g. data_processing, marketing_emails, analytics, third_party_sharing)."""
        raise NotImplementedError

    def get_consents(self, user_id: str) -> list[dict]:
        """Get all consent records for a user."""
        raise NotImplementedError

    def save_privacy_log(self, entry: dict) -> None:
        """Save a privacy audit log entry (data access/export/deletion/anonymisation)."""
        raise NotImplementedError

    def list_privacy_logs(self, user_id: Optional[str] = None, limit: int = 100) -> list[dict]:
        """List privacy audit logs, optionally filtered by user_id."""
        raise NotImplementedError

    def anonymise_employee(self, employee_id: str) -> None:
        """Mark an employee as anonymised (preserve shift history, anonymise PII)."""
        raise NotImplementedError

    def get_anonymised_employees(self) -> list[dict]:
        """Get list of all anonymised employees."""
        raise NotImplementedError

    # --- Analytics Data Layer ---

    def get_rosters_by_date_range(
        self, venue_id: str, start_date: date, end_date: date
    ) -> list[Roster]:
        """
        Get all rosters for a venue within a date range.

        Args:
            venue_id: The venue ID
            start_date: Start date (inclusive)
            end_date: End date (inclusive)

        Returns:
            List of Roster objects
        """
        raise NotImplementedError

    def get_revenue_snapshots(
        self, venue_id: str, start_date: date, end_date: date
    ) -> list[dict]:
        """
        Get revenue data for a venue within a date range.

        Returns list of {date, revenue} dicts for each day.
        """
        raise NotImplementedError

    def save_analytics_snapshot(self, snapshot: dict) -> None:
        """
        Save an analytics snapshot (e.g., daily labour metrics).

        Snapshot format: {
            "venue_id": str,
            "date": date,
            "metric_type": str,  # "labour_cost", "forecast_accuracy", etc.
            "value": Any,
            "created_at": datetime
        }
        """
        raise NotImplementedError

    def get_analytics_snapshots(
        self,
        venue_id: str,
        metric_type: str,
        start_date: date,
        end_date: date,
    ) -> list[dict]:
        """
        Get analytics snapshots for a venue and metric type.

        Args:
            venue_id: The venue ID
            metric_type: Type of metric (e.g., "labour_cost")
            start_date: Start date (inclusive)
            end_date: End date (inclusive)

        Returns:
            List of snapshot dicts
        """
        raise NotImplementedError

    def save_audit_log(self, entry: dict) -> None:
        """Save an audit log entry for tenant isolation tracking."""
        raise NotImplementedError

    def list_audit_logs(
        self, venue_id: str, limit: int = 100, offset: int = 0
    ) -> list[dict]:
        """List audit logs for a venue."""
        raise NotImplementedError

    # --- Credential Management (API Keys & Webhook Secrets) ---

    def save_api_key_record(self, record: dict) -> None:
        """Save or update an API key record."""
        raise NotImplementedError

    def list_api_key_records(self, user_id: str) -> list[dict]:
        """List all API key records for a user."""
        raise NotImplementedError

    def get_api_key_record(self, key_id: str) -> Optional[dict]:
        """Get an API key record by key ID."""
        raise NotImplementedError

    def save_webhook_secret(self, venue_id: str, secret_record: dict) -> None:
        """Save or update a webhook secret record."""
        raise NotImplementedError

    def get_webhook_secrets(self, venue_id: str) -> list[dict]:
        """Get all webhook secret records for a venue (current + grace period)."""
        raise NotImplementedError

    # --- A/B Testing ---

    def save_experiment(self, experiment: dict) -> None:
        """Save or update an experiment."""
        raise NotImplementedError

    def get_experiment(self, experiment_id: str) -> Optional[dict]:
        """Get an experiment by ID."""
        raise NotImplementedError

    def list_experiments(self, active_only: bool = False) -> list[dict]:
        """
        List all experiments.

        Args:
            active_only: If True, only return experiments with status='active'

        Returns:
            List of experiment dicts
        """
        raise NotImplementedError

    def save_experiment_outcome(self, outcome: dict) -> None:
        """Save an experiment outcome."""
        raise NotImplementedError

    def list_experiment_outcomes(self, experiment_id: str) -> list[dict]:
        """
        List all outcomes for an experiment.

        Args:
            experiment_id: The experiment ID

        Returns:
            List of outcome dicts
        """
        raise NotImplementedError

    # --- White-Label Theming ---

    def save_theme(self, venue_id: str, theme: dict) -> None:
        """Save or update a theme configuration for a venue."""
        raise NotImplementedError

    def get_theme(self, venue_id: str) -> Optional[dict]:
        """Get a theme configuration for a venue. Returns None if not set."""
        raise NotImplementedError

    def delete_theme(self, venue_id: str) -> None:
        """Delete a theme configuration, resetting to defaults."""
        raise NotImplementedError

    # --- Preference Learning ---

    def save_preference_profile(self, employee_id: str, profile: dict) -> None:
        """Save or update a preference profile for an employee."""
        raise NotImplementedError

    def get_preference_profile(self, employee_id: str) -> Optional[dict]:
        """Get a preference profile for an employee. Returns None if not found."""
        raise NotImplementedError

    def list_preference_profiles(self, venue_id: str) -> list[dict]:
        """List all preference profiles for a venue."""
        raise NotImplementedError

    # --- Payroll Export ---

    def save_payroll_batch(self, batch: dict) -> None:
        """Save or update a payroll batch."""
        raise NotImplementedError

    def get_payroll_batch(self, batch_id: str) -> Optional[dict]:
        """Get a payroll batch by ID."""
        raise NotImplementedError

    def list_payroll_batches(self, venue_id: str) -> list[dict]:
        """List all payroll batches for a venue."""
        raise NotImplementedError

    def save_payroll_export(self, export: dict) -> None:
        """Record a payroll export to external service (Xero, KeyPay)."""
        raise NotImplementedError

    def list_payroll_exports(self, venue_id: str, limit: int = 50) -> list[dict]:
        """List payroll exports for a venue."""
        raise NotImplementedError

    # --- Shift Bidding Marketplace ---

    def save_open_shift(self, shift: dict) -> None:
        """Save or update an open shift."""
        raise NotImplementedError

    def get_open_shift(self, shift_id: str) -> Optional[dict]:
        """Get an open shift by ID."""
        raise NotImplementedError

    def list_open_shifts(self, venue_id: str, status: str) -> list[dict]:
        """List open shifts for a venue filtered by status."""
        raise NotImplementedError

    def save_bid(self, bid: dict) -> None:
        """Save or update a bid."""
        raise NotImplementedError

    def get_bid(self, bid_id: str) -> Optional[dict]:
        """Get a bid by ID."""
        raise NotImplementedError

    def list_bids(self, open_shift_id: str) -> list[dict]:
        """List all bids for an open shift."""
        raise NotImplementedError

    # --- Revenue Forecasting ---

    def save_revenue_model(self, venue_id: str, model: dict) -> None:
        """Save or update a trained revenue model for a venue."""
        raise NotImplementedError

    def get_revenue_model(self, venue_id: str) -> Optional[dict]:
        """Get trained revenue model for a venue."""
        raise NotImplementedError

    def save_revenue_actual(self, venue_id: str, date: str, revenue: dict) -> None:
        """Save actual revenue for a date (date in ISO format)."""
        raise NotImplementedError

    def list_revenue_actuals(
        self, venue_id: str, start: str, end: str
    ) -> list[dict]:
        """List actual revenue records for a venue within date range (ISO format)."""
        raise NotImplementedError

    # --- Approval Workflow ---

    def save_approval_request(self, request: dict) -> None:
        """Save or update an approval request."""
        raise NotImplementedError

    def get_approval_request(self, request_id: str) -> Optional[dict]:
        """Get an approval request by ID."""
        raise NotImplementedError

    def list_approval_requests(
        self,
        venue_id: Optional[str] = None,
        roster_id: Optional[str] = None,
        status: Optional[str] = None,
    ) -> list[dict]:
        """
        List approval requests with optional filters.

        Args:
            venue_id: Filter by venue (optional)
            roster_id: Filter by roster (optional)
            status: Filter by status (optional)

        Returns:
            List of approval request dicts
        """
        raise NotImplementedError

    def save_roster_revision(self, revision: dict) -> None:
        """Save a roster revision with change tracking."""
        raise NotImplementedError

    def list_roster_revisions(self, roster_id: str) -> list[dict]:
        """
        List all revisions for a roster.

        Args:
            roster_id: Roster ID

        Returns:
            List of revision dicts, ordered by revision_number
        """
        raise NotImplementedError

    # --- Push Notifications (moved from MemoryStore) ---

    def save_push_subscription(self, user_id: str, subscription: dict) -> None:
        """Save or update a push notification subscription for a user."""
        raise NotImplementedError

    def get_push_subscription(self, user_id: str) -> Optional[dict]:
        """Get push notification subscription for a user."""
        raise NotImplementedError

    def delete_push_subscription(self, user_id: str) -> None:
        """Delete push notification subscription for a user."""
        raise NotImplementedError

    def list_push_subscriptions(self, venue_id: str) -> list[dict]:
        """List all push subscriptions for staff at a venue."""
        raise NotImplementedError

    # --- Shift Management (critical for operations) ---

    def get_shift(self, shift_id: str) -> Optional[Shift]:
        """Get a single shift by ID."""
        raise NotImplementedError

    def list_shifts(self, venue_id: Optional[str] = None) -> list[Shift]:
        """List shifts, optionally filtered by venue."""
        raise NotImplementedError

    def get_venue_shifts_by_date(self, venue_id: str, shift_date: date) -> list[Shift]:
        """Get all shifts for a venue on a specific date."""
        raise NotImplementedError

    def get_venue_shifts_by_date_range(
        self, venue_id: str, start_date: date, end_date: date
    ) -> list[Shift]:
        """Get all shifts for a venue within a date range."""
        raise NotImplementedError

    def list_shifts_by_employee(self, employee_id: str, venue_id: Optional[str] = None) -> list[Shift]:
        """List all shifts for an employee, optionally filtered by venue."""
        raise NotImplementedError


# ============================================================================
# In-memory store (development / testing)
# ============================================================================

class MemoryStore(BaseStore):
    """In-memory storage — same as the existing _store dict but structured."""

    def __init__(self):
        self._venues: dict[str, VenueConfig] = {}
        self._employees: dict[str, Employee] = {}
        self._forecasts: list[DemandForecast] = []
        self._rosters: dict[str, Roster] = {}
        self._shifts: dict[str, Shift] = {}  # Key: shift_id (for individual shift operations)
        self._users: dict[str, dict] = {}
        self._refresh_tokens: dict[str, dict] = {}
        self._login_attempts: list[dict] = []
        self._subscriptions: dict[str, dict] = {}
        self._billing_events: list[dict] = []
        self._webhook_events: dict[str, dict] = {}  # Key: webhook_id (idempotency)
        self._onboarding_states: dict[str, dict] = {}
        self._plugin_installs: dict[str, dict] = {}
        self._feed_configs: dict[str, dict] = {}  # Key: f"{venue_id}:{feed_name}"
        self._roster_templates: dict[str, dict] = {}  # Key: template_id
        self._password_reset_tokens: dict[str, dict] = {}
        self._email_verification_tokens: dict[str, dict] = {}
        self._webhook_subscriptions: dict[str, dict] = {}  # Key: subscription_id
        self._webhook_deliveries: dict[str, list[dict]] = {}  # Key: subscription_id
        self._webhook_retry_queue: dict[str, dict] = {}  # Key: delivery_id
        self._dead_letters: dict[str, dict] = {}  # Key: delivery_id
        self._notification_preferences: dict[str, dict] = {}  # Key: user_id
        self._shift_swaps: dict[str, dict] = {}  # Key: swap_id
        self._audit_logs: list[dict] = []  # Ordered list of audit entries
        self._api_key_records: dict[str, dict] = {}  # Key: key_id
        self._webhook_secrets: dict[str, list[dict]] = {}  # Key: venue_id
        self._consents: dict[str, list[dict]] = {}  # Key: user_id, Value: list of consent records
        self._privacy_logs: list[dict] = []  # Privacy audit trail
        self._anonymised_employees: dict[str, dict] = {}  # Key: employee_id
        self._open_shifts: dict[str, dict] = {}  # Key: shift_id
        self._bids: dict[str, dict] = {}  # Key: bid_id
        self._bids_by_shift: dict[str, list[str]] = {}  # Key: open_shift_id, Value: list of bid_ids
        self._approval_requests: dict[str, dict] = {}  # Key: request_id
        self._roster_revisions: dict[str, list[dict]] = {}  # Key: roster_id, Value: list of revisions
        self._push_subscriptions: dict[str, dict] = {}  # Key: user_id
        self._themes: dict[str, dict] = {}  # Key: venue_id
        self._experiments: dict[str, dict] = {}  # Key: experiment_id
        self._experiment_outcomes: dict[str, dict] = {}  # Key: outcome_id
        self._preference_profiles: dict[str, dict] = {}  # Key: employee_id
        self._payroll_batches: dict[str, dict] = {}  # Key: batch_id
        self._payroll_exports: list[dict] = []  # List of export records
        self._revenue_snapshots: dict[str, dict] = {}  # Key: f"{venue_id}:{date}"
        self._analytics_snapshots: dict[str, dict] = {}  # Key: f"{venue_id}:{date}:{metric_type}"
        self._revenue_models: dict[str, dict] = {}  # Key: venue_id
        self._revenue_actuals: list[dict] = []  # List of revenue records

    def save_venue(self, venue):
        self._venues[venue.id] = venue

    def list_venues(self):
        return list(self._venues.values())

    def get_venue(self, venue_id):
        return self._venues.get(venue_id)

    def save_employee(self, employee):
        self._employees[employee.id] = employee

    def list_employees(self):
        return list(self._employees.values())

    def get_employee(self, employee_id):
        return self._employees.get(employee_id)

    def add_forecasts(self, forecasts):
        self._forecasts.extend(forecasts)

    def get_forecasts(self, venue_id=None, start_date=None, end_date=None):
        results = self._forecasts
        if venue_id:
            results = [f for f in results if f.venue_id == venue_id]
        if start_date:
            results = [f for f in results if f.date >= start_date]
        if end_date:
            results = [f for f in results if f.date <= end_date]
        return results

    def save_roster(self, roster):
        self._rosters[roster.id] = roster

    def list_rosters(self):
        return list(self._rosters.values())

    def get_roster(self, roster_id):
        return self._rosters.get(roster_id)

    def save_shift(self, shift: Shift) -> None:
        """Save a single shift."""
        self._shifts[shift.id] = shift

    def get_shift(self, shift_id: str) -> Optional[Shift]:
        """Get a single shift by ID."""
        return self._shifts.get(shift_id)

    def list_shifts(self, venue_id: Optional[str] = None) -> list[Shift]:
        """List shifts, optionally filtered by venue."""
        shifts = list(self._shifts.values())
        if venue_id:
            # Filter by venue_id if available in shift data
            shifts = [s for s in shifts if getattr(s, 'venue_id', None) == venue_id]
        return shifts

    def get_venue_shifts_by_date(self, venue_id: str, shift_date: date) -> list[Shift]:
        """Get all shifts for a venue on a specific date."""
        return [
            s for s in self._shifts.values()
            if (getattr(s, 'venue_id', None) == venue_id or
                # Fallback: check if shift belongs to a roster of this venue
                any(r.venue_id == venue_id for r in self._rosters.values() if s.id in [sh.id for sh in r.shifts]))
            and s.date == shift_date
        ]

    def get_venue_shifts_by_date_range(
        self, venue_id: str, start_date: date, end_date: date
    ) -> list[Shift]:
        """Get all shifts for a venue within a date range."""
        return [
            s for s in self._shifts.values()
            if (getattr(s, 'venue_id', None) == venue_id or
                # Fallback: check if shift belongs to a roster of this venue
                any(r.venue_id == venue_id for r in self._rosters.values() if s.id in [sh.id for sh in r.shifts]))
            and start_date <= s.date <= end_date
        ]

    def list_shifts_by_employee(self, employee_id: str, venue_id: Optional[str] = None) -> list[Shift]:
        """List all shifts for an employee, optionally filtered by venue."""
        shifts = [s for s in self._shifts.values() if s.employee_id == employee_id]
        if venue_id:
            shifts = [s for s in shifts if getattr(s, 'venue_id', None) == venue_id]
        return shifts

    def is_webhook_processed(self, webhook_id: str) -> bool:
        """Check if a webhook has already been processed."""
        return webhook_id in self._webhook_events

    def save_webhook_event(self, webhook_id: str, event_type: str, payload_hash: str) -> None:
        """Record a processed webhook event with auto-cleanup."""
        self._webhook_events[webhook_id] = {
            'webhook_id': webhook_id,
            'event_type': event_type,
            'payload_hash': payload_hash,
            'processed_at': datetime.utcnow(),
        }
        # Auto-cleanup: keep only last 10000 events
        if len(self._webhook_events) > 10000:
            sorted_events = sorted(
                self._webhook_events.items(),
                key=lambda x: x[1]['processed_at']
            )
            self._webhook_events = dict(sorted_events[-5000:])

    def save_user(self, user):
        self._users[user["id"]] = user

    def get_user_by_email(self, email):
        for user in self._users.values():
            if user["email"] == email:
                return user
        return None

    def get_user_by_id(self, user_id):
        return self._users.get(user_id)

    def list_users(self):
        return list(self._users.values())

    def save_refresh_token(self, token_hash, user_id, expires_at):
        self._refresh_tokens[token_hash] = {
            "user_id": user_id,
            "token_hash": token_hash,
            "expires_at": expires_at,
            "is_revoked": False,
        }

    def get_refresh_token(self, token_hash):
        return self._refresh_tokens.get(token_hash)

    def revoke_refresh_token(self, token_hash):
        if token_hash in self._refresh_tokens:
            self._refresh_tokens[token_hash]["is_revoked"] = True

    def record_login_attempt(self, email, ip_address, success):
        self._login_attempts.append({
            "email": email,
            "ip_address": ip_address,
            "success": success,
            "created_at": datetime.utcnow(),
        })

    def check_login_rate_limit(self, ip_address, minutes=1):
        cutoff = datetime.utcnow() - timedelta(minutes=minutes)
        failed_count = sum(
            1 for attempt in self._login_attempts
            if attempt["ip_address"] == ip_address
            and not attempt["success"]
            and attempt["created_at"] > cutoff
        )
        return failed_count

    def save_subscription(self, subscription):
        """Save or update subscription."""
        self._subscriptions[subscription["venue_id"]] = subscription

    def get_subscription(self, venue_id):
        """Get subscription by venue ID."""
        return self._subscriptions.get(venue_id)

    def list_subscriptions(self):
        """List all subscriptions."""
        return list(self._subscriptions.values())

    def save_billing_event(self, event):
        """Save billing event."""
        self._billing_events.append(event)

    def save_onboarding_state(self, state: dict) -> None:
        """Save onboarding state for a venue."""
        self._onboarding_states[state["venue_id"]] = state

    def get_onboarding_state(self, venue_id: str) -> Optional[dict]:
        """Get onboarding state for a venue."""
        return self._onboarding_states.get(venue_id)

    def save_plugin_install(self, install: dict) -> None:
        """Save or update a plugin installation record."""
        org_id = install.get("organisation_id")
        if org_id:
            self._plugin_installs[org_id] = install

    def get_plugin_install(self, organisation_id: str) -> Optional[dict]:
        """Get plugin installation record by organisation ID."""
        return self._plugin_installs.get(organisation_id)

    def list_plugin_installs(self) -> list[dict]:
        """List all plugin installations."""
        return list(self._plugin_installs.values())

    def save_feed_config(self, venue_id: str, feed_name: str, config: dict) -> None:
        """Save or update feed configuration for a venue."""
        key = f"{venue_id}:{feed_name}"
        self._feed_configs[key] = config

    def get_feed_config(self, venue_id: str, feed_name: str) -> Optional[dict]:
        """Get feed configuration for a venue."""
        key = f"{venue_id}:{feed_name}"
        return self._feed_configs.get(key)

    def list_feed_configs(self, venue_id: str) -> list[dict]:
        """List all feed configurations for a venue."""
        return [
            cfg for key, cfg in self._feed_configs.items()
            if key.startswith(f"{venue_id}:")
        ]

    def save_roster_template(self, template: dict) -> None:
        """Save or update a roster template."""
        self._roster_templates[template["id"]] = template

    def get_roster_template(self, template_id: str) -> Optional[dict]:
        """Get a roster template by ID."""
        return self._roster_templates.get(template_id)

    def list_roster_templates(self, venue_id: str) -> list[dict]:
        """List all roster templates for a venue."""
        return [
            t for t in self._roster_templates.values()
            if t.get("venue_id") == venue_id
        ]

    def delete_roster_template(self, template_id: str) -> None:
        """Delete a roster template by ID."""
        if template_id in self._roster_templates:
            del self._roster_templates[template_id]

    def save_password_reset_token(self, token_hash: str, user_id: str, expires_at: datetime) -> None:
        """Save a password reset token."""
        self._password_reset_tokens[token_hash] = {
            "token_hash": token_hash,
            "user_id": user_id,
            "expires_at": expires_at,
        }

    def get_password_reset_token(self, token_hash: str) -> Optional[dict]:
        """Get a password reset token by hash."""
        return self._password_reset_tokens.get(token_hash)

    def delete_password_reset_token(self, token_hash: str) -> None:
        """Delete a password reset token."""
        if token_hash in self._password_reset_tokens:
            del self._password_reset_tokens[token_hash]

    def save_email_verification_token(self, token_hash: str, user_id: str, expires_at: datetime) -> None:
        """Save an email verification token."""
        self._email_verification_tokens[token_hash] = {
            "token_hash": token_hash,
            "user_id": user_id,
            "expires_at": expires_at,
        }

    def get_email_verification_token(self, token_hash: str) -> Optional[dict]:
        """Get an email verification token by hash."""
        return self._email_verification_tokens.get(token_hash)

    def delete_email_verification_token(self, token_hash: str) -> None:
        """Delete an email verification token."""
        if token_hash in self._email_verification_tokens:
            del self._email_verification_tokens[token_hash]

    def save_webhook_subscription(self, subscription: dict) -> None:
        """Save or update a webhook subscription."""
        subscription_id = subscription.get("id")
        if subscription_id:
            self._webhook_subscriptions[subscription_id] = subscription

    def get_webhook_subscription(self, subscription_id: str) -> Optional[dict]:
        """Get a webhook subscription by ID."""
        return self._webhook_subscriptions.get(subscription_id)

    def list_webhook_subscriptions(self, venue_id: str) -> list[dict]:
        """List all webhook subscriptions for a venue."""
        return [
            sub for sub in self._webhook_subscriptions.values()
            if sub.get("venue_id") == venue_id
        ]

    def delete_webhook_subscription(self, subscription_id: str) -> None:
        """Delete a webhook subscription."""
        if subscription_id in self._webhook_subscriptions:
            del self._webhook_subscriptions[subscription_id]
        if subscription_id in self._webhook_deliveries:
            del self._webhook_deliveries[subscription_id]

    def save_webhook_delivery(self, delivery: dict) -> None:
        """Save a webhook delivery record."""
        delivery_id = delivery.get("id")
        subscription_id = delivery.get("subscription_id")

        # Save to retry queue for queue management
        if delivery_id and delivery.get("status") == "pending":
            self._webhook_retry_queue[delivery_id] = delivery

        if not subscription_id:
            return

        if subscription_id not in self._webhook_deliveries:
            self._webhook_deliveries[subscription_id] = []

        # Prepend new delivery (most recent first)
        self._webhook_deliveries[subscription_id].insert(0, delivery)

        # Keep only last 1000 deliveries per subscription
        if len(self._webhook_deliveries[subscription_id]) > 1000:
            self._webhook_deliveries[subscription_id] = (
                self._webhook_deliveries[subscription_id][:1000]
            )

    def list_webhook_deliveries(self, subscription_id: str, limit: int) -> list[dict]:
        """List webhook delivery records for a subscription."""
        deliveries = self._webhook_deliveries.get(subscription_id, [])
        return deliveries[:limit]

    def get_webhook_delivery(self, delivery_id: str) -> Optional[dict]:
        """Get a webhook delivery record by ID."""
        if not hasattr(self, '_webhook_retry_queue'):
            self._webhook_retry_queue = {}
        if not hasattr(self, '_dead_letters'):
            self._dead_letters = {}

        # Check retry queue first
        if delivery_id in self._webhook_retry_queue:
            return self._webhook_retry_queue[delivery_id]

        # Check dead letters
        if delivery_id in self._dead_letters:
            return self._dead_letters[delivery_id]

        return None

    def list_pending_retries(self, before: datetime) -> list[dict]:
        """List pending webhook deliveries ready for retry."""
        if not hasattr(self, '_webhook_retry_queue'):
            self._webhook_retry_queue = {}

        pending = []
        for delivery in self._webhook_retry_queue.values():
            if delivery.get("status") != "pending":
                continue

            next_retry_str = delivery.get("next_retry_at")
            if not next_retry_str:
                continue

            # Parse ISO datetime string
            try:
                if isinstance(next_retry_str, str):
                    # Handle both UTC and timezone-aware formats
                    next_retry = datetime.fromisoformat(
                        next_retry_str.replace("Z", "+00:00")
                    )
                else:
                    next_retry = next_retry_str

                if next_retry <= before:
                    pending.append(delivery)
            except (ValueError, AttributeError):
                # Skip deliveries with invalid dates
                continue

        # Sort by next_retry_at (oldest first)
        pending.sort(
            key=lambda d: datetime.fromisoformat(
                d.get("next_retry_at", "").replace("Z", "+00:00")
            )
            if d.get("next_retry_at") else datetime.now(timezone.utc)
        )

        return pending

    def save_dead_letter(self, dead_letter: dict) -> None:
        """Save a dead letter delivery record."""
        if not hasattr(self, '_dead_letters'):
            self._dead_letters = {}

        delivery_id = dead_letter.get("id")
        if delivery_id:
            self._dead_letters[delivery_id] = dead_letter

    def list_dead_letters(self, venue_id: Optional[str] = None, limit: int = 50) -> list[dict]:
        """List dead letter queue entries."""
        if not hasattr(self, '_dead_letters'):
            self._dead_letters = {}

        dead_letters = list(self._dead_letters.values())

        # Filter by venue if specified
        if venue_id:
            dead_letters = [
                dl for dl in dead_letters
                if dl.get("venue_id") == venue_id
            ]

        # Sort by dead_lettered_at (newest first)
        dead_letters.sort(
            key=lambda dl: dl.get("dead_lettered_at", ""),
            reverse=True,
        )

        return dead_letters[:limit]

    def delete_dead_letter(self, delivery_id: str) -> None:
        """Delete a dead letter entry."""
        if not hasattr(self, '_dead_letters'):
            self._dead_letters = {}

        if delivery_id in self._dead_letters:
            del self._dead_letters[delivery_id]

    def purge_dead_letters(self, before: datetime) -> int:
        """Delete dead letter entries older than the specified date."""
        if not hasattr(self, '_dead_letters'):
            self._dead_letters = {}

        initial_count = len(self._dead_letters)
        to_delete = []

        for delivery_id, dl in self._dead_letters.items():
            dl_at_str = dl.get("dead_lettered_at")
            if not dl_at_str:
                continue

            try:
                dl_at = datetime.fromisoformat(
                    dl_at_str.replace("Z", "+00:00")
                )
                if dl_at < before:
                    to_delete.append(delivery_id)
            except (ValueError, AttributeError):
                pass

        for delivery_id in to_delete:
            del self._dead_letters[delivery_id]

        return len(to_delete)

    def save_shift_swap(self, swap: dict) -> None:
        """Save or update a shift swap record."""
        self._shift_swaps[swap["id"]] = swap

    def list_shift_swaps(self, venue_id: Optional[str] = None) -> list[dict]:
        """List shift swaps, optionally filtered by venue."""
        swaps = list(self._shift_swaps.values())
        if venue_id:
            swaps = [s for s in swaps if s.get("venue_id") == venue_id]
        return swaps

    def get_shift_swap(self, swap_id: str) -> Optional[dict]:
        """Get a shift swap record by ID."""
        return self._shift_swaps.get(swap_id)

    def save_notification_preferences(self, user_id: str, prefs: dict) -> None:
        """Save or update notification preferences for a user."""
        self._notification_preferences[user_id] = prefs

    def get_notification_preferences(self, user_id: str) -> Optional[dict]:
        """Get notification preferences for a user."""
        return self._notification_preferences.get(user_id)

    # --- Data Retention & Privacy (Australian Privacy Act 1988) ---

    def purge_old_webhook_events(self, before_date: datetime) -> int:
        """Delete webhook events older than the specified date. Returns count deleted."""
        if not hasattr(self, '_webhook_events'):
            return 0

        initial_count = len(self._webhook_events)
        to_delete = [
            k for k, v in self._webhook_events.items()
            if v.get('processed_at', datetime.utcnow()) < before_date
        ]

        for key in to_delete:
            del self._webhook_events[key]

        return len(to_delete)

    def purge_old_login_attempts(self, before_date: datetime) -> int:
        """Delete login attempts older than the specified date. Returns count deleted."""
        initial_count = len(self._login_attempts)
        self._login_attempts = [
            attempt for attempt in self._login_attempts
            if attempt.get('created_at', datetime.utcnow()) >= before_date
        ]
        return initial_count - len(self._login_attempts)

    def purge_revoked_tokens(self, before_date: datetime) -> int:
        """Delete revoked refresh tokens older than the specified date. Returns count deleted."""
        initial_count = len(self._refresh_tokens)
        to_delete = [
            k for k, v in self._refresh_tokens.items()
            if v.get('is_revoked', False) and v.get('expires_at', datetime.utcnow()) < before_date
        ]

        for key in to_delete:
            del self._refresh_tokens[key]

        return len(to_delete)

    def save_consent(self, user_id: str, consent_type: str, granted: bool, timestamp: datetime) -> None:
        """Record a user's privacy consent."""
        if user_id not in self._consents:
            self._consents[user_id] = []

        self._consents[user_id].append({
            'user_id': user_id,
            'consent_type': consent_type,
            'granted': granted,
            'timestamp': timestamp,
        })

    def get_consents(self, user_id: str) -> list[dict]:
        """Get all consent records for a user."""
        return self._consents.get(user_id, [])

    def save_privacy_log(self, entry: dict) -> None:
        """Save a privacy audit log entry."""
        entry['logged_at'] = entry.get('logged_at', datetime.utcnow())
        self._privacy_logs.append(entry)

    def list_privacy_logs(self, user_id: Optional[str] = None, limit: int = 100) -> list[dict]:
        """List privacy audit logs, optionally filtered by user_id."""
        logs = self._privacy_logs
        if user_id:
            logs = [log for log in logs if log.get('user_id') == user_id]

        return logs[-limit:]

    def anonymise_employee(self, employee_id: str) -> None:
        """Mark an employee as anonymised."""
        self._anonymised_employees[employee_id] = {
            'employee_id': employee_id,
            'anonymised_at': datetime.utcnow(),
        }

        # Anonymise the employee record if it exists
        emp = self.get_employee(employee_id)
        if emp:
            emp.name = f"Anonymised Employee #{employee_id[:8]}"
            emp.email = f"anon_{employee_id[:8]}@deleted.local"
            emp.phone = None
            self.save_employee(emp)

    def get_anonymised_employees(self) -> list[dict]:
        """Get list of all anonymised employees."""
        return list(self._anonymised_employees.values())

    # --- Analytics Data Layer ---

    def get_rosters_by_date_range(
        self, venue_id: str, start_date: date, end_date: date
    ) -> list[Roster]:
        """Get all rosters for a venue within a date range."""
        results = []
        for roster in self._rosters.values():
            if roster.venue_id == venue_id:
                # Check if roster's week falls within date range
                if roster.week_start <= end_date and roster.week_end >= start_date:
                    results.append(roster)
        return results

    def get_revenue_snapshots(
        self, venue_id: str, start_date: date, end_date: date
    ) -> list[dict]:
        """Get revenue data for a venue within a date range."""
        results = []
        for key, snapshot in self._revenue_snapshots.items():
            snap_venue_id, snap_date_str = key.split(":")
            snap_date = date.fromisoformat(snap_date_str)

            if snap_venue_id == venue_id and start_date <= snap_date <= end_date:
                results.append({
                    "date": snap_date,
                    "revenue": snapshot.get("revenue", Decimal("0.00")),
                })

        return results

    def save_analytics_snapshot(self, snapshot: dict) -> None:
        """Save an analytics snapshot."""
        key = f"{snapshot['venue_id']}:{snapshot.get('date')}:{snapshot.get('metric_type')}"
        self._analytics_snapshots[key] = snapshot

    def get_analytics_snapshots(
        self,
        venue_id: str,
        metric_type: str,
        start_date: date,
        end_date: date,
    ) -> list[dict]:
        """Get analytics snapshots for a venue and metric type."""
        results = []
        for snapshot in self._analytics_snapshots.values():
            snap_venue_id = snapshot.get('venue_id')
            snap_metric = snapshot.get('metric_type')
            snap_date_str = snapshot.get('date')

            if snap_venue_id == venue_id and snap_metric == metric_type:
                try:
                    snap_date = date.fromisoformat(snap_date_str) if isinstance(snap_date_str, str) else snap_date_str
                    if start_date <= snap_date <= end_date:
                        results.append(snapshot)
                except (ValueError, TypeError):
                    pass

        return results

    def save_audit_log(self, entry: dict) -> None:
        """Save an audit log entry."""
        entry['created_at'] = entry.get('created_at', datetime.utcnow())
        self._audit_logs.append(entry)

    def list_audit_logs(
        self, venue_id: str, limit: int = 100, offset: int = 0
    ) -> list[dict]:
        """List audit logs for a venue."""
        # Filter by venue_id and sort by created_at descending
        logs = [log for log in self._audit_logs if log.get('venue_id') == venue_id]
        logs.sort(key=lambda x: x.get('created_at', datetime.utcnow()), reverse=True)

        # Apply pagination
        return logs[offset : offset + limit]

    # --- White-Label Theming ---

    def save_theme(self, venue_id: str, theme: dict) -> None:
        """Save or update a theme configuration for a venue."""
        self._themes[venue_id] = theme

    def get_theme(self, venue_id: str) -> Optional[dict]:
        """Get a theme configuration for a venue. Returns None if not set."""
        return self._themes.get(venue_id)

    def delete_theme(self, venue_id: str) -> None:
        """Delete a theme configuration, resetting to defaults."""
        if venue_id in self._themes:
            del self._themes[venue_id]

    # --- A/B Testing ---

    def save_experiment(self, experiment: dict) -> None:
        """Save or update an experiment."""
        self._experiments[experiment["id"]] = experiment

    def get_experiment(self, experiment_id: str) -> Optional[dict]:
        """Get an experiment by ID."""
        return self._experiments.get(experiment_id)

    def list_experiments(self, active_only: bool = False) -> list[dict]:
        """
        List all experiments.

        Args:
            active_only: If True, only return experiments with status='active'

        Returns:
            List of experiment dicts
        """
        experiments = list(self._experiments.values())
        if active_only:
            experiments = [e for e in experiments if e.get("status") == "active"]
        return experiments

    def save_experiment_outcome(self, outcome: dict) -> None:
        """Save an experiment outcome."""
        self._experiment_outcomes[outcome["id"]] = outcome

    def list_experiment_outcomes(self, experiment_id: str) -> list[dict]:
        """
        List all outcomes for an experiment.

        Args:
            experiment_id: The experiment ID

        Returns:
            List of outcome dicts
        """
        outcomes = [
            o for o in self._experiment_outcomes.values()
            if o.get("experiment_id") == experiment_id
        ]
        return outcomes

    # --- Credential Management (API Keys & Webhook Secrets) ---

    def save_api_key_record(self, record: dict) -> None:
        """Save or update an API key record."""
        self._api_key_records[record["id"]] = record

    def list_api_key_records(self, user_id: str) -> list[dict]:
        """List all API key records for a user."""
        return [
            r for r in self._api_key_records.values()
            if r["user_id"] == user_id
        ]

    def get_api_key_record(self, key_id: str) -> Optional[dict]:
        """Get an API key record by key ID."""
        return self._api_key_records.get(key_id)

    def save_webhook_secret(self, venue_id: str, secret_record: dict) -> None:
        """Save or update a webhook secret record."""
        if venue_id not in self._webhook_secrets:
            self._webhook_secrets[venue_id] = []

        # Update if exists, otherwise append
        found = False
        for i, secret in enumerate(self._webhook_secrets[venue_id]):
            if secret.get("id") == secret_record.get("id"):
                self._webhook_secrets[venue_id][i] = secret_record
                found = True
                break

        if not found:
            self._webhook_secrets[venue_id].append(secret_record)

    def get_webhook_secrets(self, venue_id: str) -> list[dict]:
        """Get all webhook secret records for a venue."""
        return self._webhook_secrets.get(venue_id, [])

    def save_preference_profile(self, employee_id: str, profile: dict) -> None:
        """Save or update a preference profile for an employee."""
        self._preference_profiles[employee_id] = profile

    def get_preference_profile(self, employee_id: str) -> Optional[dict]:
        """Get a preference profile for an employee. Returns None if not found."""
        return self._preference_profiles.get(employee_id)

    def list_preference_profiles(self, venue_id: str) -> list[dict]:
        """List all preference profiles for a venue."""
        # Filter profiles by venue_id
        return [
            p for p in self._preference_profiles.values()
            if p.get('venue_id') == venue_id
        ]

    def save_payroll_batch(self, batch: dict) -> None:
        """Save or update a payroll batch."""
        self._payroll_batches[batch['batch_id']] = batch

    def get_payroll_batch(self, batch_id: str) -> Optional[dict]:
        """Get a payroll batch by ID."""
        return self._payroll_batches.get(batch_id)

    def list_payroll_batches(self, venue_id: str) -> list[dict]:
        """List all payroll batches for a venue."""
        return [
            b for b in self._payroll_batches.values()
            if b.get('venue_id') == venue_id
        ]

    def save_payroll_export(self, export: dict) -> None:
        """Record a payroll export to external service."""
        self._payroll_exports.append(export)

    def list_payroll_exports(self, venue_id: str, limit: int = 50) -> list[dict]:
        """List payroll exports for a venue."""
        return [
            e for e in self._payroll_exports
            if e.get('venue_id') == venue_id
        ][-limit:]

    # --- Shift Bidding Marketplace ---

    def save_open_shift(self, shift: dict) -> None:
        """Save or update an open shift."""
        self._open_shifts[shift["id"]] = shift

    def get_open_shift(self, shift_id: str) -> Optional[dict]:
        """Get an open shift by ID."""
        return self._open_shifts.get(shift_id)

    def list_open_shifts(self, venue_id: str, status: str) -> list[dict]:
        """List open shifts for a venue filtered by status."""
        return [
            shift for shift in self._open_shifts.values()
            if shift.get("venue_id") == venue_id and shift.get("status") == status
        ]

    def save_bid(self, bid: dict) -> None:
        """Save or update a bid."""
        bid_id = bid["id"]
        open_shift_id = bid["open_shift_id"]

        # Save bid
        self._bids[bid_id] = bid

        # Track bid -> shift relationship
        if open_shift_id not in self._bids_by_shift:
            self._bids_by_shift[open_shift_id] = []

        if bid_id not in self._bids_by_shift[open_shift_id]:
            self._bids_by_shift[open_shift_id].append(bid_id)

    def get_bid(self, bid_id: str) -> Optional[dict]:
        """Get a bid by ID."""
        return self._bids.get(bid_id)

    def list_bids(self, open_shift_id: str) -> list[dict]:
        """List all bids for an open shift."""
        bid_ids = self._bids_by_shift.get(open_shift_id, [])
        return [self._bids[bid_id] for bid_id in bid_ids if bid_id in self._bids]

    # --- Approval Workflow ---

    def save_approval_request(self, request: dict) -> None:
        """Save or update an approval request."""
        self._approval_requests[request["id"]] = request

    def get_approval_request(self, request_id: str) -> Optional[dict]:
        """Get an approval request by ID."""
        return self._approval_requests.get(request_id)

    def list_approval_requests(
        self,
        venue_id: Optional[str] = None,
        roster_id: Optional[str] = None,
        status: Optional[str] = None,
    ) -> list[dict]:
        """List approval requests with optional filters."""
        results = list(self._approval_requests.values())

        if venue_id:
            results = [r for r in results if r.get("venue_id") == venue_id]
        if roster_id:
            results = [r for r in results if r.get("roster_id") == roster_id]
        if status:
            results = [r for r in results if r.get("status") == status]

        return results

    def save_roster_revision(self, revision: dict) -> None:
        """Save a roster revision with change tracking."""
        roster_id = revision["roster_id"]
        if roster_id not in self._roster_revisions:
            self._roster_revisions[roster_id] = []
        self._roster_revisions[roster_id].append(revision)

    def list_roster_revisions(self, roster_id: str) -> list[dict]:
        """List all revisions for a roster."""
        revisions = self._roster_revisions.get(roster_id, [])
        return sorted(revisions, key=lambda r: r.get("revision_number", 0))

    # --- Push Notifications ---

    def save_push_subscription(self, user_id: str, subscription: dict) -> None:
        """Save or update a push notification subscription for a user."""
        self._push_subscriptions[user_id] = subscription

    def get_push_subscription(self, user_id: str) -> Optional[dict]:
        """Get push notification subscription for a user."""
        return self._push_subscriptions.get(user_id)

    def delete_push_subscription(self, user_id: str) -> None:
        """Delete push notification subscription for a user."""
        if user_id in self._push_subscriptions:
            del self._push_subscriptions[user_id]

    def list_push_subscriptions(self, venue_id: str) -> list[dict]:
        """List all push subscriptions for staff at a venue."""
        # Filter subscriptions by venue_id if present in subscription data
        results = []
        for sub in self._push_subscriptions.values():
            if sub.get("venue_id") == venue_id:
                results.append(sub)
        return results

    # --- Revenue Forecasting ---

    def save_revenue_model(self, venue_id: str, model: dict) -> None:
        """Save or update a trained revenue model for a venue."""
        self._revenue_models[venue_id] = model

    def get_revenue_model(self, venue_id: str) -> Optional[dict]:
        """Get trained revenue model for a venue."""
        return self._revenue_models.get(venue_id)

    def save_revenue_actual(self, venue_id: str, date: str, revenue: dict) -> None:
        """Save actual revenue for a date (date in ISO format)."""
        # Key format: venue_id:date
        key = f"{venue_id}:{date}"
        # Remove existing record for this date if present
        self._revenue_actuals = [
            r for r in self._revenue_actuals
            if f"{r.get('venue_id')}:{r.get('date')}" != key
        ]
        # Add new record
        record = {'venue_id': venue_id, 'date': date}
        record.update(revenue)
        self._revenue_actuals.append(record)

    def list_revenue_actuals(
        self, venue_id: str, start: str, end: str
    ) -> list[dict]:
        """List actual revenue records for a venue within date range (ISO format)."""
        # Parse dates
        try:
            start_date = datetime.fromisoformat(start).date()
            end_date = datetime.fromisoformat(end).date()
        except (ValueError, AttributeError):
            return []

        results = []
        for record in self._revenue_actuals:
            if record.get('venue_id') != venue_id:
                continue
            try:
                record_date = datetime.fromisoformat(record.get('date', '')).date()
                if start_date <= record_date <= end_date:
                    results.append(record)
            except (ValueError, AttributeError):
                continue

        return results


# ============================================================================
# PostgreSQL store (production)
# ============================================================================

class PostgresStore(BaseStore):
    """PostgreSQL-backed storage using psycopg2."""

    def __init__(self, dsn: str, max_retries: int = 3, retry_delay: float = 2.0):
        import time as _time

        try:
            import psycopg2
            import psycopg2.extras
        except ImportError:
            raise RuntimeError("psycopg2 not installed — run: pip install psycopg2-binary")

        last_error = None
        for attempt in range(1, max_retries + 1):
            try:
                self._conn = psycopg2.connect(dsn)
                self._conn.autocommit = True
                logger.info("Connected to PostgreSQL: %s", dsn.split("@")[-1])
                return  # success
            except psycopg2.OperationalError as e:
                last_error = e
                if attempt < max_retries:
                    logger.warning(
                        "Database connection attempt %d/%d failed: %s — retrying in %.1fs",
                        attempt, max_retries, e, retry_delay,
                    )
                    _time.sleep(retry_delay)
                else:
                    logger.error(
                        "Database connection failed after %d attempts: %s",
                        max_retries, e,
                    )

        raise RuntimeError(
            f"Could not connect to PostgreSQL after {max_retries} attempts. "
            f"Last error: {last_error}. Check DATABASE_URL and ensure the database is running."
        )

    def close(self):
        """Close the database connection."""
        if hasattr(self, '_conn') and self._conn and not self._conn.closed:
            try:
                self._conn.close()
                logger.info("PostgreSQL connection closed")
            except Exception as e:
                logger.warning("Error closing PostgreSQL connection: %s", e)

    def _cursor(self):
        import psycopg2.extras
        return self._conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    # --- Venues ---

    def save_venue(self, venue):
        with self._cursor() as cur:
            cur.execute("""
                INSERT INTO venues (id, name, tanda_org_id, state, timezone, min_staff, max_labour_pct, pos_system, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO UPDATE SET
                    name=EXCLUDED.name, tanda_org_id=EXCLUDED.tanda_org_id,
                    state=EXCLUDED.state, min_staff=EXCLUDED.min_staff,
                    max_labour_pct=EXCLUDED.max_labour_pct, pos_system=EXCLUDED.pos_system,
                    updated_at=now()
            """, (
                venue.id, venue.name, venue.tanda_org_id, venue.state.value,
                venue.timezone, json.dumps(venue.min_staff),
                float(venue.max_labour_pct), venue.pos_system, venue.created_at,
            ))

    def list_venues(self):
        with self._cursor() as cur:
            cur.execute("SELECT * FROM venues ORDER BY name")
            return [self._row_to_venue(r) for r in cur.fetchall()]

    def get_venue(self, venue_id):
        with self._cursor() as cur:
            cur.execute("SELECT * FROM venues WHERE id = %s", (venue_id,))
            row = cur.fetchone()
            return self._row_to_venue(row) if row else None

    def _row_to_venue(self, row):
        return VenueConfig(
            id=row["id"], name=row["name"], tanda_org_id=row["tanda_org_id"],
            state=State(row["state"]), timezone=row["timezone"],
            min_staff=row["min_staff"] if isinstance(row["min_staff"], dict) else json.loads(row["min_staff"]),
            max_labour_pct=float(row["max_labour_pct"]),
            pos_system=row.get("pos_system"),
            created_at=row["created_at"],
        )

    # --- Employees ---

    def save_employee(self, emp):
        with self._cursor() as cur:
            cur.execute("""
                INSERT INTO employees (id, venue_id, tanda_id, name, employment_type, award_level,
                    hourly_base_rate, skills, availability, max_hours_per_week, consecutive_days, phone, email, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO UPDATE SET
                    name=EXCLUDED.name, employment_type=EXCLUDED.employment_type,
                    hourly_base_rate=EXCLUDED.hourly_base_rate, skills=EXCLUDED.skills,
                    availability=EXCLUDED.availability, max_hours_per_week=EXCLUDED.max_hours_per_week,
                    phone=EXCLUDED.phone, email=EXCLUDED.email, updated_at=now()
            """, (
                emp.id, getattr(emp, 'venue_id', 'demo-venue'), emp.tanda_id, emp.name,
                emp.employment_type.value, emp.award_level.value,
                float(emp.hourly_base_rate), emp.skills,
                json.dumps(emp.availability), emp.max_hours_per_week,
                emp.consecutive_days_limit, emp.phone, emp.email,
                emp.created_at, emp.updated_at,
            ))

    def list_employees(self):
        with self._cursor() as cur:
            cur.execute("SELECT * FROM employees WHERE active = true ORDER BY name")
            return [self._row_to_employee(r) for r in cur.fetchall()]

    def get_employee(self, employee_id):
        with self._cursor() as cur:
            cur.execute("SELECT * FROM employees WHERE id = %s", (employee_id,))
            row = cur.fetchone()
            return self._row_to_employee(row) if row else None

    def _row_to_employee(self, row):
        avail = row["availability"]
        if isinstance(avail, str):
            avail = json.loads(avail)
        return Employee(
            id=row["id"], tanda_id=row.get("tanda_id"), name=row["name"],
            employment_type=EmploymentType(row["employment_type"]),
            award_level=AwardLevel(row["award_level"]),
            state=State("vic"),  # Will derive from venue in production
            hourly_base_rate=Decimal(str(row["hourly_base_rate"])),
            phone=row.get("phone"), email=row.get("email"),
            skills=row.get("skills", []),
            availability=avail,
            max_hours_per_week=float(row["max_hours_per_week"]),
            consecutive_days_limit=row.get("consecutive_days", 6),
            created_at=row["created_at"], updated_at=row["updated_at"],
        )

    # --- Forecasts ---

    def add_forecasts(self, forecasts):
        with self._cursor() as cur:
            for fc in forecasts:
                signals = [s.value if hasattr(s, 'value') else s for s in fc.signals_used]
                cur.execute("""
                    INSERT INTO forecasts (id, venue_id, forecast_date, hour, predicted_covers, confidence, signals_used, model_version)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (venue_id, forecast_date, hour, model_version) DO UPDATE SET
                        predicted_covers=EXCLUDED.predicted_covers, confidence=EXCLUDED.confidence
                """, (
                    fc.id, fc.venue_id, fc.date, fc.hour,
                    float(fc.predicted_covers), float(fc.confidence),
                    signals, fc.model_version,
                ))

    def get_forecasts(self, venue_id=None, start_date=None, end_date=None):
        conditions, params = [], []
        if venue_id:
            conditions.append("venue_id = %s")
            params.append(venue_id)
        if start_date:
            conditions.append("forecast_date >= %s")
            params.append(start_date)
        if end_date:
            conditions.append("forecast_date <= %s")
            params.append(end_date)

        where = " WHERE " + " AND ".join(conditions) if conditions else ""
        with self._cursor() as cur:
            cur.execute(f"SELECT * FROM forecasts{where} ORDER BY forecast_date, hour", params)
            return [self._row_to_forecast(r) for r in cur.fetchall()]

    def _row_to_forecast(self, row):
        return DemandForecast(
            id=row["id"], venue_id=row["venue_id"],
            date=row["forecast_date"], hour=row["hour"],
            predicted_covers=float(row["predicted_covers"]),
            confidence=float(row["confidence"]),
            signals_used=row.get("signals_used", []),
            model_version=row["model_version"],
        )

    # --- Rosters ---

    def save_roster(self, roster):
        with self._cursor() as cur:
            cur.execute("""
                INSERT INTO rosters (id, venue_id, week_start, week_end, total_cost, created_at)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO UPDATE SET total_cost=EXCLUDED.total_cost
            """, (
                roster.id, roster.venue_id, roster.week_start, roster.week_end,
                float(roster.total_cost) if roster.total_cost else None,
                roster.created_at,
            ))
            # Save shifts
            for shift in roster.shifts:
                cur.execute("""
                    INSERT INTO shifts (id, roster_id, employee_id, shift_date, start_time, end_time,
                        break_minutes, status, role, cost, penalty_multiplier)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (id) DO NOTHING
                """, (
                    shift.id, roster.id, shift.employee_id, shift.date,
                    shift.start_time, shift.end_time, shift.break_minutes,
                    shift.status.value, shift.role,
                    float(shift.cost) if shift.cost else None,
                    shift.penalty_multiplier,
                ))

    def list_rosters(self):
        with self._cursor() as cur:
            cur.execute("SELECT * FROM rosters ORDER BY week_start DESC")
            rosters = []
            for row in cur.fetchall():
                cur.execute("SELECT * FROM shifts WHERE roster_id = %s ORDER BY shift_date, start_time", (row["id"],))
                shifts = [self._row_to_shift(s) for s in cur.fetchall()]
                rosters.append(self._row_to_roster(row, shifts))
            return rosters

    def get_roster(self, roster_id):
        with self._cursor() as cur:
            cur.execute("SELECT * FROM rosters WHERE id = %s", (roster_id,))
            row = cur.fetchone()
            if not row:
                return None
            cur.execute("SELECT * FROM shifts WHERE roster_id = %s ORDER BY shift_date, start_time", (roster_id,))
            shifts = [self._row_to_shift(s) for s in cur.fetchall()]
            return self._row_to_roster(row, shifts)

    def _row_to_roster(self, row, shifts):
        return Roster(
            id=row["id"], venue_id=row["venue_id"],
            week_start=row["week_start"], week_end=row["week_end"],
            shifts=shifts,
            total_cost=Decimal(str(row["total_cost"])) if row["total_cost"] else None,
            created_at=row["created_at"],
        )

    def _row_to_shift(self, row):
        return Shift(
            id=row["id"], employee_id=row["employee_id"],
            date=row["shift_date"],
            start_time=row["start_time"], end_time=row["end_time"],
            break_minutes=row.get("break_minutes", 0),
            status=ShiftStatus(row["status"]),
            role=row.get("role", "general"),
            cost=Decimal(str(row["cost"])) if row.get("cost") else None,
            penalty_multiplier=float(row.get("penalty_multiplier", 1.0)),
        )

    def save_shift(self, shift: Shift) -> None:
        """Save a single shift (convenience method for shift bidding)."""
        with self._cursor() as cur:
            cur.execute("""
                INSERT INTO shifts
                    (id, employee_id, shift_date, start_time, end_time, break_minutes,
                     status, role, cost, penalty_multiplier, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO UPDATE SET
                    employee_id=EXCLUDED.employee_id,
                    status=EXCLUDED.status
            """, (
                shift.id,
                shift.employee_id,
                shift.date,
                shift.start_time,
                shift.end_time,
                shift.break_minutes,
                shift.status.value,
                shift.role,
                float(shift.cost) if shift.cost else None,
                shift.penalty_multiplier,
                datetime.utcnow(),
            ))

    def get_shift(self, shift_id: str) -> Optional[Shift]:
        """Get a single shift by ID."""
        with self._cursor() as cur:
            cur.execute("SELECT * FROM shifts WHERE id = %s", (shift_id,))
            row = cur.fetchone()
            return self._row_to_shift(row) if row else None

    def list_shifts(self, venue_id: Optional[str] = None) -> list[Shift]:
        """List shifts, optionally filtered by venue."""
        with self._cursor() as cur:
            if venue_id:
                cur.execute("""
                    SELECT s.* FROM shifts s
                    JOIN rosters r ON s.roster_id = r.id
                    WHERE r.venue_id = %s
                    ORDER BY s.shift_date, s.start_time
                """, (venue_id,))
            else:
                cur.execute("SELECT * FROM shifts ORDER BY shift_date, start_time")
            return [self._row_to_shift(r) for r in cur.fetchall()]

    def get_venue_shifts_by_date(self, venue_id: str, shift_date: date) -> list[Shift]:
        """Get all shifts for a venue on a specific date."""
        with self._cursor() as cur:
            cur.execute("""
                SELECT s.* FROM shifts s
                JOIN rosters r ON s.roster_id = r.id
                WHERE r.venue_id = %s AND s.shift_date = %s
                ORDER BY s.start_time
            """, (venue_id, shift_date))
            return [self._row_to_shift(r) for r in cur.fetchall()]

    def get_venue_shifts_by_date_range(
        self, venue_id: str, start_date: date, end_date: date
    ) -> list[Shift]:
        """Get all shifts for a venue within a date range."""
        with self._cursor() as cur:
            cur.execute("""
                SELECT s.* FROM shifts s
                JOIN rosters r ON s.roster_id = r.id
                WHERE r.venue_id = %s AND s.shift_date BETWEEN %s AND %s
                ORDER BY s.shift_date, s.start_time
            """, (venue_id, start_date, end_date))
            return [self._row_to_shift(r) for r in cur.fetchall()]

    def list_shifts_by_employee(self, employee_id: str, venue_id: Optional[str] = None) -> list[Shift]:
        """List all shifts for an employee, optionally filtered by venue."""
        with self._cursor() as cur:
            if venue_id:
                cur.execute("""
                    SELECT s.* FROM shifts s
                    JOIN rosters r ON s.roster_id = r.id
                    WHERE s.employee_id = %s AND r.venue_id = %s
                    ORDER BY s.shift_date, s.start_time
                """, (employee_id, venue_id))
            else:
                cur.execute("""
                    SELECT * FROM shifts WHERE employee_id = %s
                    ORDER BY shift_date, start_time
                """, (employee_id,))
            return [self._row_to_shift(r) for r in cur.fetchall()]

    # --- Xero Credentials ---

    def save_xero_credentials(self, venue_id: str, credentials_dict: dict) -> None:
        """Save Xero OAuth credentials."""
        with self._cursor() as cur:
            cur.execute("""
                INSERT INTO xero_credentials
                    (venue_id, client_id, client_secret, tenant_id, access_token,
                     refresh_token, token_expires, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (venue_id) DO UPDATE SET
                    access_token=EXCLUDED.access_token,
                    refresh_token=EXCLUDED.refresh_token,
                    token_expires=EXCLUDED.token_expires,
                    updated_at=EXCLUDED.updated_at
            """, (
                venue_id,
                credentials_dict.get("client_id"),
                credentials_dict.get("client_secret"),
                credentials_dict.get("tenant_id"),
                credentials_dict.get("access_token"),
                credentials_dict.get("refresh_token"),
                credentials_dict.get("token_expires"),
                credentials_dict.get("created_at"),
                credentials_dict.get("updated_at"),
            ))

    def get_xero_credentials(self, venue_id: str) -> Optional[dict]:
        """Retrieve Xero credentials."""
        with self._cursor() as cur:
            cur.execute(
                "SELECT * FROM xero_credentials WHERE venue_id = %s",
                (venue_id,)
            )
            row = cur.fetchone()
            if row:
                return {
                    "venue_id": row["venue_id"],
                    "client_id": row["client_id"],
                    "client_secret": row["client_secret"],
                    "tenant_id": row["tenant_id"],
                    "access_token": row["access_token"],
                    "refresh_token": row["refresh_token"],
                    "token_expires": row["token_expires"],
                    "created_at": row["created_at"],
                    "updated_at": row["updated_at"],
                }
        return None

    def delete_xero_credentials(self, venue_id: str) -> None:
        """Delete Xero credentials."""
        with self._cursor() as cur:
            cur.execute("DELETE FROM xero_credentials WHERE venue_id = %s", (venue_id,))

    # --- Webhook Events (Idempotency) ---

    def is_webhook_processed(self, webhook_id: str) -> bool:
        """Check if a webhook has already been processed."""
        with self._cursor() as cur:
            cur.execute(
                "SELECT id FROM webhook_events WHERE webhook_id = %s",
                (webhook_id,)
            )
            return cur.fetchone() is not None

    def save_webhook_event(self, webhook_id: str, event_type: str, payload_hash: str) -> None:
        """Record a processed webhook event."""
        with self._cursor() as cur:
            cur.execute("""
                INSERT INTO webhook_events (webhook_id, event_type, payload_hash, processed_at)
                VALUES (%s, %s, %s, now())
                ON CONFLICT (webhook_id) DO NOTHING
            """, (webhook_id, event_type, payload_hash))

    # --- Users ---

    def save_user(self, user):
        """Save or update a user."""
        with self._cursor() as cur:
            cur.execute("""
                INSERT INTO users (id, email, password_hash, name, role, api_key_hash, is_active, created_at, last_login)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO UPDATE SET
                    email=EXCLUDED.email, password_hash=EXCLUDED.password_hash,
                    name=EXCLUDED.name, role=EXCLUDED.role,
                    api_key_hash=EXCLUDED.api_key_hash, is_active=EXCLUDED.is_active,
                    last_login=EXCLUDED.last_login
            """, (
                user.get("id"), user.get("email"), user.get("password_hash"),
                user.get("name"), user.get("role"), user.get("api_key_hash", ""),
                user.get("is_active", True), user.get("created_at"), user.get("last_login"),
            ))

    def get_user_by_email(self, email):
        """Get user by email."""
        with self._cursor() as cur:
            cur.execute("SELECT * FROM users WHERE email = %s", (email,))
            row = cur.fetchone()
            return dict(row) if row else None

    def get_user_by_id(self, user_id):
        """Get user by ID."""
        with self._cursor() as cur:
            cur.execute("SELECT * FROM users WHERE id = %s", (user_id,))
            row = cur.fetchone()
            return dict(row) if row else None

    def list_users(self):
        """List all users."""
        with self._cursor() as cur:
            cur.execute("SELECT * FROM users ORDER BY created_at DESC")
            return [dict(row) for row in cur.fetchall()]

    def save_refresh_token(self, token_hash, user_id, expires_at):
        """Save a refresh token."""
        with self._cursor() as cur:
            cur.execute("""
                INSERT INTO refresh_tokens (token_hash, user_id, expires_at, is_revoked, created_at)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (token_hash) DO UPDATE SET
                    expires_at=EXCLUDED.expires_at, is_revoked=EXCLUDED.is_revoked
            """, (token_hash, user_id, expires_at, False, datetime.utcnow()))

    def get_refresh_token(self, token_hash):
        """Get refresh token by hash."""
        with self._cursor() as cur:
            cur.execute("SELECT * FROM refresh_tokens WHERE token_hash = %s", (token_hash,))
            row = cur.fetchone()
            return dict(row) if row else None

    def revoke_refresh_token(self, token_hash):
        """Revoke a refresh token."""
        with self._cursor() as cur:
            cur.execute(
                "UPDATE refresh_tokens SET is_revoked = true WHERE token_hash = %s",
                (token_hash,)
            )

    def record_login_attempt(self, email, ip_address, success):
        """Record a login attempt."""
        with self._cursor() as cur:
            cur.execute("""
                INSERT INTO login_attempts (email, ip_address, success, created_at)
                VALUES (%s, %s, %s, %s)
            """, (email, ip_address, success, datetime.utcnow()))

    def check_login_rate_limit(self, ip_address, minutes=1):
        """Count failed login attempts from IP in last N minutes."""
        with self._cursor() as cur:
            cutoff = datetime.utcnow() - timedelta(minutes=minutes)
            cur.execute("""
                SELECT COUNT(*) as count FROM login_attempts
                WHERE ip_address = %s AND success = false AND created_at > %s
            """, (ip_address, cutoff))
            row = cur.fetchone()
            return row["count"] if row else 0

    def save_onboarding_state(self, state: dict) -> None:
        """Save onboarding state for a venue."""
        with self._cursor() as cur:
            cur.execute("""
                INSERT INTO onboarding_states (venue_id, state_data, updated_at)
                VALUES (%s, %s, now())
                ON CONFLICT (venue_id) DO UPDATE SET
                    state_data=EXCLUDED.state_data, updated_at=now()
            """, (state.get("venue_id"), json.dumps(state)))

    def get_onboarding_state(self, venue_id: str) -> Optional[dict]:
        """Get onboarding state for a venue."""
        with self._cursor() as cur:
            cur.execute(
                "SELECT state_data FROM onboarding_states WHERE venue_id = %s",
                (venue_id,)
            )
            row = cur.fetchone()
            if row and row.get("state_data"):
                if isinstance(row["state_data"], str):
                    return json.loads(row["state_data"])
                return row["state_data"]
        return None

    # --- Subscriptions ---

    def save_subscription(self, subscription: dict) -> None:
        """Save or update a subscription record."""
        with self._cursor() as cur:
            cur.execute("""
                INSERT INTO subscriptions
                    (venue_id, stripe_customer_id, stripe_subscription_id, tier, status,
                     current_period_start, current_period_end, payment_method,
                     last_payment_date, next_billing_date, cancel_at_period_end,
                     created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (venue_id) DO UPDATE SET
                    stripe_customer_id=EXCLUDED.stripe_customer_id,
                    stripe_subscription_id=EXCLUDED.stripe_subscription_id,
                    tier=EXCLUDED.tier,
                    status=EXCLUDED.status,
                    current_period_start=EXCLUDED.current_period_start,
                    current_period_end=EXCLUDED.current_period_end,
                    payment_method=EXCLUDED.payment_method,
                    last_payment_date=EXCLUDED.last_payment_date,
                    next_billing_date=EXCLUDED.next_billing_date,
                    cancel_at_period_end=EXCLUDED.cancel_at_period_end,
                    updated_at=EXCLUDED.updated_at
            """, (
                subscription["venue_id"],
                subscription.get("stripe_customer_id"),
                subscription.get("stripe_subscription_id"),
                subscription.get("tier", "starter"),
                subscription.get("status", "inactive"),
                subscription.get("current_period_start"),
                subscription.get("current_period_end"),
                subscription.get("payment_method"),
                subscription.get("last_payment_date"),
                subscription.get("next_billing_date"),
                subscription.get("cancel_at_period_end", False),
                subscription.get("created_at", datetime.utcnow()),
                subscription.get("updated_at", datetime.utcnow()),
            ))

    def get_subscription(self, venue_id: str) -> Optional[dict]:
        """Get subscription for a venue."""
        with self._cursor() as cur:
            cur.execute("SELECT * FROM subscriptions WHERE venue_id = %s", (venue_id,))
            row = cur.fetchone()
            return dict(row) if row else None

    def list_subscriptions(self) -> list[dict]:
        """List all subscriptions."""
        with self._cursor() as cur:
            cur.execute("SELECT * FROM subscriptions ORDER BY created_at DESC")
            return [dict(row) for row in cur.fetchall()]

    def save_billing_event(self, event: dict) -> None:
        """Save a billing event record."""
        with self._cursor() as cur:
            cur.execute("""
                INSERT INTO billing_events
                    (event_id, venue_id, event_type, stripe_event_id, payload, processed, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (
                event["event_id"],
                event.get("venue_id"),
                event.get("event_type"),
                event.get("stripe_event_id"),
                json.dumps(event.get("payload", {})),
                event.get("processed", False),
                event.get("created_at", datetime.utcnow()),
            ))

    def save_plugin_install(self, install: dict) -> None:
        """Save or update a plugin installation record."""
        with self._cursor() as cur:
            cur.execute("""
                INSERT INTO plugin_installs
                    (organisation_id, venue_id, status, tokens, installed_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (organisation_id) DO UPDATE SET
                    venue_id=EXCLUDED.venue_id,
                    status=EXCLUDED.status,
                    tokens=EXCLUDED.tokens,
                    updated_at=now()
            """, (
                install.get("organisation_id"),
                install.get("venue_id"),
                install.get("status", "active"),
                json.dumps(install.get("tokens", {})),
                install.get("installed_at", datetime.utcnow()),
                install.get("updated_at", datetime.utcnow()),
            ))

    def get_plugin_install(self, organisation_id: str) -> Optional[dict]:
        """Get plugin installation record by organisation ID."""
        with self._cursor() as cur:
            cur.execute(
                "SELECT * FROM plugin_installs WHERE organisation_id = %s",
                (organisation_id,)
            )
            row = cur.fetchone()
            return dict(row) if row else None

    def list_plugin_installs(self) -> list[dict]:
        """List all plugin installations."""
        with self._cursor() as cur:
            cur.execute("SELECT * FROM plugin_installs ORDER BY updated_at DESC")
            return [dict(row) for row in cur.fetchall()]

    def save_feed_config(self, venue_id: str, feed_name: str, config: dict) -> None:
        """Save or update feed configuration for a venue."""
        with self._cursor() as cur:
            cur.execute("""
                INSERT INTO feed_configs
                    (venue_id, feed_name, enabled, api_key, poll_interval_minutes,
                     last_updated_at, last_tested_at, last_test_status, custom_params)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (venue_id, feed_name) DO UPDATE SET
                    enabled=EXCLUDED.enabled,
                    api_key=EXCLUDED.api_key,
                    poll_interval_minutes=EXCLUDED.poll_interval_minutes,
                    last_updated_at=EXCLUDED.last_updated_at,
                    last_tested_at=EXCLUDED.last_tested_at,
                    last_test_status=EXCLUDED.last_test_status,
                    custom_params=EXCLUDED.custom_params
            """, (
                venue_id,
                feed_name,
                config.get("enabled", True),
                config.get("api_key", ""),
                config.get("poll_interval_minutes", 30),
                config.get("last_updated_at", datetime.utcnow()),
                config.get("last_tested_at"),
                config.get("last_test_status"),
                json.dumps({k: v for k, v in config.items()
                           if k not in ("venue_id", "feed_name", "enabled", "api_key",
                                      "poll_interval_minutes", "last_updated_at",
                                      "last_tested_at", "last_test_status")}),
            ))

    def get_feed_config(self, venue_id: str, feed_name: str) -> Optional[dict]:
        """Get feed configuration for a venue."""
        with self._cursor() as cur:
            cur.execute(
                """SELECT * FROM feed_configs
                   WHERE venue_id = %s AND feed_name = %s""",
                (venue_id, feed_name)
            )
            row = cur.fetchone()
            if row:
                cfg = dict(row)
                if cfg.get("custom_params"):
                    custom = json.loads(cfg["custom_params"])
                    cfg.update(custom)
                    del cfg["custom_params"]
                return cfg
            return None

    def list_feed_configs(self, venue_id: str) -> list[dict]:
        """List all feed configurations for a venue."""
        with self._cursor() as cur:
            cur.execute(
                "SELECT * FROM feed_configs WHERE venue_id = %s ORDER BY feed_name",
                (venue_id,)
            )
            result = []
            for row in cur.fetchall():
                cfg = dict(row)
                if cfg.get("custom_params"):
                    custom = json.loads(cfg["custom_params"])
                    cfg.update(custom)
                    del cfg["custom_params"]
                result.append(cfg)
            return result

    def save_roster_template(self, template: dict) -> None:
        """Save or update a roster template."""
        with self._cursor() as cur:
            cur.execute("""
                INSERT INTO roster_templates (id, name, venue_id, description, created_by, created_at, updated_at, shift_patterns)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO UPDATE SET
                    name=EXCLUDED.name, description=EXCLUDED.description,
                    updated_at=EXCLUDED.updated_at, shift_patterns=EXCLUDED.shift_patterns
            """, (
                template["id"], template["name"], template["venue_id"],
                template["description"], template["created_by"],
                template["created_at"], template.get("updated_at"),
                json.dumps(template.get("shift_patterns", []))
            ))

    def get_roster_template(self, template_id: str) -> Optional[dict]:
        """Get a roster template by ID."""
        with self._cursor() as cur:
            cur.execute("SELECT * FROM roster_templates WHERE id = %s", (template_id,))
            row = cur.fetchone()
            if not row:
                return None
            data = dict(row)
            if data.get("shift_patterns"):
                data["shift_patterns"] = json.loads(data["shift_patterns"])
            return data

    def list_roster_templates(self, venue_id: str) -> list[dict]:
        """List all roster templates for a venue."""
        with self._cursor() as cur:
            cur.execute(
                "SELECT * FROM roster_templates WHERE venue_id = %s ORDER BY created_at DESC",
                (venue_id,)
            )
            result = []
            for row in cur.fetchall():
                data = dict(row)
                if data.get("shift_patterns"):
                    data["shift_patterns"] = json.loads(data["shift_patterns"])
                result.append(data)
            return result

    def delete_roster_template(self, template_id: str) -> None:
        """Delete a roster template by ID."""
        with self._cursor() as cur:
            cur.execute("DELETE FROM roster_templates WHERE id = %s", (template_id,))

    def save_password_reset_token(self, token_hash: str, user_id: str, expires_at: datetime) -> None:
        """Save a password reset token."""
        with self._cursor() as cur:
            cur.execute("""
                INSERT INTO password_reset_tokens (token_hash, user_id, expires_at, created_at)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (token_hash) DO UPDATE SET
                    expires_at=EXCLUDED.expires_at
            """, (token_hash, user_id, expires_at, datetime.utcnow()))

    def get_password_reset_token(self, token_hash: str) -> Optional[dict]:
        """Get a password reset token by hash."""
        with self._cursor() as cur:
            cur.execute(
                "SELECT * FROM password_reset_tokens WHERE token_hash = %s",
                (token_hash,)
            )
            row = cur.fetchone()
            return dict(row) if row else None

    def delete_password_reset_token(self, token_hash: str) -> None:
        """Delete a password reset token."""
        with self._cursor() as cur:
            cur.execute(
                "DELETE FROM password_reset_tokens WHERE token_hash = %s",
                (token_hash,)
            )

    def save_email_verification_token(self, token_hash: str, user_id: str, expires_at: datetime) -> None:
        """Save an email verification token."""
        with self._cursor() as cur:
            cur.execute("""
                INSERT INTO email_verification_tokens (token_hash, user_id, expires_at, created_at)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (token_hash) DO UPDATE SET
                    expires_at=EXCLUDED.expires_at
            """, (token_hash, user_id, expires_at, datetime.utcnow()))

    def get_email_verification_token(self, token_hash: str) -> Optional[dict]:
        """Get an email verification token by hash."""
        with self._cursor() as cur:
            cur.execute(
                "SELECT * FROM email_verification_tokens WHERE token_hash = %s",
                (token_hash,)
            )
            row = cur.fetchone()
            return dict(row) if row else None

    def delete_email_verification_token(self, token_hash: str) -> None:
        """Delete an email verification token."""
        with self._cursor() as cur:
            cur.execute(
                "DELETE FROM email_verification_tokens WHERE token_hash = %s",
                (token_hash,)
            )

    def save_webhook_subscription(self, subscription: dict) -> None:
        """Save or update a webhook subscription."""
        with self._cursor() as cur:
            cur.execute("""
                INSERT INTO webhook_subscriptions
                    (id, venue_id, callback_url, events, secret, active, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO UPDATE SET
                    callback_url=EXCLUDED.callback_url,
                    events=EXCLUDED.events,
                    active=EXCLUDED.active,
                    updated_at=EXCLUDED.updated_at
            """, (
                subscription.get("id"),
                subscription.get("venue_id"),
                subscription.get("callback_url"),
                json.dumps(subscription.get("events", [])),
                subscription.get("secret"),
                subscription.get("active", True),
                subscription.get("created_at"),
                subscription.get("updated_at"),
            ))

    def get_webhook_subscription(self, subscription_id: str) -> Optional[dict]:
        """Get a webhook subscription by ID."""
        with self._cursor() as cur:
            cur.execute(
                "SELECT * FROM webhook_subscriptions WHERE id = %s",
                (subscription_id,)
            )
            row = cur.fetchone()
            if row:
                result = dict(row)
                if isinstance(result.get("events"), str):
                    result["events"] = json.loads(result["events"])
                return result
        return None

    def list_webhook_subscriptions(self, venue_id: str) -> list[dict]:
        """List all webhook subscriptions for a venue."""
        with self._cursor() as cur:
            cur.execute(
                "SELECT * FROM webhook_subscriptions WHERE venue_id = %s ORDER BY created_at DESC",
                (venue_id,)
            )
            result = []
            for row in cur.fetchall():
                sub = dict(row)
                if isinstance(sub.get("events"), str):
                    sub["events"] = json.loads(sub["events"])
                result.append(sub)
            return result

    def delete_webhook_subscription(self, subscription_id: str) -> None:
        """Delete a webhook subscription."""
        with self._cursor() as cur:
            # Delete subscriptions
            cur.execute(
                "DELETE FROM webhook_subscriptions WHERE id = %s",
                (subscription_id,)
            )
            # Delete associated deliveries
            cur.execute(
                "DELETE FROM webhook_deliveries WHERE subscription_id = %s",
                (subscription_id,)
            )

    def save_webhook_delivery(self, delivery: dict) -> None:
        """Save a webhook delivery record."""
        with self._cursor() as cur:
            cur.execute("""
                INSERT INTO webhook_deliveries
                    (id, subscription_id, event_type, status, response_code,
                     attempts, last_attempt_at, next_retry_at, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO UPDATE SET
                    status=EXCLUDED.status,
                    response_code=EXCLUDED.response_code,
                    attempts=EXCLUDED.attempts,
                    last_attempt_at=EXCLUDED.last_attempt_at,
                    next_retry_at=EXCLUDED.next_retry_at
            """, (
                delivery.get("id"),
                delivery.get("subscription_id"),
                delivery.get("event_type"),
                delivery.get("status"),
                delivery.get("response_code"),
                delivery.get("attempts"),
                delivery.get("last_attempt_at"),
                delivery.get("next_retry_at"),
                delivery.get("created_at"),
            ))

    def list_webhook_deliveries(self, subscription_id: str, limit: int) -> list[dict]:
        """List webhook delivery records for a subscription."""
        with self._cursor() as cur:
            cur.execute("""
                SELECT * FROM webhook_deliveries
                WHERE subscription_id = %s
                ORDER BY created_at DESC
                LIMIT %s
            """, (subscription_id, limit))
            return [dict(row) for row in cur.fetchall()]

    def get_webhook_delivery(self, delivery_id: str) -> Optional[dict]:
        """Get a webhook delivery record by ID."""
        with self._cursor() as cur:
            cur.execute(
                "SELECT * FROM webhook_deliveries WHERE id = %s",
                (delivery_id,)
            )
            row = cur.fetchone()
            return dict(row) if row else None

    def list_pending_retries(self, before: datetime) -> list[dict]:
        """List pending webhook deliveries ready for retry."""
        with self._cursor() as cur:
            cur.execute("""
                SELECT * FROM webhook_deliveries
                WHERE status = 'pending'
                  AND next_retry_at IS NOT NULL
                  AND next_retry_at <= %s
                ORDER BY next_retry_at ASC
                LIMIT 100
            """, (before,))
            return [dict(row) for row in cur.fetchall()]

    def save_dead_letter(self, dead_letter: dict) -> None:
        """Save a dead letter delivery record."""
        with self._cursor() as cur:
            cur.execute("""
                INSERT INTO webhook_dead_letters
                    (id, url, payload, headers, venue_id, subscription_id,
                     event_type, status, attempt, attempts, created_at,
                     dead_lettered_at, error, response_code, last_attempt_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO UPDATE SET
                    status=EXCLUDED.status,
                    dead_lettered_at=EXCLUDED.dead_lettered_at
            """, (
                dead_letter.get("id"),
                dead_letter.get("url"),
                json.dumps(dead_letter.get("payload", {})),
                json.dumps(dead_letter.get("headers", {})),
                dead_letter.get("venue_id"),
                dead_letter.get("subscription_id"),
                dead_letter.get("event_type"),
                dead_letter.get("status"),
                dead_letter.get("attempt", 0),
                json.dumps(dead_letter.get("attempts", [])),
                dead_letter.get("created_at"),
                dead_letter.get("dead_lettered_at"),
                dead_letter.get("error"),
                dead_letter.get("response_code"),
                dead_letter.get("last_attempt_at"),
            ))

    def list_dead_letters(self, venue_id: Optional[str] = None, limit: int = 50) -> list[dict]:
        """List dead letter queue entries."""
        with self._cursor() as cur:
            if venue_id:
                cur.execute("""
                    SELECT * FROM webhook_dead_letters
                    WHERE venue_id = %s
                    ORDER BY dead_lettered_at DESC NULLS LAST
                    LIMIT %s
                """, (venue_id, limit))
            else:
                cur.execute("""
                    SELECT * FROM webhook_dead_letters
                    ORDER BY dead_lettered_at DESC NULLS LAST
                    LIMIT %s
                """, (limit,))
            return [dict(row) for row in cur.fetchall()]

    def delete_dead_letter(self, delivery_id: str) -> None:
        """Delete a dead letter entry."""
        with self._cursor() as cur:
            cur.execute(
                "DELETE FROM webhook_dead_letters WHERE id = %s",
                (delivery_id,)
            )

    def purge_dead_letters(self, before: datetime) -> int:
        """Delete dead letter entries older than the specified date."""
        with self._cursor() as cur:
            cur.execute(
                "DELETE FROM webhook_dead_letters WHERE dead_lettered_at < %s",
                (before,)
            )
            return cur.rowcount

    def save_shift_swap(self, swap: dict) -> None:
        """Save or update a shift swap record."""
        with self._cursor() as cur:
            cur.execute("""
                INSERT INTO shift_swaps (
                    id, shift_id, offered_by, requested_by, my_shift_id,
                    offered_shift_id, date, start_time, end_time, role, venue,
                    venue_id, status, message, created_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO UPDATE SET
                    status = EXCLUDED.status,
                    message = EXCLUDED.message
            """, (
                swap.get("id"),
                swap.get("shift_id"),
                swap.get("offered_by"),
                swap.get("requested_by"),
                swap.get("my_shift_id"),
                swap.get("offered_shift_id"),
                swap.get("date"),
                swap.get("start_time"),
                swap.get("end_time"),
                swap.get("role"),
                swap.get("venue"),
                swap.get("venue_id"),
                swap.get("status", "pending"),
                swap.get("message", ""),
                swap.get("created_at", datetime.utcnow().isoformat()),
            ))

    def list_shift_swaps(self, venue_id: Optional[str] = None) -> list[dict]:
        """List shift swaps, optionally filtered by venue."""
        with self._cursor() as cur:
            if venue_id:
                cur.execute("""
                    SELECT * FROM shift_swaps
                    WHERE venue_id = %s
                    ORDER BY created_at DESC
                """, (venue_id,))
            else:
                cur.execute("""
                    SELECT * FROM shift_swaps
                    ORDER BY created_at DESC
                """)
            return [dict(row) for row in cur.fetchall()]

    def get_shift_swap(self, swap_id: str) -> Optional[dict]:
        """Get a shift swap record by ID."""
        with self._cursor() as cur:
            cur.execute("SELECT * FROM shift_swaps WHERE id = %s", (swap_id,))
            row = cur.fetchone()
            return dict(row) if row else None

    # --- Data Retention & Privacy (Australian Privacy Act 1988) ---

    def purge_old_webhook_events(self, before_date: datetime) -> int:
        """Delete webhook events older than the specified date. Returns count deleted."""
        with self._cursor() as cur:
            cur.execute("""
                DELETE FROM webhook_events
                WHERE processed_at < %s
                RETURNING id
            """, (before_date,))
            return len(cur.fetchall())

    def purge_old_login_attempts(self, before_date: datetime) -> int:
        """Delete login attempts older than the specified date. Returns count deleted."""
        with self._cursor() as cur:
            cur.execute("""
                DELETE FROM login_attempts
                WHERE created_at < %s
                RETURNING id
            """, (before_date,))
            return len(cur.fetchall())

    def purge_revoked_tokens(self, before_date: datetime) -> int:
        """Delete revoked refresh tokens older than the specified date. Returns count deleted."""
        with self._cursor() as cur:
            cur.execute("""
                DELETE FROM refresh_tokens
                WHERE is_revoked = true AND expires_at < %s
                RETURNING id
            """, (before_date,))
            return len(cur.fetchall())

    def save_consent(self, user_id: str, consent_type: str, granted: bool, timestamp: datetime) -> None:
        """Record a user's privacy consent."""
        with self._cursor() as cur:
            cur.execute("""
                INSERT INTO privacy_consents (user_id, consent_type, granted, timestamp)
                VALUES (%s, %s, %s, %s)
            """, (user_id, consent_type, granted, timestamp))

    def get_consents(self, user_id: str) -> list[dict]:
        """Get all consent records for a user."""
        with self._cursor() as cur:
            cur.execute("""
                SELECT user_id, consent_type, granted, timestamp
                FROM privacy_consents
                WHERE user_id = %s
                ORDER BY timestamp DESC
            """, (user_id,))
            return [dict(row) for row in cur.fetchall()]

    def save_privacy_log(self, entry: dict) -> None:
        """Save a privacy audit log entry."""
        with self._cursor() as cur:
            cur.execute("""
                INSERT INTO privacy_audit_log (user_id, action, resource_type, details, logged_at)
                VALUES (%s, %s, %s, %s, %s)
            """, (
                entry.get('user_id'),
                entry.get('action'),
                entry.get('resource_type'),
                json.dumps(entry.get('details', {})),
                entry.get('logged_at', datetime.utcnow()),
            ))

    def list_privacy_logs(self, user_id: Optional[str] = None, limit: int = 100) -> list[dict]:
        """List privacy audit logs, optionally filtered by user_id."""
        with self._cursor() as cur:
            if user_id:
                cur.execute("""
                    SELECT user_id, action, resource_type, details, logged_at
                    FROM privacy_audit_log
                    WHERE user_id = %s
                    ORDER BY logged_at DESC
                    LIMIT %s
                """, (user_id, limit))
            else:
                cur.execute("""
                    SELECT user_id, action, resource_type, details, logged_at
                    FROM privacy_audit_log
                    ORDER BY logged_at DESC
                    LIMIT %s
                """, (limit,))

            return [dict(row) for row in cur.fetchall()]

    def anonymise_employee(self, employee_id: str) -> None:
        """Mark an employee as anonymised (anonymise PII, preserve history)."""
        with self._cursor() as cur:
            # Update employee record
            anon_name = f"Anonymised Employee #{employee_id[:8]}"
            anon_email = f"anon_{employee_id[:8]}@deleted.local"

            cur.execute("""
                UPDATE employees
                SET name = %s, email = %s, phone = NULL, anonymised_at = %s, updated_at = now()
                WHERE id = %s
            """, (anon_name, anon_email, datetime.utcnow(), employee_id))

            # Record in anonymisation log
            cur.execute("""
                INSERT INTO anonymised_employees (employee_id, anonymised_at)
                VALUES (%s, %s)
                ON CONFLICT (employee_id) DO UPDATE SET anonymised_at = EXCLUDED.anonymised_at
            """, (employee_id, datetime.utcnow()))

    def get_anonymised_employees(self) -> list[dict]:
        """Get list of all anonymised employees."""
        with self._cursor() as cur:
            cur.execute("""
                SELECT employee_id, anonymised_at
                FROM anonymised_employees
                ORDER BY anonymised_at DESC
            """)
            return [dict(row) for row in cur.fetchall()]

    # --- Analytics Data Layer ---

    def get_rosters_by_date_range(
        self, venue_id: str, start_date: date, end_date: date
    ) -> list[Roster]:
        """Get all rosters for a venue within a date range."""
        with self._cursor() as cur:
            cur.execute("""
                SELECT r.* FROM rosters r
                WHERE r.venue_id = %s
                  AND r.week_start <= %s
                  AND r.week_end >= %s
                ORDER BY r.week_start
            """, (venue_id, end_date, start_date))

            rosters = []
            for row in cur.fetchall():
                # Reconstruct Roster with shifts
                shifts = []
                cur.execute("""
                    SELECT * FROM shifts WHERE roster_id = %s
                """, (row['id'],))

                for shift_row in cur.fetchall():
                    shift = Shift(
                        id=shift_row['id'],
                        employee_id=shift_row['employee_id'],
                        date=shift_row['date'],
                        start_time=shift_row['start_time'],
                        end_time=shift_row['end_time'],
                        break_minutes=shift_row.get('break_minutes', 0),
                        status=ShiftStatus(shift_row['status']),
                        role=shift_row.get('role', ''),
                        cost=Decimal(str(shift_row['cost'])) if shift_row.get('cost') else None,
                        penalty_multiplier=float(shift_row.get('penalty_multiplier', 1.0)),
                    )
                    shifts.append(shift)

                roster = Roster(
                    id=row['id'],
                    venue_id=row['venue_id'],
                    week_start=row['week_start'],
                    week_end=row['week_end'],
                    shifts=shifts,
                    total_cost=Decimal(str(row['total_cost'])) if row.get('total_cost') else None,
                    created_at=row['created_at'],
                )
                rosters.append(roster)

            return rosters

    def get_revenue_snapshots(
        self, venue_id: str, start_date: date, end_date: date
    ) -> list[dict]:
        """Get revenue data for a venue within a date range."""
        with self._cursor() as cur:
            cur.execute("""
                SELECT date, revenue FROM revenue_snapshots
                WHERE venue_id = %s
                  AND date >= %s
                  AND date <= %s
                ORDER BY date
            """, (venue_id, start_date, end_date))

            results = []
            for row in cur.fetchall():
                results.append({
                    "date": row['date'],
                    "revenue": Decimal(str(row['revenue'])),
                })

            return results

    def save_analytics_snapshot(self, snapshot: dict) -> None:
        """Save an analytics snapshot."""
        with self._cursor() as cur:
            cur.execute("""
                INSERT INTO analytics_snapshots
                    (venue_id, date, metric_type, value, created_at)
                VALUES (%s, %s, %s, %s, %s)
            """, (
                snapshot['venue_id'],
                snapshot.get('date'),
                snapshot.get('metric_type'),
                json.dumps(snapshot.get('value')),
                snapshot.get('created_at', datetime.utcnow()),
            ))

    def get_analytics_snapshots(
        self,
        venue_id: str,
        metric_type: str,
        start_date: date,
        end_date: date,
    ) -> list[dict]:
        """Get analytics snapshots for a venue and metric type."""
        with self._cursor() as cur:
            cur.execute("""
                SELECT venue_id, date, metric_type, value, created_at
                FROM analytics_snapshots
                WHERE venue_id = %s
                  AND metric_type = %s
                  AND date >= %s
                  AND date <= %s
                ORDER BY date
            """, (venue_id, metric_type, start_date, end_date))

            results = []
            for row in cur.fetchall():
                results.append({
                    "venue_id": row['venue_id'],
                    "date": row['date'],
                    "metric_type": row['metric_type'],
                    "value": json.loads(row['value']),
                    "created_at": row['created_at'],
                })

            return results

    def save_audit_log(self, entry: dict) -> None:
        """Save an audit log entry."""
        with self._cursor() as cur:
            cur.execute("""
                INSERT INTO audit_logs (venue_id, user_id, action, resource_type, resource_id, details, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (
                entry.get('venue_id'),
                entry.get('user_id'),
                entry.get('action'),
                entry.get('resource_type'),
                entry.get('resource_id'),
                json.dumps(entry.get('details', {})),
                entry.get('created_at', datetime.utcnow()),
            ))

    def list_audit_logs(
        self, venue_id: str, limit: int = 100, offset: int = 0
    ) -> list[dict]:
        """List audit logs for a venue."""
        with self._cursor() as cur:
            cur.execute("""
                SELECT id, venue_id, user_id, action, resource_type, resource_id, details, created_at
                FROM audit_logs
                WHERE venue_id = %s
                ORDER BY created_at DESC
                LIMIT %s OFFSET %s
            """, (venue_id, limit, offset))

            results = []
            for row in cur.fetchall():
                result = dict(row)
                if result.get('details'):
                    result['details'] = json.loads(result['details'])
                results.append(result)
            return results

    # --- White-Label Theming ---

    def save_theme(self, venue_id: str, theme: dict) -> None:
        """Save or update a theme configuration for a venue."""
        with self._cursor() as cur:
            cur.execute("""
                INSERT INTO themes (venue_id, config)
                VALUES (%s, %s)
                ON CONFLICT (venue_id) DO UPDATE SET config = EXCLUDED.config, updated_at = now()
            """, (venue_id, json.dumps(theme)))

    def get_theme(self, venue_id: str) -> Optional[dict]:
        """Get a theme configuration for a venue. Returns None if not set."""
        with self._cursor() as cur:
            cur.execute("""
                SELECT config FROM themes WHERE venue_id = %s
            """, (venue_id,))

            row = cur.fetchone()
            if row:
                return json.loads(row[0])
            return None

    def delete_theme(self, venue_id: str) -> None:
        """Delete a theme configuration, resetting to defaults."""
        with self._cursor() as cur:
            cur.execute("""
                DELETE FROM themes WHERE venue_id = %s
            """, (venue_id,))

    # --- Credential Management (API Keys & Webhook Secrets) ---

    def save_api_key_record(self, record: dict) -> None:
        """Save or update an API key record."""
        with self._cursor() as cur:
            cur.execute("""
                INSERT INTO api_key_records
                    (id, user_id, name, key_hash, is_active, created_at, expires_at,
                     last_used_at, usage_count, revoked_at, suspicious_flags)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO UPDATE SET
                    name=EXCLUDED.name,
                    is_active=EXCLUDED.is_active,
                    last_used_at=EXCLUDED.last_used_at,
                    usage_count=EXCLUDED.usage_count,
                    revoked_at=EXCLUDED.revoked_at,
                    suspicious_flags=EXCLUDED.suspicious_flags
            """, (
                record["id"],
                record["user_id"],
                record["name"],
                record["key_hash"],
                record["is_active"],
                record["created_at"],
                record.get("expires_at"),
                record.get("last_used_at"),
                record.get("usage_count", 0),
                record.get("revoked_at"),
                record.get("suspicious_flags", 0),
            ))

    def list_api_key_records(self, user_id: str) -> list[dict]:
        """List all API key records for a user."""
        with self._cursor() as cur:
            cur.execute("""
                SELECT * FROM api_key_records
                WHERE user_id = %s
                ORDER BY created_at DESC
            """, (user_id,))
            return [dict(row) for row in cur.fetchall()]

    def get_api_key_record(self, key_id: str) -> Optional[dict]:
        """Get an API key record by key ID."""
        with self._cursor() as cur:
            cur.execute(
                "SELECT * FROM api_key_records WHERE id = %s",
                (key_id,)
            )
            row = cur.fetchone()
            return dict(row) if row else None

    def save_webhook_secret(self, venue_id: str, secret_record: dict) -> None:
        """Save or update a webhook secret record."""
        with self._cursor() as cur:
            cur.execute("""
                INSERT INTO webhook_secrets
                    (id, venue_id, secret_hash, is_active, grace_expires_at, created_at, rotated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO UPDATE SET
                    is_active=EXCLUDED.is_active,
                    grace_expires_at=EXCLUDED.grace_expires_at,
                    rotated_at=EXCLUDED.rotated_at
            """, (
                secret_record["id"],
                venue_id,
                secret_record["secret_hash"],
                secret_record["is_active"],
                secret_record.get("grace_expires_at"),
                secret_record["created_at"],
                secret_record.get("rotated_at"),
            ))

    def get_webhook_secrets(self, venue_id: str) -> list[dict]:
        """Get all webhook secret records for a venue."""
        with self._cursor() as cur:
            cur.execute("""
                SELECT * FROM webhook_secrets
                WHERE venue_id = %s
                ORDER BY created_at DESC
            """, (venue_id,))
            return [dict(row) for row in cur.fetchall()]

    def save_preference_profile(self, employee_id: str, profile: dict) -> None:
        """Save or update a preference profile for an employee."""
        with self._cursor() as cur:
            cur.execute("""
                INSERT INTO preference_profiles (employee_id, profile_data, updated_at)
                VALUES (%s, %s, %s)
                ON CONFLICT (employee_id) DO UPDATE
                SET profile_data = EXCLUDED.profile_data, updated_at = EXCLUDED.updated_at
            """, (employee_id, json.dumps(profile), datetime.utcnow()))

    def get_preference_profile(self, employee_id: str) -> Optional[dict]:
        """Get a preference profile for an employee. Returns None if not found."""
        with self._cursor() as cur:
            cur.execute("""
                SELECT profile_data FROM preference_profiles
                WHERE employee_id = %s
            """, (employee_id,))
            row = cur.fetchone()
            if row:
                return json.loads(row['profile_data'])
            return None

    def list_preference_profiles(self, venue_id: str) -> list[dict]:
        """List all preference profiles for a venue."""
        with self._cursor() as cur:
            cur.execute("""
                SELECT profile_data FROM preference_profiles
                WHERE profile_data->>'venue_id' = %s
                ORDER BY updated_at DESC
            """, (venue_id,))
            return [json.loads(row['profile_data']) for row in cur.fetchall()]


    # --- A/B Testing ---

    def save_experiment(self, experiment: dict) -> None:
        """Save or update an experiment."""
        with self._cursor() as cur:
            cur.execute("""
                INSERT INTO ab_experiments (
                    id, name, description, control_strategy, variant_strategy,
                    start_date, end_date, status, created_at, updated_at,
                    control_venues, variant_venues, minimum_sample_size
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO UPDATE SET
                    name=EXCLUDED.name, description=EXCLUDED.description,
                    status=EXCLUDED.status, updated_at=EXCLUDED.updated_at,
                    control_venues=EXCLUDED.control_venues,
                    variant_venues=EXCLUDED.variant_venues
            """, (
                experiment["id"],
                experiment["name"],
                experiment["description"],
                experiment["control_strategy"],
                experiment["variant_strategy"],
                experiment["start_date"],
                experiment["end_date"],
                experiment["status"],
                experiment["created_at"],
                experiment["updated_at"],
                json.dumps(experiment.get("control_venues", [])),
                json.dumps(experiment.get("variant_venues", [])),
                experiment.get("minimum_sample_size", 30),
            ))

    def get_experiment(self, experiment_id: str) -> Optional[dict]:
        """Get an experiment by ID."""
        with self._cursor() as cur:
            cur.execute("""
                SELECT id, name, description, control_strategy, variant_strategy,
                       start_date, end_date, status, created_at, updated_at,
                       control_venues, variant_venues, minimum_sample_size
                FROM ab_experiments
                WHERE id = %s
            """, (experiment_id,))

            row = cur.fetchone()
            if not row:
                return None

            return {
                "id": row["id"],
                "name": row["name"],
                "description": row["description"],
                "control_strategy": row["control_strategy"],
                "variant_strategy": row["variant_strategy"],
                "start_date": row["start_date"].isoformat() if hasattr(row["start_date"], "isoformat") else row["start_date"],
                "end_date": row["end_date"].isoformat() if hasattr(row["end_date"], "isoformat") else row["end_date"],
                "status": row["status"],
                "created_at": row["created_at"].isoformat() if hasattr(row["created_at"], "isoformat") else row["created_at"],
                "updated_at": row["updated_at"].isoformat() if hasattr(row["updated_at"], "isoformat") else row["updated_at"],
                "control_venues": json.loads(row["control_venues"]) if isinstance(row["control_venues"], str) else row["control_venues"],
                "variant_venues": json.loads(row["variant_venues"]) if isinstance(row["variant_venues"], str) else row["variant_venues"],
                "minimum_sample_size": row["minimum_sample_size"],
            }

    def list_experiments(self, active_only: bool = False) -> list[dict]:
        """List all experiments, optionally filtering to active only."""
        with self._cursor() as cur:
            query = """
                SELECT id, name, description, control_strategy, variant_strategy,
                       start_date, end_date, status, created_at, updated_at,
                       control_venues, variant_venues, minimum_sample_size
                FROM ab_experiments
            """
            if active_only:
                query += " WHERE status = 'active'"
            query += " ORDER BY created_at DESC"

            cur.execute(query)

            results = []
            for row in cur.fetchall():
                results.append({
                    "id": row["id"],
                    "name": row["name"],
                    "description": row["description"],
                    "control_strategy": row["control_strategy"],
                    "variant_strategy": row["variant_strategy"],
                    "start_date": row["start_date"].isoformat() if hasattr(row["start_date"], "isoformat") else row["start_date"],
                    "end_date": row["end_date"].isoformat() if hasattr(row["end_date"], "isoformat") else row["end_date"],
                    "status": row["status"],
                    "created_at": row["created_at"].isoformat() if hasattr(row["created_at"], "isoformat") else row["created_at"],
                    "updated_at": row["updated_at"].isoformat() if hasattr(row["updated_at"], "isoformat") else row["updated_at"],
                    "control_venues": json.loads(row["control_venues"]) if isinstance(row["control_venues"], str) else row["control_venues"],
                    "variant_venues": json.loads(row["variant_venues"]) if isinstance(row["variant_venues"], str) else row["variant_venues"],
                    "minimum_sample_size": row["minimum_sample_size"],
                })

            return results

    def save_experiment_outcome(self, outcome: dict) -> None:
        """Save an experiment outcome."""
        with self._cursor() as cur:
            cur.execute("""
                INSERT INTO ab_experiment_outcomes (
                    id, experiment_id, venue_id, roster_id, "group",
                    total_labour_cost, labour_percentage, demand_coverage_pct,
                    compliance_score, staff_satisfaction_proxy,
                    overtime_hours, penalty_hours, recorded_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                outcome["id"],
                outcome["experiment_id"],
                outcome["venue_id"],
                outcome["roster_id"],
                outcome["group"],
                outcome["total_labour_cost"],
                outcome["labour_percentage"],
                outcome["demand_coverage_pct"],
                outcome["compliance_score"],
                outcome["staff_satisfaction_proxy"],
                outcome["overtime_hours"],
                outcome["penalty_hours"],
                outcome["recorded_at"],
            ))

    def list_experiment_outcomes(self, experiment_id: str) -> list[dict]:
        """List all outcomes for an experiment."""
        with self._cursor() as cur:
            cur.execute("""
                SELECT id, experiment_id, venue_id, roster_id, "group",
                       total_labour_cost, labour_percentage, demand_coverage_pct,
                       compliance_score, staff_satisfaction_proxy,
                       overtime_hours, penalty_hours, recorded_at
                FROM ab_experiment_outcomes
                WHERE experiment_id = %s
                ORDER BY recorded_at DESC
            """, (experiment_id,))

            results = []
            for row in cur.fetchall():
                results.append({
                    "id": row["id"],
                    "experiment_id": row["experiment_id"],
                    "venue_id": row["venue_id"],
                    "roster_id": row["roster_id"],
                    "group": row["group"],
                    "total_labour_cost": float(row["total_labour_cost"]),
                    "labour_percentage": float(row["labour_percentage"]),
                    "demand_coverage_pct": float(row["demand_coverage_pct"]),
                    "compliance_score": float(row["compliance_score"]),
                    "staff_satisfaction_proxy": float(row["staff_satisfaction_proxy"]),
                    "overtime_hours": float(row["overtime_hours"]),
                    "penalty_hours": float(row["penalty_hours"]),
                    "recorded_at": row["recorded_at"],
                })

            return results

    def save_payroll_batch(self, batch: dict) -> None:
        """Save or update a payroll batch."""
        with self._cursor() as cur:
            cur.execute("""
                INSERT INTO payroll_batches (batch_id, venue_id, period_start, period_end, status, data, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (batch_id) DO UPDATE
                SET status = EXCLUDED.status, data = EXCLUDED.data
            """, (
                batch["batch_id"],
                batch["venue_id"],
                batch["period_start"],
                batch["period_end"],
                batch.get("status", "draft"),
                json.dumps(batch),
                batch.get("created_at", datetime.utcnow().isoformat()),
            ))

    def get_payroll_batch(self, batch_id: str) -> Optional[dict]:
        """Get a payroll batch by ID."""
        with self._cursor() as cur:
            cur.execute("""
                SELECT data FROM payroll_batches
                WHERE batch_id = %s
            """, (batch_id,))
            row = cur.fetchone()
            if row:
                return json.loads(row["data"])
        return None

    def list_payroll_batches(self, venue_id: str) -> list[dict]:
        """List all payroll batches for a venue."""
        with self._cursor() as cur:
            cur.execute("""
                SELECT data FROM payroll_batches
                WHERE venue_id = %s
                ORDER BY created_at DESC
            """, (venue_id,))
            results = []
            for row in cur.fetchall():
                results.append(json.loads(row["data"]))
            return results

    def save_payroll_export(self, export: dict) -> None:
        """Record a payroll export to external service."""
        with self._cursor() as cur:
            cur.execute("""
                INSERT INTO payroll_exports (batch_id, service, status, data, exported_at)
                VALUES (%s, %s, %s, %s, %s)
            """, (
                export.get("batch_id"),
                export.get("service"),
                export.get("status", "success"),
                json.dumps(export),
                export.get("exported_at", datetime.utcnow().isoformat()),
            ))

    def list_payroll_exports(self, venue_id: str, limit: int = 50) -> list[dict]:
        """List payroll exports for a venue."""
        with self._cursor() as cur:
            cur.execute("""
                SELECT pe.data FROM payroll_exports pe
                JOIN payroll_batches pb ON pe.batch_id = pb.batch_id
                WHERE pb.venue_id = %s
                ORDER BY pe.exported_at DESC
                LIMIT %s
            """, (venue_id, limit))
            results = []
            for row in cur.fetchall():
                results.append(json.loads(row["data"]))
            return results

    # --- Notification Preferences ---

    def save_notification_preferences(self, user_id: str, prefs: dict) -> None:
        """Save or update notification preferences for a user."""
        with self._cursor() as cur:
            cur.execute("""
                INSERT INTO notification_preferences (user_id, preferences, created_at, updated_at)
                VALUES (%s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                ON CONFLICT (user_id) DO UPDATE
                SET preferences=EXCLUDED.preferences, updated_at=CURRENT_TIMESTAMP
            """, (user_id, json.dumps(prefs)))

    def get_notification_preferences(self, user_id: str) -> Optional[dict]:
        """Get notification preferences for a user."""
        with self._cursor() as cur:
            cur.execute("""
                SELECT preferences FROM notification_preferences WHERE user_id = %s
            """, (user_id,))
            row = cur.fetchone()
            if row:
                return json.loads(row["preferences"])
        return None

    # --- Approval Workflow ---

    def save_approval_request(self, request: dict) -> None:
        """Save or update an approval request."""
        with self._cursor() as cur:
            cur.execute("""
                INSERT INTO approval_requests (
                    request_id, roster_id, venue_id, submitted_by, submitted_at,
                    status, reviewed_by, reviewed_at, review_notes, revision_number,
                    escalated_at, escalated_to, tier, auto_approved_by_rules,
                    failed_rules, data, created_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (request_id) DO UPDATE
                SET status=EXCLUDED.status, reviewed_by=EXCLUDED.reviewed_by,
                    reviewed_at=EXCLUDED.reviewed_at, review_notes=EXCLUDED.review_notes,
                    escalated_at=EXCLUDED.escalated_at, escalated_to=EXCLUDED.escalated_to,
                    data=EXCLUDED.data
            """, (
                request.get("id"),
                request.get("roster_id"),
                request.get("venue_id"),
                request.get("submitted_by"),
                request.get("submitted_at"),
                request.get("status"),
                request.get("reviewed_by"),
                request.get("reviewed_at"),
                request.get("review_notes"),
                request.get("revision_number", 1),
                request.get("escalated_at"),
                request.get("escalated_to"),
                request.get("tier"),
                json.dumps(request.get("auto_approved_by_rules", [])),
                json.dumps(request.get("failed_rules", [])),
                json.dumps(request),
                datetime.utcnow(),
            ))

    def get_approval_request(self, request_id: str) -> Optional[dict]:
        """Get an approval request by ID."""
        with self._cursor() as cur:
            cur.execute("""
                SELECT data FROM approval_requests WHERE request_id = %s
            """, (request_id,))
            row = cur.fetchone()
            if row:
                return json.loads(row["data"])
        return None

    def list_approval_requests(
        self,
        venue_id: Optional[str] = None,
        roster_id: Optional[str] = None,
        status: Optional[str] = None,
    ) -> list[dict]:
        """List approval requests with optional filters."""
        with self._cursor() as cur:
            query = "SELECT data FROM approval_requests WHERE 1=1"
            params = []

            if venue_id:
                query += " AND venue_id = %s"
                params.append(venue_id)
            if roster_id:
                query += " AND roster_id = %s"
                params.append(roster_id)
            if status:
                query += " AND status = %s"
                params.append(status)

            query += " ORDER BY submitted_at DESC"

            cur.execute(query, params)
            results = []
            for row in cur.fetchall():
                results.append(json.loads(row["data"]))
            return results

    def save_roster_revision(self, revision: dict) -> None:
        """Save a roster revision with change tracking."""
        import uuid
        with self._cursor() as cur:
            cur.execute("""
                INSERT INTO roster_revisions (
                    revision_id, roster_id, revision_number, changes, created_at, data
                ) VALUES (%s, %s, %s, %s, %s, %s)
            """, (
                revision.get("id", str(uuid.uuid4())),
                revision.get("roster_id"),
                revision.get("revision_number"),
                json.dumps(revision.get("changes", {})),
                revision.get("created_at", datetime.utcnow()),
                json.dumps(revision),
            ))

    def list_roster_revisions(self, roster_id: str) -> list[dict]:
        """List all revisions for a roster."""
        with self._cursor() as cur:
            cur.execute("""
                SELECT data FROM roster_revisions
                WHERE roster_id = %s
                ORDER BY revision_number ASC
            """, (roster_id,))
            results = []
            for row in cur.fetchall():
                results.append(json.loads(row["data"]))
            return results

    # --- Push Notifications ---

    def save_push_subscription(self, user_id: str, subscription: dict) -> None:
        """Save or update a push notification subscription for a user."""
        with self._cursor() as cur:
            cur.execute("""
                INSERT INTO push_subscriptions (user_id, venue_id, subscription_data, created_at, updated_at)
                VALUES (%s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                ON CONFLICT (user_id) DO UPDATE
                SET subscription_data=EXCLUDED.subscription_data, updated_at=CURRENT_TIMESTAMP
            """, (user_id, subscription.get("venue_id"), json.dumps(subscription)))

    def get_push_subscription(self, user_id: str) -> Optional[dict]:
        """Get push notification subscription for a user."""
        with self._cursor() as cur:
            cur.execute("""
                SELECT subscription_data FROM push_subscriptions WHERE user_id = %s
            """, (user_id,))
            row = cur.fetchone()
            if row:
                return json.loads(row["subscription_data"])
        return None

    def delete_push_subscription(self, user_id: str) -> None:
        """Delete push notification subscription for a user."""
        with self._cursor() as cur:
            cur.execute("""
                DELETE FROM push_subscriptions WHERE user_id = %s
            """, (user_id,))

    def list_push_subscriptions(self, venue_id: str) -> list[dict]:
        """List all push subscriptions for staff at a venue."""
        with self._cursor() as cur:
            cur.execute("""
                SELECT subscription_data FROM push_subscriptions WHERE venue_id = %s
            """, (venue_id,))
            results = []
            for row in cur.fetchall():
                results.append(json.loads(row["subscription_data"]))
            return results


    def save_revenue_model(self, venue_id: str, model: dict) -> None:
        """Save or update a trained revenue model for a venue."""
        with self._cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS revenue_models (
                    venue_id TEXT PRIMARY KEY,
                    model_data JSONB,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            cur.execute("""
                INSERT INTO revenue_models (venue_id, model_data, updated_at)
                VALUES (%s, %s, CURRENT_TIMESTAMP)
                ON CONFLICT (venue_id) DO UPDATE
                SET model_data = EXCLUDED.model_data, updated_at = CURRENT_TIMESTAMP
            """, (venue_id, json.dumps(model)))

    def get_revenue_model(self, venue_id: str) -> Optional[dict]:
        """Get trained revenue model for a venue."""
        with self._cursor() as cur:
            cur.execute("""
                SELECT model_data FROM revenue_models
                WHERE venue_id = %s
            """, (venue_id,))
            row = cur.fetchone()
            if row:
                return json.loads(row['model_data'])
        return None

    def save_revenue_actual(self, venue_id: str, date: str, revenue: dict) -> None:
        """Save actual revenue for a date (date in ISO format)."""
        with self._cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS revenue_actuals (
                    id SERIAL PRIMARY KEY,
                    venue_id TEXT,
                    date DATE,
                    revenue_data JSONB,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(venue_id, date)
                )
            """)
            cur.execute("""
                INSERT INTO revenue_actuals (venue_id, date, revenue_data, created_at)
                VALUES (%s, %s, %s, CURRENT_TIMESTAMP)
                ON CONFLICT (venue_id, date) DO UPDATE
                SET revenue_data = EXCLUDED.revenue_data
            """, (venue_id, date, json.dumps(revenue)))

    def list_revenue_actuals(
        self, venue_id: str, start: str, end: str
    ) -> list[dict]:
        """List actual revenue records for a venue within date range (ISO format)."""
        with self._cursor() as cur:
            cur.execute("""
                SELECT venue_id, date, revenue_data FROM revenue_actuals
                WHERE venue_id = %s
                AND date BETWEEN %s AND %s
                ORDER BY date ASC
            """, (venue_id, start, end))
            results = []
            for row in cur.fetchall():
                record = json.loads(row['revenue_data'])
                record['venue_id'] = row['venue_id']
                record['date'] = row['date'].isoformat() if hasattr(row['date'], 'isoformat') else row['date']
                results.append(record)
            return results

    # --- Shift Bidding Marketplace ---

    def save_open_shift(self, shift: dict) -> None:
        """Save or update an open shift."""
        with self._cursor() as cur:
            cur.execute("""
                INSERT INTO open_shifts
                    (id, venue_id, date, start_time, end_time, role_required,
                     skills_required, min_rate, max_rate, posted_by, posted_at,
                     deadline, status, notes, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO UPDATE SET
                    status=EXCLUDED.status,
                    notes=EXCLUDED.notes
            """, (
                shift["id"],
                shift["venue_id"],
                shift["date"],
                shift["start_time"],
                shift["end_time"],
                shift["role_required"],
                json.dumps(shift.get("skills_required", [])),
                shift["min_rate"],
                shift.get("max_rate"),
                shift["posted_by"],
                shift["posted_at"],
                shift["deadline"],
                shift["status"],
                shift.get("notes"),
                datetime.utcnow(),
            ))

    def get_open_shift(self, shift_id: str) -> Optional[dict]:
        """Get an open shift by ID."""
        with self._cursor() as cur:
            cur.execute("""
                SELECT * FROM open_shifts WHERE id = %s
            """, (shift_id,))
            row = cur.fetchone()
            if not row:
                return None
            return {
                "id": row["id"],
                "venue_id": row["venue_id"],
                "date": row["date"].isoformat() if hasattr(row["date"], "isoformat") else str(row["date"]),
                "start_time": row["start_time"].isoformat() if hasattr(row["start_time"], "isoformat") else str(row["start_time"]),
                "end_time": row["end_time"].isoformat() if hasattr(row["end_time"], "isoformat") else str(row["end_time"]),
                "role_required": row["role_required"],
                "skills_required": json.loads(row["skills_required"]) if row["skills_required"] else [],
                "min_rate": str(row["min_rate"]),
                "max_rate": str(row["max_rate"]) if row["max_rate"] else None,
                "posted_by": row["posted_by"],
                "posted_at": row["posted_at"].isoformat() if hasattr(row["posted_at"], "isoformat") else str(row["posted_at"]),
                "deadline": row["deadline"].isoformat() if hasattr(row["deadline"], "isoformat") else str(row["deadline"]),
                "status": row["status"],
                "notes": row.get("notes"),
            }

    def list_open_shifts(self, venue_id: str, status: str) -> list[dict]:
        """List open shifts for a venue filtered by status."""
        with self._cursor() as cur:
            cur.execute("""
                SELECT * FROM open_shifts
                WHERE venue_id = %s AND status = %s
                ORDER BY deadline ASC
            """, (venue_id, status))
            results = []
            for row in cur.fetchall():
                results.append({
                    "id": row["id"],
                    "venue_id": row["venue_id"],
                    "date": row["date"].isoformat() if hasattr(row["date"], "isoformat") else str(row["date"]),
                    "start_time": row["start_time"].isoformat() if hasattr(row["start_time"], "isoformat") else str(row["start_time"]),
                    "end_time": row["end_time"].isoformat() if hasattr(row["end_time"], "isoformat") else str(row["end_time"]),
                    "role_required": row["role_required"],
                    "skills_required": json.loads(row["skills_required"]) if row["skills_required"] else [],
                    "min_rate": str(row["min_rate"]),
                    "max_rate": str(row["max_rate"]) if row["max_rate"] else None,
                    "posted_by": row["posted_by"],
                    "posted_at": row["posted_at"].isoformat() if hasattr(row["posted_at"], "isoformat") else str(row["posted_at"]),
                    "deadline": row["deadline"].isoformat() if hasattr(row["deadline"], "isoformat") else str(row["deadline"]),
                    "status": row["status"],
                    "notes": row.get("notes"),
                })
            return results

    def save_bid(self, bid: dict) -> None:
        """Save or update a bid."""
        with self._cursor() as cur:
            cur.execute("""
                INSERT INTO bids
                    (id, open_shift_id, employee_id, offered_rate, message,
                     seniority_years, preference_score, submitted_at, status, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO UPDATE SET
                    status=EXCLUDED.status,
                    offered_rate=EXCLUDED.offered_rate
            """, (
                bid["id"],
                bid["open_shift_id"],
                bid["employee_id"],
                bid["offered_rate"],
                bid.get("message"),
                bid.get("seniority_years", 0),
                bid.get("preference_score", 0),
                bid["submitted_at"],
                bid["status"],
                datetime.utcnow(),
            ))

    def get_bid(self, bid_id: str) -> Optional[dict]:
        """Get a bid by ID."""
        with self._cursor() as cur:
            cur.execute("""
                SELECT * FROM bids WHERE id = %s
            """, (bid_id,))
            row = cur.fetchone()
            if not row:
                return None
            return {
                "id": row["id"],
                "open_shift_id": row["open_shift_id"],
                "employee_id": row["employee_id"],
                "offered_rate": str(row["offered_rate"]),
                "message": row.get("message"),
                "seniority_years": float(row.get("seniority_years", 0)),
                "preference_score": float(row.get("preference_score", 0)),
                "submitted_at": row["submitted_at"].isoformat() if hasattr(row["submitted_at"], "isoformat") else str(row["submitted_at"]),
                "status": row["status"],
            }

    def list_bids(self, open_shift_id: str) -> list[dict]:
        """List all bids for an open shift."""
        with self._cursor() as cur:
            cur.execute("""
                SELECT * FROM bids
                WHERE open_shift_id = %s
                ORDER BY submitted_at ASC
            """, (open_shift_id,))
            results = []
            for row in cur.fetchall():
                results.append({
                    "id": row["id"],
                    "open_shift_id": row["open_shift_id"],
                    "employee_id": row["employee_id"],
                    "offered_rate": str(row["offered_rate"]),
                    "message": row.get("message"),
                    "seniority_years": float(row.get("seniority_years", 0)),
                    "preference_score": float(row.get("preference_score", 0)),
                    "submitted_at": row["submitted_at"].isoformat() if hasattr(row["submitted_at"], "isoformat") else str(row["submitted_at"]),
                    "status": row["status"],
                })
            return results


# ============================================================================
# Factory
# ============================================================================

_instance: Optional[BaseStore] = None


def get_db() -> BaseStore:
    """Get the database store (singleton). Uses PostgreSQL if DATABASE_URL is set."""
    global _instance
    if _instance is None:
        if DATABASE_URL:
            try:
                _instance = PostgresStore(DATABASE_URL)
                logger.info("Using PostgreSQL store")
            except Exception as e:
                logger.warning("PostgreSQL unavailable (%s), falling back to in-memory", e)
                _instance = MemoryStore()
        else:
            logger.info("No DATABASE_URL set — using in-memory store")
            _instance = MemoryStore()
    return _instance


def reset_db():
    """Reset the database instance (for testing)."""
    global _instance
    _instance = None

# TEMPORARY: These methods will be moved to MemoryStore.__init__ in next update
