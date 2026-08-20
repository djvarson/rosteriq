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


def _json(obj) -> str:
    """json.dumps for anything that goes into a JSONB column.

    Every blob we store is assembled from live objects, and those carry
    datetimes (submitted_at, imported_at) and Decimals (costs). Bare
    json.dumps raises TypeError on both, which surfaces as a 500 on a normal
    action — the roster approval workflow was dead on Postgres for exactly
    this reason, and the tests could not see it because MemoryStore keeps the
    dict as-is. default=str makes the write survive; readers already pass
    these values through str()/Decimal() anyway.
    """
    return json.dumps(obj, default=str)


def _jsonb(value, default=None):
    """Tolerant JSON-column read. psycopg2 returns JSONB columns ALREADY
    parsed (dict/list) while TEXT columns come back raw — json.loads() on a
    parsed value raises TypeError and 500s in production while MemoryStore
    tests stay green. Every JSON column read must go through this."""
    if value is None:
        return default
    if isinstance(value, (dict, list)):
        return value
    try:
        return json.loads(value)
    except (TypeError, ValueError):
        return default

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

    # --- AI agent data helpers -------------------------------------------
    # The AI agent (ai_agent.py) reads venue data through these date-ranged
    # helpers. They're concrete here — delegating to the per-store primitives
    # above — so MemoryStore and PostgresStore both get them without
    # duplication. Previously the agent called these on the store directly,
    # they didn't exist, and every such tool silently returned an error.

    def get_shifts(self, venue_id: str, start_date, end_date) -> list:
        """All shifts for a venue within [start_date, end_date], flattened from
        the venue's rosters (shifts live inside Roster.shifts)."""
        shifts = []
        for roster in (self.get_rosters_by_date_range(venue_id, start_date, end_date) or []):
            for s in (getattr(roster, "shifts", None) or []):
                sd = getattr(s, "date", None)
                if sd is None or (start_date <= sd <= end_date):
                    shifts.append(s)
        return shifts

    def get_venue_config(self, venue_id: str):
        """Alias for get_venue — the AI agent calls it get_venue_config."""
        return self.get_venue(venue_id)

    def get_reservations(self, venue_id: str, start_date, end_date) -> list:
        """Reservations for the AI agent, sourced from ingested direct bookings.
        Returns attribute-accessible objects (the agent reads via getattr)."""
        from types import SimpleNamespace
        s = start_date.isoformat() if hasattr(start_date, "isoformat") else str(start_date)
        e = end_date.isoformat() if hasattr(end_date, "isoformat") else str(end_date)
        try:
            rows = self.get_direct_bookings(venue_id, s, e) or []
        except Exception:
            rows = []
        out = []
        for b in rows:
            d = b if isinstance(b, dict) else {}
            out.append(SimpleNamespace(
                date=d.get("date"),
                time=d.get("time"),
                covers=d.get("covers", d.get("party_size")),
                party_size=d.get("party_size", d.get("covers")),
                name=d.get("name") or d.get("guest_name") or "",
            ))
        return out

    def get_functions(self, venue_id: str, start_date, end_date) -> list:
        """Private functions / events. No dedicated store yet — degrade
        gracefully (empty) rather than erroring the agent's events tool."""
        return []

    # --- Native time clock (timesheets captured by RosterIQ itself) --------

    def save_timesheet(self, ts: dict) -> None:
        """Insert or update a timesheet row (dict with id, venue_id,
        employee_id, work_date, clock_in, clock_out, break_minutes, status)."""
        raise NotImplementedError

    def get_timesheet(self, ts_id: str):
        raise NotImplementedError

    def get_open_timesheet(self, venue_id: str, employee_id: str):
        """The employee's currently-open (not clocked out) timesheet, if any."""
        raise NotImplementedError

    def get_timesheets(self, venue_id: str, start_date, end_date) -> list:
        """Timesheets for a venue whose work_date falls in [start, end]."""
        raise NotImplementedError

    def set_timeclock_pin(self, venue_id: str, employee_id: str, pin_hash: str) -> None:
        raise NotImplementedError

    def get_timeclock_pin(self, venue_id: str, employee_id: str):
        """Return the stored pin hash or None."""
        raise NotImplementedError

    # --- Compliance checklists (opening/closing lists, temp logs) ----------

    def save_checklist_template(self, tpl: dict) -> None:
        raise NotImplementedError

    def get_checklist_template(self, tpl_id: str):
        raise NotImplementedError

    def list_checklist_templates(self, venue_id: str) -> list:
        raise NotImplementedError

    def save_checklist_run(self, run: dict) -> None:
        raise NotImplementedError

    def get_checklist_run(self, run_id: str):
        raise NotImplementedError

    def list_checklist_runs(self, venue_id: str, start_date, end_date) -> list:
        raise NotImplementedError

    # --- Menu costing (ingredients + recipes) ------------------------------

    def save_ingredient(self, ing: dict) -> None:
        raise NotImplementedError

    def get_ingredient(self, ing_id: str):
        raise NotImplementedError

    def list_ingredients(self, venue_id: str) -> list:
        raise NotImplementedError

    def save_recipe(self, recipe: dict) -> None:
        raise NotImplementedError

    def get_recipe(self, recipe_id: str):
        raise NotImplementedError

    def list_recipes(self, venue_id: str) -> list:
        raise NotImplementedError

    # --- Leave / unavailability requests (staff portal) --------------------

    def save_leave_request(self, req: dict) -> None:
        raise NotImplementedError

    def get_leave_request(self, req_id: str):
        raise NotImplementedError

    def list_leave_requests(self, venue_id: str) -> list:
        raise NotImplementedError

    # --- Shift cover requests (staff portal phase 2) -----------------------

    def save_shift_cover(self, cover: dict) -> None:
        raise NotImplementedError

    def get_shift_cover(self, cover_id: str):
        raise NotImplementedError

    def list_shift_covers(self, venue_id: str) -> list:
        raise NotImplementedError

    # --- Announcements (communication hub) ---------------------------------

    def save_announcement(self, ann: dict) -> None:
        raise NotImplementedError

    def get_announcement(self, ann_id: str):
        raise NotImplementedError

    def list_announcements(self, venue_id: str) -> list:
        raise NotImplementedError

    # --- SOP / JSP document library (procedures + acknowledgements) --------

    def save_sop_document(self, doc: dict) -> None:
        raise NotImplementedError

    def get_sop_document(self, doc_id: str):
        raise NotImplementedError

    def delete_sop_document(self, doc_id: str) -> None:
        """Hard-delete a document (routes only allow this when it has no acks)."""
        raise NotImplementedError

    def list_sop_documents(self, venue_id: str, include_inactive: bool = False) -> list:
        raise NotImplementedError

    def save_sop_ack(self, ack: dict) -> None:
        raise NotImplementedError

    def list_sop_acks(self, venue_id: str, doc_id: str = None) -> list:
        raise NotImplementedError

    # --- Team feed (two-way posts: staff + managers) -----------------------

    def save_feed_post(self, post: dict) -> None:
        raise NotImplementedError

    def get_feed_post(self, post_id: str):
        raise NotImplementedError

    def list_feed_posts(self, venue_id: str, limit: int = 50) -> list:
        raise NotImplementedError

    def append_feed_comment(self, post_id: str, comment: dict):
        """Atomically append ``comment`` to the post; returns the updated post
        (or None if the post does not exist)."""
        raise NotImplementedError

    def toggle_feed_reaction(self, post_id: str, emoji: str, user_id: str):
        """Atomically add/remove ``user_id`` under ``emoji``; returns
        ``(updated_post, "added"|"removed")`` or ``(None, None)`` if missing."""
        raise NotImplementedError

    # --- Inventory: stocktakes + supplier orders ---------------------------

    def save_stocktake(self, st: dict) -> None:
        raise NotImplementedError

    def get_stocktake(self, st_id: str):
        raise NotImplementedError

    def list_stocktakes(self, venue_id: str) -> list:
        raise NotImplementedError

    def save_supplier_order(self, order: dict) -> None:
        raise NotImplementedError

    def get_supplier_order(self, order_id: str):
        raise NotImplementedError

    def list_supplier_orders(self, venue_id: str) -> list:
        raise NotImplementedError

    def transition_supplier_order(self, order_id: str, from_status: str,
                                  to_status: str, stamp_field: Optional[str] = None) -> bool:
        """Atomically move an order between statuses. Returns False if the
        order wasn't in from_status (someone else won the race)."""
        raise NotImplementedError

    def increment_ingredient_stock(self, ingredient_id: str, delta: float) -> None:
        """Atomic stock adjustment (never read-add-write in route code)."""
        raise NotImplementedError

    def update_stocktake_count(self, st_id: str, ingredient_id: str, counted: float) -> bool:
        """Atomically set one item's count on an OPEN stocktake. Returns False
        if the stocktake isn't open or the item isn't in it."""
        raise NotImplementedError

    # --- Dish sales (sales -> stock depletion -> live food cost) -----------

    def save_dish_sale(self, sale: dict) -> None:
        raise NotImplementedError

    def list_dish_sales(self, venue_id: str, start_date, end_date) -> list:
        raise NotImplementedError

    # --- Supplier invoices (receive against actuals + price updates) -------

    def save_supplier_invoice(self, inv: dict) -> None:
        raise NotImplementedError

    def list_supplier_invoices(self, venue_id: str) -> list:
        raise NotImplementedError

    # --- Xero bill push ledger (one push per invoice; PK == invoice id) ----

    def save_xero_bill_push(self, rec: dict) -> None:
        """Record that an invoice was pushed to Xero. First write wins:
        a second save for the same invoice id is a silent no-op."""
        raise NotImplementedError

    def get_xero_bill_push(self, invoice_id: str):
        raise NotImplementedError

    def list_xero_bill_pushes(self, venue_id: str) -> list:
        raise NotImplementedError

    # --- MYOB bill push ledger (one push per invoice; PK == invoice id) ----

    def save_myob_bill_push(self, rec: dict) -> None:
        """Record that an invoice was pushed to MYOB. First write wins:
        a second save for the same invoice id is a silent no-op."""
        raise NotImplementedError

    def get_myob_bill_push(self, invoice_id: str):
        raise NotImplementedError

    def list_myob_bill_pushes(self, venue_id: str) -> list:
        raise NotImplementedError

    # --- Wastage log (record spoiled/dropped stock) ------------------------

    def save_waste_entry(self, entry: dict) -> None:
        raise NotImplementedError

    def list_waste_entries(self, venue_id: str, start_date, end_date) -> list:
        raise NotImplementedError

    # --- POS item mapping + import dedup -----------------------------------

    def save_pos_item_map(self, m: dict) -> None:
        raise NotImplementedError

    def list_pos_item_maps(self, venue_id: str) -> list:
        raise NotImplementedError

    def delete_pos_item_map(self, venue_id: str, normalized_name: str) -> None:
        raise NotImplementedError

    def save_import_batch(self, batch: dict) -> None:
        raise NotImplementedError

    def get_import_batch(self, batch_id: str):
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

    def list_events(self, venue_id=None, category=None, action_prefix=None,
                    since=None, limit: int = 100, offset: int = 0) -> list[dict]:
        """Event log query. venue_id=None means platform-wide (owner view);
        category filters details.category ('audit'|'security'|'error'|'perf'|
        'integration'|'ai'|'job'); action_prefix matches action LIKE 'prefix%';
        since is a datetime."""
        raise NotImplementedError

    def event_rollup(self, since, venue_id=None, category=None, group_by="action",
                     limit: int = 20) -> list[dict]:
        """Aggregate the event log — the query behind every health/insight view.

        group_by is "action" or a key inside details ("fingerprint", "route",
        "provider", "job", "outcome"). Returns rows of
        {key, count, failures, last_seen, sample} ordered by count desc.
        Aggregating in the STORE matters: the alternative is pulling tens of
        thousands of rows into python on every dashboard load.
        """
        raise NotImplementedError

    def ping(self) -> bool:
        """Cheapest possible "is the database answering?" — for readiness
        probes, which run every few seconds and must not do real work."""
        raise NotImplementedError

    def prune_events(self, before) -> int:
        """Delete events older than ``before``; returns the number removed.
        Traffic-driven categories (perf/integration/ai) are what make the table
        grow, so retention is what keeps the log affordable."""
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

    def save_direct_bookings(self, venue_id: str, bookings: list[dict]) -> int:
        """
        Persist directly-ingested bookings for a venue (CSV/webhook import for any
        booking system without a native adapter). Each booking: {date (ISO),
        party_size, time (optional HH:MM)}. Returns the number stored.
        """
        raise NotImplementedError

    def get_direct_bookings(
        self, venue_id: str, start: str, end: str
    ) -> list[dict]:
        """Get directly-ingested bookings for a venue within a date range (ISO)."""
        raise NotImplementedError

    def count_direct_bookings(self, venue_id: str) -> int:
        """Count directly-ingested bookings stored for a venue."""
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

    # --- Roster publishing state machine ---

    def get_roster_state(self, roster_id: str) -> str:
        """Current publish state of a roster ('draft' if never transitioned)."""
        raise NotImplementedError

    def update_roster_state(self, roster_id: str, new_state: str, reason: str,
                            actor_id: str = "system") -> None:
        """Set a roster's publish state and append to its state history."""
        raise NotImplementedError

    def get_roster_state_history(self, roster_id: str) -> list[dict]:
        """The roster's state-transition history (oldest first)."""
        raise NotImplementedError

    def save_publication_event(self, event: dict) -> None:
        """Record a roster publication event."""
        raise NotImplementedError

    def get_publication_history(self, venue_id: str, limit: int = 50) -> list[dict]:
        """Recent publication events for a venue (newest first)."""
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

    def venue_id_for_shift(self, shift_id: str) -> Optional[str]:
        """The venue a shift belongs to (via its roster), or None. Shift
        objects carry no venue_id, so tenancy checks on a shift id must go
        through this. Default: walk rosters (stores override with a query)."""
        try:
            for roster in self.list_rosters() or []:
                for sh in getattr(roster, "shifts", None) or []:
                    if getattr(sh, "id", None) == shift_id:
                        return getattr(roster, "venue_id", None)
        except Exception:
            return None
        return None

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
        self._webhook_deliveries_by_id: dict[str, dict] = {}  # Key: delivery_id (all statuses)
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
        self._roster_states: dict[str, str] = {}  # roster_id -> current publish state
        self._roster_state_history: dict[str, list[dict]] = {}  # roster_id -> transitions
        self._publication_events: list[dict] = []  # roster publication events
        self._push_subscriptions: dict[str, dict] = {}  # Key: user_id
        self._timesheets: dict[str, dict] = {}  # Key: timesheet id
        self._timeclock_pins: dict[str, str] = {}  # Key: f"{venue_id}:{employee_id}" -> pin hash
        self._checklist_templates: dict[str, dict] = {}
        self._checklist_runs: dict[str, dict] = {}
        self._ingredients: dict[str, dict] = {}
        self._recipes: dict[str, dict] = {}
        self._leave_requests: dict[str, dict] = {}
        self._shift_covers: dict[str, dict] = {}
        self._announcements: dict[str, dict] = {}
        self._sop_documents: dict[str, dict] = {}  # Key: doc id (SOP/JSP library)
        self._sop_acks: dict[str, dict] = {}  # Key: ack id; unique per (doc, version, employee)
        self._feed_posts: dict[str, dict] = {}  # Key: post id (team feed)
        self._stocktakes: dict[str, dict] = {}
        self._supplier_orders: dict[str, dict] = {}
        self._dish_sales: dict[str, dict] = {}
        self._supplier_invoices: dict[str, dict] = {}
        self._xero_bill_pushes: dict[str, dict] = {}  # Key: invoice id (one push per invoice)
        self._myob_bill_pushes: dict[str, dict] = {}  # Key: invoice id (one push per invoice)
        self._waste_log: dict[str, dict] = {}
        self._pos_item_maps: dict[str, dict] = {}
        self._import_batches: dict[str, dict] = {}
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
        self._direct_bookings: list[dict] = []  # List of {venue_id, date, party_size, time}

    # Public aliases for the backing collections. These let tests and tooling
    # seed/inspect in-memory data directly (e.g. store.venues[id] = venue)
    # without reaching into private attributes. Each returns the live dict, so
    # item assignment works.
    @property
    def venues(self) -> "dict[str, VenueConfig]":
        return self._venues

    @property
    def employees(self) -> "dict[str, Employee]":
        return self._employees

    @property
    def rosters(self) -> "dict[str, Roster]":
        return self._rosters

    @property
    def shifts(self) -> "dict[str, Shift]":
        return self._shifts

    @property
    def forecasts(self) -> "list[DemandForecast]":
        return self._forecasts

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

    # --- Native time clock ---

    def save_timesheet(self, ts):
        self._timesheets[ts["id"]] = dict(ts)

    def get_timesheet(self, ts_id):
        return self._timesheets.get(ts_id)

    def get_open_timesheet(self, venue_id, employee_id):
        for t in self._timesheets.values():
            if (t.get("venue_id") == venue_id and t.get("employee_id") == employee_id
                    and not t.get("clock_out")):
                return t
        return None

    def get_timesheets(self, venue_id, start_date, end_date):
        out = []
        for t in self._timesheets.values():
            if t.get("venue_id") != venue_id:
                continue
            d = t.get("work_date")
            if d and start_date <= d <= end_date:
                out.append(t)
        return sorted(out, key=lambda t: (str(t.get("work_date")), str(t.get("clock_in"))))

    def set_timeclock_pin(self, venue_id, employee_id, pin_hash):
        self._timeclock_pins[f"{venue_id}:{employee_id}"] = pin_hash

    def get_timeclock_pin(self, venue_id, employee_id):
        return self._timeclock_pins.get(f"{venue_id}:{employee_id}")

    # --- Compliance checklists ---

    def save_checklist_template(self, tpl):
        self._checklist_templates[tpl["id"]] = dict(tpl)

    def get_checklist_template(self, tpl_id):
        return self._checklist_templates.get(tpl_id)

    def list_checklist_templates(self, venue_id):
        return sorted(
            [t for t in self._checklist_templates.values() if t.get("venue_id") == venue_id],
            key=lambda t: t.get("name", ""),
        )

    def save_checklist_run(self, run):
        self._checklist_runs[run["id"]] = dict(run)

    def get_checklist_run(self, run_id):
        return self._checklist_runs.get(run_id)

    def list_checklist_runs(self, venue_id, start_date, end_date):
        out = []
        for r in self._checklist_runs.values():
            if r.get("venue_id") != venue_id:
                continue
            d = r.get("run_date")
            if d and start_date <= d <= end_date:
                out.append(r)
        return sorted(out, key=lambda r: (str(r.get("run_date")), str(r.get("started_at"))))

    # --- Menu costing ---

    def save_ingredient(self, ing):
        self._ingredients[ing["id"]] = dict(ing)

    def get_ingredient(self, ing_id):
        return self._ingredients.get(ing_id)

    def list_ingredients(self, venue_id):
        return sorted(
            [i for i in self._ingredients.values() if i.get("venue_id") == venue_id],
            key=lambda i: i.get("name", ""),
        )

    def save_recipe(self, recipe):
        self._recipes[recipe["id"]] = dict(recipe)

    def get_recipe(self, recipe_id):
        return self._recipes.get(recipe_id)

    def list_recipes(self, venue_id):
        return sorted(
            [r for r in self._recipes.values() if r.get("venue_id") == venue_id],
            key=lambda r: r.get("name", ""),
        )

    # --- Leave requests ---

    def save_leave_request(self, req):
        self._leave_requests[req["id"]] = dict(req)

    def get_leave_request(self, req_id):
        return self._leave_requests.get(req_id)

    def list_leave_requests(self, venue_id):
        return sorted(
            [r for r in self._leave_requests.values() if r.get("venue_id") == venue_id],
            key=lambda r: str(r.get("created_at")), reverse=True,
        )

    # --- Shift covers ---

    def save_shift_cover(self, cover):
        self._shift_covers[cover["id"]] = dict(cover)

    def get_shift_cover(self, cover_id):
        return self._shift_covers.get(cover_id)

    def list_shift_covers(self, venue_id):
        return sorted(
            [c for c in self._shift_covers.values() if c.get("venue_id") == venue_id],
            key=lambda c: str(c.get("created_at")), reverse=True,
        )

    # --- Announcements ---

    def save_announcement(self, ann):
        self._announcements[ann["id"]] = dict(ann)

    def get_announcement(self, ann_id):
        return self._announcements.get(ann_id)

    def list_announcements(self, venue_id):
        return sorted(
            [a for a in self._announcements.values() if a.get("venue_id") == venue_id],
            key=lambda a: (not a.get("pinned"), str(a.get("created_at"))),
        )

    # --- SOP / JSP document library ---

    def save_sop_document(self, doc):
        self._sop_documents[doc["id"]] = dict(doc)

    def get_sop_document(self, doc_id):
        return self._sop_documents.get(doc_id)

    def delete_sop_document(self, doc_id):
        self._sop_documents.pop(doc_id, None)

    def list_sop_documents(self, venue_id, include_inactive=False):
        rows = [d for d in self._sop_documents.values() if d.get("venue_id") == venue_id]
        if not include_inactive:
            rows = [d for d in rows if d.get("active", True)]
        return sorted(rows, key=lambda d: (str(d.get("created_at")), str(d.get("title"))))

    def save_sop_ack(self, ack):
        # Mirrors the PG UNIQUE (doc_id, doc_version, employee_id) ... ON
        # CONFLICT DO NOTHING: the first acknowledgement per key wins.
        key = (ack["doc_id"], int(ack["doc_version"]), ack["employee_id"])
        for existing in self._sop_acks.values():
            if (existing.get("doc_id"), int(existing.get("doc_version") or 0),
                    existing.get("employee_id")) == key:
                return
        self._sop_acks[ack["id"]] = dict(ack)

    def list_sop_acks(self, venue_id, doc_id=None):
        rows = [a for a in self._sop_acks.values() if a.get("venue_id") == venue_id]
        if doc_id is not None:
            rows = [a for a in rows if a.get("doc_id") == doc_id]
        return sorted(rows, key=lambda a: str(a.get("acknowledged_at")))

    # --- Team feed ---

    def save_feed_post(self, post):
        self._feed_posts[post["id"]] = dict(post)

    def get_feed_post(self, post_id):
        return self._feed_posts.get(post_id)

    def list_feed_posts(self, venue_id, limit=50):
        # Iterate newest-inserted first so a stable sort keeps insertion order
        # as the tiebreak when two posts share a created_at.
        rows = [
            p for p in reversed(list(self._feed_posts.values()))
            if p.get("venue_id") == venue_id and not p.get("removed")
        ]
        rows.sort(key=lambda p: (bool(p.get("pinned")), str(p.get("created_at"))),
                  reverse=True)
        return rows[: max(int(limit or 50), 0)]

    def append_feed_comment(self, post_id, comment):
        post = self._feed_posts.get(post_id)
        if not post:
            return None
        comments = post.get("comments")
        if not isinstance(comments, list):
            comments = []
        # Append in place on the stored row (no whole-blob rewrite).
        comments.append(dict(comment))
        post["comments"] = comments
        post["updated_at"] = datetime.utcnow()
        return post

    def toggle_feed_reaction(self, post_id, emoji, user_id):
        post = self._feed_posts.get(post_id)
        if not post:
            return None, None
        reactions = post.get("reactions")
        if not isinstance(reactions, dict):
            reactions = {}
        ids = list(reactions.get(emoji) or [])
        if user_id in ids:
            ids = [i for i in ids if i != user_id]
            state = "removed"
        else:
            ids.append(user_id)
            state = "added"
        if ids:
            reactions[emoji] = ids
        else:
            reactions.pop(emoji, None)
        post["reactions"] = reactions
        post["updated_at"] = datetime.utcnow()
        return post, state

    # --- Inventory ---

    def save_stocktake(self, st):
        self._stocktakes[st["id"]] = dict(st)

    def get_stocktake(self, st_id):
        return self._stocktakes.get(st_id)

    def list_stocktakes(self, venue_id):
        return sorted(
            [s for s in self._stocktakes.values() if s.get("venue_id") == venue_id],
            key=lambda s: str(s.get("started_at")), reverse=True,
        )

    def save_supplier_order(self, order):
        self._supplier_orders[order["id"]] = dict(order)

    def get_supplier_order(self, order_id):
        return self._supplier_orders.get(order_id)

    def list_supplier_orders(self, venue_id):
        return sorted(
            [o for o in self._supplier_orders.values() if o.get("venue_id") == venue_id],
            key=lambda o: str(o.get("created_at")), reverse=True,
        )

    def transition_supplier_order(self, order_id, from_status, to_status, stamp_field=None):
        order = self._supplier_orders.get(order_id)
        if not order or order.get("status") != from_status:
            return False
        order["status"] = to_status
        if stamp_field:
            order[stamp_field] = datetime.utcnow()
        return True

    def increment_ingredient_stock(self, ingredient_id, delta):
        ing = self._ingredients.get(ingredient_id)
        if ing is not None:
            ing["stock_qty"] = float(ing.get("stock_qty") or 0) + float(delta)

    def update_stocktake_count(self, st_id, ingredient_id, counted):
        st = self._stocktakes.get(st_id)
        if not st or st.get("status") != "open":
            return False
        for item in st.get("items", []):
            if item.get("ingredient_id") == ingredient_id:
                item["counted"] = float(counted)
                return True
        return False

    # --- Supplier invoices ---

    def save_supplier_invoice(self, inv):
        self._supplier_invoices[inv["id"]] = dict(inv)

    def list_supplier_invoices(self, venue_id):
        return sorted(
            [i for i in self._supplier_invoices.values() if i.get("venue_id") == venue_id],
            key=lambda i: str(i.get("created_at")), reverse=True,
        )

    # --- Xero bill push ledger ---

    def save_xero_bill_push(self, rec):
        # First write wins, mirroring PG's ON CONFLICT (id) DO NOTHING.
        if rec["id"] not in self._xero_bill_pushes:
            self._xero_bill_pushes[rec["id"]] = dict(rec)

    def get_xero_bill_push(self, invoice_id):
        rec = self._xero_bill_pushes.get(invoice_id)
        return dict(rec) if rec else None

    def list_xero_bill_pushes(self, venue_id):
        return sorted(
            [dict(p) for p in self._xero_bill_pushes.values() if p.get("venue_id") == venue_id],
            key=lambda p: str(p.get("pushed_at")), reverse=True,
        )

    # --- MYOB bill push ledger ---

    def save_myob_bill_push(self, rec):
        # First write wins, mirroring PG's ON CONFLICT (id) DO NOTHING.
        if rec["id"] not in self._myob_bill_pushes:
            self._myob_bill_pushes[rec["id"]] = dict(rec)

    def get_myob_bill_push(self, invoice_id):
        rec = self._myob_bill_pushes.get(invoice_id)
        return dict(rec) if rec else None

    def list_myob_bill_pushes(self, venue_id):
        return sorted(
            [dict(p) for p in self._myob_bill_pushes.values() if p.get("venue_id") == venue_id],
            key=lambda p: str(p.get("pushed_at")), reverse=True,
        )

    # --- Wastage log ---

    def save_waste_entry(self, entry):
        self._waste_log[entry["id"]] = dict(entry)

    def list_waste_entries(self, venue_id, start_date, end_date):
        out = []
        for w in self._waste_log.values():
            if w.get("venue_id") != venue_id:
                continue
            d = w.get("waste_date")
            d = d if isinstance(d, date) else date.fromisoformat(str(d)[:10])
            if start_date <= d <= end_date:
                out.append(w)
        return sorted(out, key=lambda w: str(w.get("created_at")), reverse=True)

    # --- Dish sales ---

    def save_dish_sale(self, sale):
        self._dish_sales[sale["id"]] = dict(sale)

    def list_dish_sales(self, venue_id, start_date, end_date):
        out = []
        for s in self._dish_sales.values():
            if s.get("venue_id") != venue_id:
                continue
            d = s.get("sale_date")
            d = d if isinstance(d, date) else date.fromisoformat(str(d)[:10])
            if start_date <= d <= end_date:
                out.append(s)
        return sorted(out, key=lambda s: (str(s.get("sale_date")), s.get("recipe_name") or ""))

    # --- POS item maps + import batches ---

    def save_pos_item_map(self, m):
        self._pos_item_maps[f"{m['venue_id']}:{m['normalized_name']}"] = dict(m)

    def list_pos_item_maps(self, venue_id):
        return sorted(
            [m for m in self._pos_item_maps.values() if m.get("venue_id") == venue_id],
            key=lambda m: m.get("normalized_name", ""),
        )

    def delete_pos_item_map(self, venue_id, normalized_name):
        self._pos_item_maps.pop(f"{venue_id}:{normalized_name}", None)

    def save_import_batch(self, batch):
        self._import_batches[batch["id"]] = dict(batch)

    def get_import_batch(self, batch_id):
        return self._import_batches.get(batch_id)

    def get_employees(self, venue_id=None):
        """Employees, optionally filtered to one venue. Used by the AI agent's
        data tools (which pass a venue_id)."""
        emps = list(self._employees.values())
        if venue_id is not None:
            emps = [e for e in emps if getattr(e, "venue_id", None) == venue_id]
        return emps

    def get_employee(self, employee_id):
        return self._employees.get(employee_id)

    def add_forecasts(self, forecasts):
        # Upsert by (venue, date, hour, model_version) to match PostgresStore's
        # ON CONFLICT — re-seeding a day replaces its rows rather than stacking
        # duplicates (the store-divergence class that has bitten this repo).
        def key(f):
            return (f.venue_id, f.date, f.hour, getattr(f, "model_version", ""))
        incoming = {key(f): f for f in forecasts}
        self._forecasts = [f for f in self._forecasts if key(f) not in incoming]
        self._forecasts.extend(incoming.values())

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

        # Index every delivery by id so it can always be retrieved by
        # get_webhook_delivery, regardless of status or subscription_id.
        if delivery_id:
            self._webhook_deliveries_by_id[delivery_id] = delivery

        # Save to retry queue for queue management
        if delivery_id and delivery.get("status") == "pending":
            self._webhook_retry_queue[delivery_id] = delivery
        elif delivery_id:
            # No longer pending (e.g. success/dead_letter) — drop from retry queue.
            self._webhook_retry_queue.pop(delivery_id, None)

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
        if not hasattr(self, '_webhook_deliveries_by_id'):
            self._webhook_deliveries_by_id = {}
        if not hasattr(self, '_dead_letters'):
            self._dead_letters = {}

        # Check retry queue first
        if delivery_id in self._webhook_retry_queue:
            return self._webhook_retry_queue[delivery_id]

        # Check dead letters
        if delivery_id in self._dead_letters:
            return self._dead_letters[delivery_id]

        # Fall back to the by-id index (covers non-pending deliveries with no
        # subscription_id, e.g. ones recorded only via record_attempt).
        if delivery_id in self._webhook_deliveries_by_id:
            return self._webhook_deliveries_by_id[delivery_id]

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

    def list_events(self, venue_id=None, category=None, action_prefix=None,
                    since=None, limit=100, offset=0):
        rows = list(self._audit_logs)
        if venue_id is not None:
            rows = [r for r in rows if r.get("venue_id") == venue_id]
        if category:
            rows = [r for r in rows if (r.get("details") or {}).get("category") == category]
        if action_prefix:
            rows = [r for r in rows if str(r.get("action") or "").startswith(action_prefix)]
        if since is not None:
            rows = [r for r in rows if r.get("created_at") and r["created_at"] >= since]
        rows.sort(key=lambda r: str(r.get("created_at")), reverse=True)
        return [dict(r) for r in rows[offset: offset + limit]]

    def event_rollup(self, since, venue_id=None, category=None, group_by="action",
                     limit=20):
        rows = self.list_events(venue_id=venue_id, category=category, since=since,
                                limit=100000)
        buckets: dict = {}
        for r in rows:
            d = r.get("details") or {}
            key = r.get("action") if group_by == "action" else d.get(group_by)
            if key is None:
                continue
            key = str(key)
            b = buckets.setdefault(key, {"key": key, "count": 0, "failures": 0,
                                         "last_seen": None, "sample": None,
                                         "_durations": []})
            b["count"] += 1
            if str(d.get("outcome")) in ("failed", "error", "denied"):
                b["failures"] += 1
            if d.get("duration_ms") is not None:
                try:
                    b["_durations"].append(float(d["duration_ms"]))
                except Exception:
                    pass
            ts = r.get("created_at")
            if b["last_seen"] is None or (ts and str(ts) > str(b["last_seen"])):
                b["last_seen"] = ts
                b["sample"] = {k: v for k, v in d.items() if k not in ("category",)}
        out = []
        for b in buckets.values():
            durations = sorted(b.pop("_durations"))
            if durations:
                b["p95_ms"] = round(durations[min(len(durations) - 1,
                                                  int(len(durations) * 0.95))], 1)
                b["max_ms"] = round(durations[-1], 1)
            out.append(b)
        out.sort(key=lambda b: b["count"], reverse=True)
        return out[:limit]

    def ping(self):
        return True

    def prune_events(self, before):
        keep = [r for r in self._audit_logs
                if not (r.get("created_at") and r["created_at"] < before)]
        removed = len(self._audit_logs) - len(keep)
        self._audit_logs[:] = keep          # in place: other refs stay valid
        return removed

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

    # --- Roster publishing state machine ---

    def get_roster_state(self, roster_id: str) -> str:
        # A freshly generated roster has never transitioned -> publishable DRAFT.
        return self._roster_states.get(roster_id, "draft")

    def update_roster_state(self, roster_id: str, new_state: str, reason: str,
                            actor_id: str = "system") -> None:
        self._roster_states[roster_id] = new_state
        self._roster_state_history.setdefault(roster_id, []).append({
            "roster_id": roster_id,
            "state": new_state,
            "reason": reason,
            "actor_id": actor_id,
            "at": datetime.utcnow().isoformat(),
        })

    def get_roster_state_history(self, roster_id: str) -> list[dict]:
        return list(self._roster_state_history.get(roster_id, []))

    def save_publication_event(self, event: dict) -> None:
        self._publication_events.append(event)

    def get_publication_history(self, venue_id: str, limit: int = 50) -> list[dict]:
        events = [e for e in self._publication_events if e.get("venue_id") == venue_id]
        return list(reversed(events))[:limit]

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

    def save_direct_bookings(self, venue_id: str, bookings: list[dict]) -> int:
        # Idempotent: skip exact-duplicate rows so re-uploading the same bookings CSV
        # doesn't double-count (which would inflate the demand signal + booking count).
        existing = {
            (b.get("venue_id"), str(b.get("date")), b.get("time"), b.get("party_size"))
            for b in self._direct_bookings
        }
        stored = 0
        for b in bookings or []:
            d = b.get("date")
            if not d:
                continue
            rec = {
                "venue_id": venue_id,
                "date": d,
                "party_size": b.get("party_size") or b.get("covers") or 0,
                "time": b.get("time"),
            }
            key = (venue_id, str(d), rec["time"], rec["party_size"])
            if key in existing:
                continue
            self._direct_bookings.append(rec)
            existing.add(key)
            stored += 1
        return stored

    def get_direct_bookings(
        self, venue_id: str, start: str, end: str
    ) -> list[dict]:
        try:
            start_date = datetime.fromisoformat(start).date()
            end_date = datetime.fromisoformat(end).date()
        except (ValueError, AttributeError):
            return []
        results = []
        for b in self._direct_bookings:
            if b.get("venue_id") != venue_id:
                continue
            try:
                bd = datetime.fromisoformat(str(b.get("date"))).date()
            except (ValueError, AttributeError):
                continue
            if start_date <= bd <= end_date:
                results.append(b)
        return results

    def count_direct_bookings(self, venue_id: str) -> int:
        return sum(1 for b in self._direct_bookings if b.get("venue_id") == venue_id)


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
                # Self-heal the core schema. Migration 001 creates these tables,
                # but its first statement (CREATE EXTENSION "uuid-ossp") fails on
                # managed Postgres where the app's role lacks superuser, which
                # rolls back the whole transaction and leaves NO core tables — so
                # every login 500s. These idempotent CREATE TABLE IF NOT EXISTS
                # statements (no privileged extension) guarantee the tables exist
                # regardless of migration state. No-op when 001 already applied.
                self._ensure_core_schema()
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

    # ------------------------------------------------------------------
    # Runtime schema guards
    #
    # A number of feature tables are NOT created by any SQL migration
    # (001/003/004). On a fresh Postgres deploy the first venue to hit the
    # relevant feature (password reset, email verify, audit logging, shift
    # swaps/open shifts, templates, theming, payroll batches/exports,
    # analytics/audit snapshots, approvals, A/B testing, privacy/consent,
    # etc.) would otherwise raise psycopg2 UndefinedTable -> HTTP 500.
    #
    # Mirroring the existing revenue_actuals / direct_bookings pattern, each
    # such table is created lazily via CREATE TABLE IF NOT EXISTS at the start
    # of the methods that touch it. The DDL lives here once so the columns,
    # JSONB fields and ON CONFLICT key constraints stay in lockstep with the
    # INSERT/SELECT/UPDATE statements below.
    # ------------------------------------------------------------------
    _TABLE_DDL = {
        "roster_states": """
            CREATE TABLE IF NOT EXISTS roster_states (
                roster_id TEXT PRIMARY KEY,
                state TEXT NOT NULL,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """,
        "roster_state_history": """
            CREATE TABLE IF NOT EXISTS roster_state_history (
                id SERIAL PRIMARY KEY,
                roster_id TEXT,
                state TEXT,
                reason TEXT,
                actor_id TEXT,
                at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """,
        "publication_events": """
            CREATE TABLE IF NOT EXISTS publication_events (
                id SERIAL PRIMARY KEY,
                venue_id TEXT,
                event JSONB,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """,
        "roster_templates": """
            CREATE TABLE IF NOT EXISTS roster_templates (
                id TEXT PRIMARY KEY,
                name TEXT,
                venue_id TEXT,
                description TEXT,
                created_by TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP,
                shift_patterns JSONB
            )
        """,
        "password_reset_tokens": """
            CREATE TABLE IF NOT EXISTS password_reset_tokens (
                token_hash TEXT PRIMARY KEY,
                user_id TEXT,
                expires_at TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """,
        "email_verification_tokens": """
            CREATE TABLE IF NOT EXISTS email_verification_tokens (
                token_hash TEXT PRIMARY KEY,
                user_id TEXT,
                expires_at TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """,
        "webhook_subscriptions": """
            CREATE TABLE IF NOT EXISTS webhook_subscriptions (
                id TEXT PRIMARY KEY,
                venue_id TEXT,
                callback_url TEXT,
                events JSONB,
                secret TEXT,
                active BOOLEAN DEFAULT true,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP
            )
        """,
        "shift_swaps": """
            CREATE TABLE IF NOT EXISTS shift_swaps (
                id TEXT PRIMARY KEY,
                shift_id TEXT,
                offered_by TEXT,
                requested_by TEXT,
                my_shift_id TEXT,
                offered_shift_id TEXT,
                date TEXT,
                start_time TEXT,
                end_time TEXT,
                role TEXT,
                venue TEXT,
                venue_id TEXT,
                status TEXT,
                message TEXT,
                created_at TEXT
            )
        """,
        "privacy_consents": """
            CREATE TABLE IF NOT EXISTS privacy_consents (
                id SERIAL PRIMARY KEY,
                user_id TEXT,
                consent_type TEXT,
                granted BOOLEAN,
                timestamp TIMESTAMP
            )
        """,
        "privacy_audit_log": """
            CREATE TABLE IF NOT EXISTS privacy_audit_log (
                id SERIAL PRIMARY KEY,
                user_id TEXT,
                action TEXT,
                resource_type TEXT,
                details JSONB,
                logged_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """,
        "anonymised_employees": """
            CREATE TABLE IF NOT EXISTS anonymised_employees (
                employee_id TEXT PRIMARY KEY,
                anonymised_at TIMESTAMP
            )
        """,
        "revenue_snapshots": """
            CREATE TABLE IF NOT EXISTS revenue_snapshots (
                id SERIAL PRIMARY KEY,
                venue_id TEXT,
                date DATE,
                revenue NUMERIC,
                UNIQUE(venue_id, date)
            )
        """,
        "analytics_snapshots": """
            CREATE TABLE IF NOT EXISTS analytics_snapshots (
                id SERIAL PRIMARY KEY,
                venue_id TEXT,
                date DATE,
                metric_type TEXT,
                value JSONB,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """,
        "audit_logs": """
            CREATE TABLE IF NOT EXISTS audit_logs (
                id SERIAL PRIMARY KEY,
                venue_id TEXT,
                user_id TEXT,
                action TEXT,
                resource_type TEXT,
                resource_id TEXT,
                details JSONB,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """,
        "themes": """
            CREATE TABLE IF NOT EXISTS themes (
                venue_id TEXT PRIMARY KEY,
                config JSONB,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """,
        "api_key_records": """
            CREATE TABLE IF NOT EXISTS api_key_records (
                id TEXT PRIMARY KEY,
                user_id TEXT,
                name TEXT,
                key_hash TEXT,
                is_active BOOLEAN,
                created_at TIMESTAMP,
                expires_at TIMESTAMP,
                last_used_at TIMESTAMP,
                usage_count INTEGER DEFAULT 0,
                revoked_at TIMESTAMP,
                suspicious_flags INTEGER DEFAULT 0
            )
        """,
        "webhook_secrets": """
            CREATE TABLE IF NOT EXISTS webhook_secrets (
                id TEXT PRIMARY KEY,
                venue_id TEXT,
                secret_hash TEXT,
                is_active BOOLEAN,
                grace_expires_at TIMESTAMP,
                created_at TIMESTAMP,
                rotated_at TIMESTAMP
            )
        """,
        "preference_profiles": """
            CREATE TABLE IF NOT EXISTS preference_profiles (
                employee_id TEXT PRIMARY KEY,
                profile_data JSONB,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """,
        "ab_experiments": """
            CREATE TABLE IF NOT EXISTS ab_experiments (
                id TEXT PRIMARY KEY,
                name TEXT,
                description TEXT,
                control_strategy TEXT,
                variant_strategy TEXT,
                start_date TIMESTAMP,
                end_date TIMESTAMP,
                status TEXT,
                created_at TIMESTAMP,
                updated_at TIMESTAMP,
                control_venues JSONB,
                variant_venues JSONB,
                minimum_sample_size INTEGER DEFAULT 30
            )
        """,
        "ab_experiment_outcomes": """
            CREATE TABLE IF NOT EXISTS ab_experiment_outcomes (
                id TEXT PRIMARY KEY,
                experiment_id TEXT,
                venue_id TEXT,
                roster_id TEXT,
                "group" TEXT,
                total_labour_cost NUMERIC,
                labour_percentage NUMERIC,
                demand_coverage_pct NUMERIC,
                compliance_score NUMERIC,
                staff_satisfaction_proxy NUMERIC,
                overtime_hours NUMERIC,
                penalty_hours NUMERIC,
                recorded_at TIMESTAMP
            )
        """,
        "payroll_batches": """
            CREATE TABLE IF NOT EXISTS payroll_batches (
                batch_id TEXT PRIMARY KEY,
                venue_id TEXT,
                period_start TEXT,
                period_end TEXT,
                status TEXT,
                data JSONB,
                created_at TEXT
            )
        """,
        "payroll_exports": """
            CREATE TABLE IF NOT EXISTS payroll_exports (
                id SERIAL PRIMARY KEY,
                batch_id TEXT,
                service TEXT,
                status TEXT,
                data JSONB,
                exported_at TEXT
            )
        """,
        "approval_requests": """
            CREATE TABLE IF NOT EXISTS approval_requests (
                request_id TEXT PRIMARY KEY,
                roster_id TEXT,
                venue_id TEXT,
                submitted_by TEXT,
                submitted_at TIMESTAMP,
                status TEXT,
                reviewed_by TEXT,
                reviewed_at TIMESTAMP,
                review_notes TEXT,
                revision_number INTEGER DEFAULT 1,
                escalated_at TIMESTAMP,
                escalated_to TEXT,
                tier TEXT,
                auto_approved_by_rules JSONB,
                failed_rules JSONB,
                data JSONB,
                created_at TIMESTAMP
            )
        """,
        "roster_revisions": """
            CREATE TABLE IF NOT EXISTS roster_revisions (
                revision_id TEXT PRIMARY KEY,
                roster_id TEXT,
                revision_number INTEGER,
                changes JSONB,
                created_at TIMESTAMP,
                data JSONB
            )
        """,
        "open_shifts": """
            CREATE TABLE IF NOT EXISTS open_shifts (
                id TEXT PRIMARY KEY,
                venue_id TEXT,
                date DATE,
                start_time TEXT,
                end_time TEXT,
                role_required TEXT,
                skills_required JSONB,
                min_rate NUMERIC,
                max_rate NUMERIC,
                posted_by TEXT,
                posted_at TIMESTAMP,
                deadline TIMESTAMP,
                status TEXT,
                notes TEXT,
                created_at TIMESTAMP
            )
        """,
        "bids": """
            CREATE TABLE IF NOT EXISTS bids (
                id TEXT PRIMARY KEY,
                open_shift_id TEXT,
                employee_id TEXT,
                offered_rate NUMERIC,
                message TEXT,
                seniority_years NUMERIC DEFAULT 0,
                preference_score NUMERIC DEFAULT 0,
                submitted_at TIMESTAMP,
                status TEXT,
                created_at TIMESTAMP
            )
        """,
        # Notification tables use a blob model (user_id + a JSON payload), which
        # is what every caller and the MemoryStore use. Migration 003 defined a
        # different, normalised schema that NO code matches; these idempotent
        # definitions are the authoritative ones the runtime relies on.
        "notification_preferences": """
            CREATE TABLE IF NOT EXISTS notification_preferences (
                user_id TEXT PRIMARY KEY,
                preferences JSONB NOT NULL DEFAULT '{}',
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            )
        """,
        "push_subscriptions": """
            CREATE TABLE IF NOT EXISTS push_subscriptions (
                user_id TEXT PRIMARY KEY,
                venue_id TEXT,
                subscription_data JSONB NOT NULL DEFAULT '{}',
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            )
        """,
        "timesheets": """
            CREATE TABLE IF NOT EXISTS timesheets (
                id TEXT PRIMARY KEY,
                venue_id TEXT NOT NULL,
                employee_id TEXT NOT NULL,
                work_date DATE NOT NULL,
                clock_in TIMESTAMP WITH TIME ZONE NOT NULL,
                clock_out TIMESTAMP WITH TIME ZONE,
                break_minutes INTEGER DEFAULT 0,
                status TEXT NOT NULL DEFAULT 'open',
                pin_verified BOOLEAN DEFAULT false,
                rostered_shift_id TEXT,
                variance_minutes INTEGER,
                approved_by TEXT,
                approved_at TIMESTAMP WITH TIME ZONE,
                adjustment_note TEXT,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            )
        """,
        "timeclock_pins": """
            CREATE TABLE IF NOT EXISTS timeclock_pins (
                venue_id TEXT NOT NULL,
                employee_id TEXT NOT NULL,
                pin_hash TEXT NOT NULL,
                updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (venue_id, employee_id)
            )
        """,
        "checklist_templates": """
            CREATE TABLE IF NOT EXISTS checklist_templates (
                id TEXT PRIMARY KEY,
                venue_id TEXT NOT NULL,
                name TEXT NOT NULL,
                schedule TEXT NOT NULL DEFAULT 'daily',
                items JSONB NOT NULL DEFAULT '[]',
                active BOOLEAN DEFAULT true,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            )
        """,
        "checklist_runs": """
            CREATE TABLE IF NOT EXISTS checklist_runs (
                id TEXT PRIMARY KEY,
                venue_id TEXT NOT NULL,
                template_id TEXT NOT NULL,
                template_name TEXT,
                run_date DATE NOT NULL,
                started_at TIMESTAMP WITH TIME ZONE,
                completed_at TIMESTAMP WITH TIME ZONE,
                completed_by TEXT,
                status TEXT NOT NULL DEFAULT 'in_progress',
                items JSONB NOT NULL DEFAULT '[]',
                flags_count INTEGER DEFAULT 0,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            )
        """,
        "ingredients": """
            CREATE TABLE IF NOT EXISTS ingredients (
                id TEXT PRIMARY KEY,
                venue_id TEXT NOT NULL,
                name TEXT NOT NULL,
                unit TEXT NOT NULL DEFAULT 'each',
                purchase_size NUMERIC NOT NULL DEFAULT 1,
                purchase_cost NUMERIC NOT NULL DEFAULT 0,
                cost_per_unit NUMERIC NOT NULL DEFAULT 0,
                supplier TEXT,
                active BOOLEAN DEFAULT true,
                stock_qty NUMERIC DEFAULT 0,
                par_level NUMERIC DEFAULT 0,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            )
        """,
        "waste_log": """
            CREATE TABLE IF NOT EXISTS waste_log (
                id TEXT PRIMARY KEY,
                venue_id TEXT NOT NULL,
                ingredient_id TEXT NOT NULL,
                ingredient_name TEXT,
                waste_date DATE NOT NULL,
                qty NUMERIC NOT NULL DEFAULT 0,
                unit TEXT,
                reason TEXT,
                value NUMERIC NOT NULL DEFAULT 0,
                note TEXT,
                logged_by TEXT,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            )
        """,
        "stocktakes": """
            CREATE TABLE IF NOT EXISTS stocktakes (
                id TEXT PRIMARY KEY,
                venue_id TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'open',
                items JSONB NOT NULL DEFAULT '[]',
                started_by TEXT,
                started_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                completed_at TIMESTAMP WITH TIME ZONE,
                total_variance_value NUMERIC,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            )
        """,
        "supplier_invoices": """
            CREATE TABLE IF NOT EXISTS supplier_invoices (
                id TEXT PRIMARY KEY,
                venue_id TEXT NOT NULL,
                supplier TEXT,
                invoice_number TEXT NOT NULL,
                invoice_date DATE,
                order_id TEXT,
                items JSONB NOT NULL DEFAULT '[]',
                total NUMERIC DEFAULT 0,
                price_changes JSONB DEFAULT '[]',
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            )
        """,
        "xero_bill_pushes": """
            CREATE TABLE IF NOT EXISTS xero_bill_pushes (
                id TEXT PRIMARY KEY,
                venue_id TEXT NOT NULL,
                invoice_id TEXT NOT NULL,
                xero_invoice_id TEXT,
                xero_invoice_number TEXT,
                status TEXT DEFAULT 'pushed',
                pushed_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            )
        """,
        "myob_bill_pushes": """
            CREATE TABLE IF NOT EXISTS myob_bill_pushes (
                id TEXT PRIMARY KEY,
                venue_id TEXT NOT NULL,
                invoice_id TEXT NOT NULL,
                myob_bill_uid TEXT,
                myob_bill_number TEXT,
                status TEXT DEFAULT 'pushed',
                pushed_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            )
        """,
        "dish_sales": """
            CREATE TABLE IF NOT EXISTS dish_sales (
                id TEXT PRIMARY KEY,
                venue_id TEXT NOT NULL,
                sale_date DATE NOT NULL,
                recipe_id TEXT NOT NULL,
                recipe_name TEXT,
                qty NUMERIC NOT NULL DEFAULT 0,
                revenue_inc_gst NUMERIC NOT NULL DEFAULT 0,
                cogs NUMERIC NOT NULL DEFAULT 0,
                source TEXT DEFAULT 'manual',
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            )
        """,
        "pos_item_maps": """
            CREATE TABLE IF NOT EXISTS pos_item_maps (
                id TEXT PRIMARY KEY,
                venue_id TEXT NOT NULL,
                normalized_name TEXT NOT NULL,
                display_name TEXT,
                recipe_id TEXT NOT NULL,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            )
        """,
        "sales_import_batches": """
            CREATE TABLE IF NOT EXISTS sales_import_batches (
                id TEXT PRIMARY KEY,
                venue_id TEXT NOT NULL,
                sale_date DATE,
                row_count INTEGER,
                revenue NUMERIC,
                imported_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            )
        """,
        "supplier_orders": """
            CREATE TABLE IF NOT EXISTS supplier_orders (
                id TEXT PRIMARY KEY,
                venue_id TEXT NOT NULL,
                supplier TEXT,
                status TEXT NOT NULL DEFAULT 'draft',
                items JSONB NOT NULL DEFAULT '[]',
                total_cost NUMERIC DEFAULT 0,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                ordered_at TIMESTAMP WITH TIME ZONE,
                received_at TIMESTAMP WITH TIME ZONE,
                updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            )
        """,
        "leave_requests": """
            CREATE TABLE IF NOT EXISTS leave_requests (
                id TEXT PRIMARY KEY,
                venue_id TEXT NOT NULL,
                employee_id TEXT NOT NULL,
                start_date DATE NOT NULL,
                end_date DATE NOT NULL,
                reason TEXT,
                status TEXT NOT NULL DEFAULT 'pending',
                decided_by TEXT,
                decided_at TIMESTAMP WITH TIME ZONE,
                decision_note TEXT,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            )
        """,
        "shift_covers": """
            CREATE TABLE IF NOT EXISTS shift_covers (
                id TEXT PRIMARY KEY,
                venue_id TEXT NOT NULL,
                shift_id TEXT NOT NULL,
                shift_date DATE NOT NULL,
                shift_start TEXT,
                shift_end TEXT,
                role TEXT,
                requested_by TEXT NOT NULL,
                reason TEXT,
                claimed_by TEXT,
                status TEXT NOT NULL DEFAULT 'open',
                decided_by TEXT,
                decided_at TIMESTAMP WITH TIME ZONE,
                decision_note TEXT,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            )
        """,
        "announcements": """
            CREATE TABLE IF NOT EXISTS announcements (
                id TEXT PRIMARY KEY,
                venue_id TEXT NOT NULL,
                title TEXT NOT NULL,
                body TEXT NOT NULL,
                author_id TEXT,
                author_name TEXT,
                pinned BOOLEAN DEFAULT false,
                sms_result JSONB,
                read_by JSONB DEFAULT '[]'::jsonb,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            )
        """,
        "sop_documents": """
            CREATE TABLE IF NOT EXISTS sop_documents (
                id TEXT PRIMARY KEY,
                venue_id TEXT NOT NULL,
                title TEXT NOT NULL,
                category TEXT,
                body TEXT NOT NULL,
                applies_to JSONB DEFAULT '[]'::jsonb,
                version INTEGER DEFAULT 1,
                requires_ack BOOLEAN DEFAULT true,
                active BOOLEAN DEFAULT true,
                author_name TEXT,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            )
        """,
        "sop_acknowledgements": """
            CREATE TABLE IF NOT EXISTS sop_acknowledgements (
                id TEXT PRIMARY KEY,
                venue_id TEXT NOT NULL,
                doc_id TEXT NOT NULL,
                doc_version INTEGER NOT NULL,
                employee_id TEXT NOT NULL,
                employee_name TEXT,
                acknowledged_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                UNIQUE (doc_id, doc_version, employee_id)
            )
        """,
        "feed_posts": """
            CREATE TABLE IF NOT EXISTS feed_posts (
                id TEXT PRIMARY KEY,
                venue_id TEXT NOT NULL,
                author_user_id TEXT,
                author_name TEXT,
                author_role TEXT,
                body TEXT NOT NULL,
                pinned BOOLEAN DEFAULT false,
                removed BOOLEAN DEFAULT false,
                reactions JSONB DEFAULT '{}'::jsonb,
                comments JSONB DEFAULT '[]'::jsonb,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            )
        """,
        "recipes": """
            CREATE TABLE IF NOT EXISTS recipes (
                id TEXT PRIMARY KEY,
                venue_id TEXT NOT NULL,
                name TEXT NOT NULL,
                category TEXT,
                sell_price_inc_gst NUMERIC NOT NULL DEFAULT 0,
                yield_portions NUMERIC NOT NULL DEFAULT 1,
                items JSONB NOT NULL DEFAULT '[]',
                active BOOLEAN DEFAULT true,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            )
        """,
    }

    def _ensure_table(self, cur, name: str) -> None:
        """Lazily create a feature table that no migration provisions.

        Idempotent (CREATE TABLE IF NOT EXISTS); cheap to call at the top of
        any read/write method that references the table.
        """
        ddl = self._TABLE_DDL.get(name)
        if ddl:
            cur.execute(ddl)

    # Core tables provisioned by migration 001, mirrored here as idempotent
    # CREATE TABLE IF NOT EXISTS so the app self-heals when 001 never completed
    # (e.g. CREATE EXTENSION privilege failure on managed Postgres). Faithful to
    # 001's columns/constraints; the privileged uuid-ossp extension is omitted
    # (the schema uses app-generated TEXT ids, so it is not needed). Ordered by
    # foreign-key dependency; each runs independently under autocommit.
    _CORE_SCHEMA_DDL = [
        ("venues", """
            CREATE TABLE IF NOT EXISTS venues (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                tanda_org_id TEXT UNIQUE,
                state TEXT NOT NULL,
                timezone TEXT NOT NULL DEFAULT 'Australia/Melbourne',
                min_staff JSONB DEFAULT '{}',
                max_labour_pct NUMERIC(5, 2) NOT NULL,
                pos_system TEXT,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            )"""),
        ("employees", """
            CREATE TABLE IF NOT EXISTS employees (
                id TEXT PRIMARY KEY,
                venue_id TEXT NOT NULL REFERENCES venues(id) ON DELETE CASCADE,
                tanda_id TEXT,
                name TEXT NOT NULL,
                employment_type TEXT NOT NULL,
                award_level TEXT NOT NULL,
                hourly_base_rate NUMERIC(10, 2) NOT NULL,
                skills JSONB DEFAULT '[]',
                availability JSONB DEFAULT '{}',
                max_hours_per_week NUMERIC(5, 2) DEFAULT 38.0,
                consecutive_days INTEGER DEFAULT 6,
                phone TEXT,
                email TEXT,
                active BOOLEAN DEFAULT true,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            )"""),
        ("rosters", """
            CREATE TABLE IF NOT EXISTS rosters (
                id TEXT PRIMARY KEY,
                venue_id TEXT NOT NULL REFERENCES venues(id) ON DELETE CASCADE,
                week_start DATE NOT NULL,
                week_end DATE NOT NULL,
                total_cost NUMERIC(12, 2),
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            )"""),
        ("shifts", """
            CREATE TABLE IF NOT EXISTS shifts (
                id TEXT PRIMARY KEY,
                roster_id TEXT NOT NULL REFERENCES rosters(id) ON DELETE CASCADE,
                employee_id TEXT NOT NULL REFERENCES employees(id) ON DELETE CASCADE,
                shift_date DATE NOT NULL,
                start_time TIME NOT NULL,
                end_time TIME NOT NULL,
                break_minutes INTEGER DEFAULT 0,
                status TEXT NOT NULL DEFAULT 'scheduled',
                role TEXT DEFAULT 'general',
                cost NUMERIC(10, 2),
                penalty_multiplier NUMERIC(5, 2) DEFAULT 1.0,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            )"""),
        ("forecasts", """
            CREATE TABLE IF NOT EXISTS forecasts (
                id TEXT PRIMARY KEY,
                venue_id TEXT NOT NULL REFERENCES venues(id) ON DELETE CASCADE,
                forecast_date DATE NOT NULL,
                hour INTEGER NOT NULL CHECK (hour >= 0 AND hour <= 23),
                predicted_covers NUMERIC(10, 2) NOT NULL,
                confidence NUMERIC(3, 2) NOT NULL CHECK (confidence >= 0 AND confidence <= 1),
                signals_used TEXT[] DEFAULT '{}',
                model_version TEXT NOT NULL,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(venue_id, forecast_date, hour, model_version)
            )"""),
        ("users", """
            CREATE TABLE IF NOT EXISTS users (
                id TEXT PRIMARY KEY,
                email TEXT NOT NULL UNIQUE,
                password_hash TEXT NOT NULL,
                name TEXT NOT NULL,
                role TEXT NOT NULL DEFAULT 'staff',
                api_key_hash TEXT,
                is_active BOOLEAN DEFAULT true,
                venue_ids JSONB DEFAULT '[]',
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                last_login TIMESTAMP WITH TIME ZONE
            )"""),
        ("refresh_tokens", """
            CREATE TABLE IF NOT EXISTS refresh_tokens (
                token_hash TEXT PRIMARY KEY,
                user_id TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                expires_at TIMESTAMP WITH TIME ZONE NOT NULL,
                is_revoked BOOLEAN DEFAULT false,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            )"""),
        ("login_attempts", """
            CREATE TABLE IF NOT EXISTS login_attempts (
                id BIGSERIAL PRIMARY KEY,
                email TEXT NOT NULL,
                ip_address TEXT NOT NULL,
                success BOOLEAN NOT NULL,
                attempted_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            )"""),
        ("subscriptions", """
            CREATE TABLE IF NOT EXISTS subscriptions (
                venue_id TEXT PRIMARY KEY REFERENCES venues(id) ON DELETE CASCADE,
                stripe_customer_id TEXT,
                stripe_subscription_id TEXT UNIQUE,
                tier TEXT NOT NULL DEFAULT 'starter',
                status TEXT NOT NULL DEFAULT 'inactive',
                current_period_start TIMESTAMP WITH TIME ZONE,
                current_period_end TIMESTAMP WITH TIME ZONE,
                payment_method TEXT,
                last_payment_date TIMESTAMP WITH TIME ZONE,
                next_billing_date TIMESTAMP WITH TIME ZONE,
                cancel_at_period_end BOOLEAN DEFAULT false,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            )"""),
        ("onboarding_states", """
            CREATE TABLE IF NOT EXISTS onboarding_states (
                venue_id TEXT PRIMARY KEY REFERENCES venues(id) ON DELETE CASCADE,
                state_data JSONB NOT NULL DEFAULT '{}',
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            )"""),
        ("plugin_installs", """
            CREATE TABLE IF NOT EXISTS plugin_installs (
                organisation_id TEXT PRIMARY KEY,
                venue_id TEXT REFERENCES venues(id) ON DELETE SET NULL,
                status TEXT NOT NULL DEFAULT 'active',
                tokens JSONB DEFAULT '{}',
                installed_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            )"""),
        ("xero_credentials", """
            CREATE TABLE IF NOT EXISTS xero_credentials (
                venue_id TEXT PRIMARY KEY REFERENCES venues(id) ON DELETE CASCADE,
                client_id TEXT,
                client_secret TEXT,
                tenant_id TEXT,
                access_token TEXT,
                refresh_token TEXT,
                token_expires TIMESTAMP WITH TIME ZONE,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            )"""),
        ("feed_configs", """
            CREATE TABLE IF NOT EXISTS feed_configs (
                venue_id TEXT NOT NULL REFERENCES venues(id) ON DELETE CASCADE,
                feed_name TEXT NOT NULL,
                enabled BOOLEAN DEFAULT true,
                api_key TEXT,
                poll_interval_minutes INTEGER DEFAULT 30,
                last_updated_at TIMESTAMP WITH TIME ZONE,
                last_tested_at TIMESTAMP WITH TIME ZONE,
                last_test_status TEXT,
                custom_params JSONB DEFAULT '{}',
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (venue_id, feed_name)
            )"""),
        ("billing_events", """
            CREATE TABLE IF NOT EXISTS billing_events (
                event_id TEXT PRIMARY KEY,
                venue_id TEXT NOT NULL REFERENCES venues(id) ON DELETE CASCADE,
                event_type TEXT NOT NULL,
                stripe_event_id TEXT UNIQUE,
                payload JSONB NOT NULL,
                processed BOOLEAN DEFAULT false,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            )"""),
        ("webhook_events", """
            CREATE TABLE IF NOT EXISTS webhook_events (
                webhook_id TEXT PRIMARY KEY,
                event_type TEXT NOT NULL,
                payload_hash TEXT NOT NULL,
                processed_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            )"""),
    ]

    def _ensure_core_schema(self) -> None:
        """Create any missing core tables (idempotent, best-effort).

        Runs at connect time. Each statement is independent under autocommit, so
        a single failure (e.g. a permission issue) is logged and the rest still
        apply. Never raises — a self-heal must never prevent app startup.
        """
        created = 0
        for name, ddl in self._CORE_SCHEMA_DDL:
            try:
                with self._cursor() as cur:
                    cur.execute(ddl)
                created += 1
            except Exception as e:  # noqa: BLE001 — best-effort, must not crash boot
                logger.warning("Core-schema ensure for %s failed: %s", name, e)
        logger.info("Core schema ensured (%d/%d tables present).",
                    created, len(self._CORE_SCHEMA_DDL))

        # Additive columns on tables that may already exist from an earlier
        # schema (CREATE TABLE IF NOT EXISTS won't add a column to an existing
        # table). venue_ids persists which venues a non-owner user may access —
        # without it, staff users get [] and can't reach their own venue.
        for alter in (
            "ALTER TABLE users ADD COLUMN IF NOT EXISTS venue_ids JSONB DEFAULT '[]'",
            "ALTER TABLE timesheets ADD COLUMN IF NOT EXISTS approved_by TEXT",
            "ALTER TABLE timesheets ADD COLUMN IF NOT EXISTS approved_at TIMESTAMP WITH TIME ZONE",
            "ALTER TABLE timesheets ADD COLUMN IF NOT EXISTS adjustment_note TEXT",
            # tanda_org_id is UNIQUE; venues without Tanda must store NULL (which
            # never collides), not '' (which collides on the second venue ever).
            # The original schema also made it NOT NULL — drop that first or the
            # NULL writes trade a unique-violation 500 for a not-null 500.
            "ALTER TABLE venues ALTER COLUMN tanda_org_id DROP NOT NULL",
            "UPDATE venues SET tanda_org_id = NULL WHERE tanda_org_id = ''",
            "ALTER TABLE ingredients ADD COLUMN IF NOT EXISTS stock_qty NUMERIC DEFAULT 0",
            "ALTER TABLE ingredients ADD COLUMN IF NOT EXISTS par_level NUMERIC DEFAULT 0",
            # The event log is queried by time, by venue and by category on every
            # Activity/health view. Without these it is a growing seq-scan.
            # Right-to-erasure writes employees.anonymised_at, which existed
            # only on the side table — so every Privacy Act erasure 500'd and
            # the PII was never scrubbed.
            "ALTER TABLE employees ADD COLUMN IF NOT EXISTS anonymised_at TIMESTAMP WITH TIME ZONE",
            "CREATE INDEX IF NOT EXISTS idx_audit_logs_created ON audit_logs (created_at DESC)",
            "CREATE INDEX IF NOT EXISTS idx_audit_logs_venue_created ON audit_logs (venue_id, created_at DESC)",
            "CREATE INDEX IF NOT EXISTS idx_audit_logs_category ON audit_logs ((details->>'category'), created_at DESC)",
            "CREATE INDEX IF NOT EXISTS idx_audit_logs_action ON audit_logs (action, created_at DESC)",
        ):
            try:
                with self._cursor() as cur:
                    cur.execute(alter)
            except Exception as e:  # noqa: BLE001
                logger.warning("Core-schema column ensure failed: %s", e)

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
                # Empty tanda_org_id -> NULL: the column is UNIQUE and most venues
                # don't use Tanda, so "" would collide on the second venue ever
                # created (the production onboarding 500 found in preflight).
                venue.id, venue.name, venue.tanda_org_id or None, venue.state.value,
                venue.timezone, _json(venue.min_staff),
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
            id=row["id"], name=row["name"], tanda_org_id=row["tanda_org_id"] or "",
            state=State(row["state"]), timezone=row["timezone"],
            min_staff=row["min_staff"] if isinstance(row["min_staff"], dict) else json.loads(row["min_staff"]),
            max_labour_pct=float(row["max_labour_pct"]),
            pos_system=row.get("pos_system"),
            created_at=row["created_at"],
        )

    # --- Employees ---

    def save_employee(self, emp):
        venue_id = emp.venue_id
        if venue_id is None:
            logger.warning(
                "Employee %s has no venue_id set, saving with venue_id=NULL",
                emp.id,
            )
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
                emp.id, venue_id, emp.tanda_id, emp.name,
                emp.employment_type.value, emp.award_level.value,
                float(emp.hourly_base_rate), _json(emp.skills),
                _json(emp.availability), emp.max_hours_per_week,
                emp.consecutive_days_limit, emp.phone, emp.email,
                emp.created_at, emp.updated_at,
            ))

    def list_employees(self):
        with self._cursor() as cur:
            cur.execute("SELECT * FROM employees WHERE active = true ORDER BY name")
            return [self._row_to_employee(r) for r in cur.fetchall()]

    # --- Native time clock ---

    def save_timesheet(self, ts):
        with self._cursor() as cur:
            self._ensure_table(cur, "timesheets")
            cur.execute("""
                INSERT INTO timesheets (id, venue_id, employee_id, work_date, clock_in,
                    clock_out, break_minutes, status, pin_verified, rostered_shift_id,
                    variance_minutes, approved_by, approved_at, adjustment_note,
                    created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now())
                ON CONFLICT (id) DO UPDATE SET
                    clock_in=EXCLUDED.clock_in, clock_out=EXCLUDED.clock_out,
                    break_minutes=EXCLUDED.break_minutes,
                    status=EXCLUDED.status, pin_verified=EXCLUDED.pin_verified,
                    rostered_shift_id=EXCLUDED.rostered_shift_id,
                    variance_minutes=EXCLUDED.variance_minutes,
                    approved_by=EXCLUDED.approved_by, approved_at=EXCLUDED.approved_at,
                    adjustment_note=EXCLUDED.adjustment_note, updated_at=now()
            """, (
                ts["id"], ts["venue_id"], ts["employee_id"], ts["work_date"],
                ts["clock_in"], ts.get("clock_out"), ts.get("break_minutes", 0),
                ts.get("status", "open"), ts.get("pin_verified", False),
                ts.get("rostered_shift_id"), ts.get("variance_minutes"),
                ts.get("approved_by"), ts.get("approved_at"), ts.get("adjustment_note"),
                ts.get("created_at", datetime.utcnow()),
            ))

    def get_timesheet(self, ts_id):
        with self._cursor() as cur:
            self._ensure_table(cur, "timesheets")
            cur.execute("SELECT * FROM timesheets WHERE id = %s", (ts_id,))
            row = cur.fetchone()
            return dict(row) if row else None

    def get_open_timesheet(self, venue_id, employee_id):
        with self._cursor() as cur:
            self._ensure_table(cur, "timesheets")
            cur.execute("""
                SELECT * FROM timesheets
                WHERE venue_id = %s AND employee_id = %s AND clock_out IS NULL
                ORDER BY clock_in DESC LIMIT 1
            """, (venue_id, employee_id))
            row = cur.fetchone()
            return dict(row) if row else None

    def get_timesheets(self, venue_id, start_date, end_date):
        with self._cursor() as cur:
            self._ensure_table(cur, "timesheets")
            cur.execute("""
                SELECT * FROM timesheets
                WHERE venue_id = %s AND work_date >= %s AND work_date <= %s
                ORDER BY work_date, clock_in
            """, (venue_id, start_date, end_date))
            return [dict(r) for r in cur.fetchall()]

    def set_timeclock_pin(self, venue_id, employee_id, pin_hash):
        with self._cursor() as cur:
            self._ensure_table(cur, "timeclock_pins")
            cur.execute("""
                INSERT INTO timeclock_pins (venue_id, employee_id, pin_hash, updated_at)
                VALUES (%s, %s, %s, now())
                ON CONFLICT (venue_id, employee_id) DO UPDATE SET
                    pin_hash=EXCLUDED.pin_hash, updated_at=now()
            """, (venue_id, employee_id, pin_hash))

    def get_timeclock_pin(self, venue_id, employee_id):
        with self._cursor() as cur:
            self._ensure_table(cur, "timeclock_pins")
            cur.execute("""
                SELECT pin_hash FROM timeclock_pins
                WHERE venue_id = %s AND employee_id = %s
            """, (venue_id, employee_id))
            row = cur.fetchone()
            return row["pin_hash"] if row else None

    # --- Compliance checklists ---

    def save_checklist_template(self, tpl):
        with self._cursor() as cur:
            self._ensure_table(cur, "checklist_templates")
            cur.execute("""
                INSERT INTO checklist_templates (id, venue_id, name, schedule, items, active, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, now())
                ON CONFLICT (id) DO UPDATE SET
                    name=EXCLUDED.name, schedule=EXCLUDED.schedule, items=EXCLUDED.items,
                    active=EXCLUDED.active, updated_at=now()
            """, (
                tpl["id"], tpl["venue_id"], tpl["name"], tpl.get("schedule", "daily"),
                _json(tpl.get("items", [])), tpl.get("active", True),
                tpl.get("created_at", datetime.utcnow()),
            ))

    @staticmethod
    def _row_to_checklist(row):
        d = dict(row)
        for f in ("items",):
            v = d.get(f)
            if isinstance(v, str):
                try:
                    d[f] = json.loads(v)
                except Exception:
                    d[f] = []
        return d

    def get_checklist_template(self, tpl_id):
        with self._cursor() as cur:
            self._ensure_table(cur, "checklist_templates")
            cur.execute("SELECT * FROM checklist_templates WHERE id = %s", (tpl_id,))
            row = cur.fetchone()
            return self._row_to_checklist(row) if row else None

    def list_checklist_templates(self, venue_id):
        with self._cursor() as cur:
            self._ensure_table(cur, "checklist_templates")
            cur.execute("SELECT * FROM checklist_templates WHERE venue_id = %s ORDER BY name", (venue_id,))
            return [self._row_to_checklist(r) for r in cur.fetchall()]

    def save_checklist_run(self, run):
        with self._cursor() as cur:
            self._ensure_table(cur, "checklist_runs")
            cur.execute("""
                INSERT INTO checklist_runs (id, venue_id, template_id, template_name, run_date,
                    started_at, completed_at, completed_by, status, items, flags_count,
                    created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now())
                ON CONFLICT (id) DO UPDATE SET
                    completed_at=EXCLUDED.completed_at, completed_by=EXCLUDED.completed_by,
                    status=EXCLUDED.status, items=EXCLUDED.items,
                    flags_count=EXCLUDED.flags_count, updated_at=now()
            """, (
                run["id"], run["venue_id"], run["template_id"], run.get("template_name"),
                run["run_date"], run.get("started_at"), run.get("completed_at"),
                run.get("completed_by"), run.get("status", "in_progress"),
                _json(run.get("items", [])), run.get("flags_count", 0),
                run.get("created_at", datetime.utcnow()),
            ))

    def get_checklist_run(self, run_id):
        with self._cursor() as cur:
            self._ensure_table(cur, "checklist_runs")
            cur.execute("SELECT * FROM checklist_runs WHERE id = %s", (run_id,))
            row = cur.fetchone()
            return self._row_to_checklist(row) if row else None

    def list_checklist_runs(self, venue_id, start_date, end_date):
        with self._cursor() as cur:
            self._ensure_table(cur, "checklist_runs")
            cur.execute("""
                SELECT * FROM checklist_runs
                WHERE venue_id = %s AND run_date >= %s AND run_date <= %s
                ORDER BY run_date, started_at
            """, (venue_id, start_date, end_date))
            return [self._row_to_checklist(r) for r in cur.fetchall()]

    # --- Menu costing ---

    def save_ingredient(self, ing):
        with self._cursor() as cur:
            self._ensure_table(cur, "ingredients")
            cur.execute("""
                INSERT INTO ingredients (id, venue_id, name, unit, purchase_size, purchase_cost,
                    cost_per_unit, supplier, active, stock_qty, par_level, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now())
                ON CONFLICT (id) DO UPDATE SET
                    name=EXCLUDED.name, unit=EXCLUDED.unit,
                    purchase_size=EXCLUDED.purchase_size, purchase_cost=EXCLUDED.purchase_cost,
                    cost_per_unit=EXCLUDED.cost_per_unit, supplier=EXCLUDED.supplier,
                    active=EXCLUDED.active, stock_qty=EXCLUDED.stock_qty,
                    par_level=EXCLUDED.par_level, updated_at=now()
            """, (
                ing["id"], ing["venue_id"], ing["name"], ing.get("unit", "each"),
                float(ing.get("purchase_size", 1)), float(ing.get("purchase_cost", 0)),
                float(ing.get("cost_per_unit", 0)), ing.get("supplier"),
                ing.get("active", True), float(ing.get("stock_qty", 0) or 0),
                float(ing.get("par_level", 0) or 0),
                ing.get("created_at", datetime.utcnow()),
            ))

    @staticmethod
    def _row_to_plain(row, json_fields=()):
        d = dict(row)
        for f in json_fields:
            v = d.get(f)
            if isinstance(v, str):
                try:
                    d[f] = json.loads(v)
                except Exception:
                    d[f] = []
        for k, v in list(d.items()):
            if hasattr(v, "quantize"):  # Decimal -> float for JSON friendliness
                d[k] = float(v)
        return d

    def get_ingredient(self, ing_id):
        with self._cursor() as cur:
            self._ensure_table(cur, "ingredients")
            cur.execute("SELECT * FROM ingredients WHERE id = %s", (ing_id,))
            row = cur.fetchone()
            return self._row_to_plain(row) if row else None

    def list_ingredients(self, venue_id):
        with self._cursor() as cur:
            self._ensure_table(cur, "ingredients")
            cur.execute("SELECT * FROM ingredients WHERE venue_id = %s ORDER BY name", (venue_id,))
            return [self._row_to_plain(r) for r in cur.fetchall()]

    def save_recipe(self, recipe):
        with self._cursor() as cur:
            self._ensure_table(cur, "recipes")
            cur.execute("""
                INSERT INTO recipes (id, venue_id, name, category, sell_price_inc_gst,
                    yield_portions, items, active, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, now())
                ON CONFLICT (id) DO UPDATE SET
                    name=EXCLUDED.name, category=EXCLUDED.category,
                    sell_price_inc_gst=EXCLUDED.sell_price_inc_gst,
                    yield_portions=EXCLUDED.yield_portions, items=EXCLUDED.items,
                    active=EXCLUDED.active, updated_at=now()
            """, (
                recipe["id"], recipe["venue_id"], recipe["name"], recipe.get("category"),
                float(recipe.get("sell_price_inc_gst", 0)), float(recipe.get("yield_portions", 1)),
                _json(recipe.get("items", [])), recipe.get("active", True),
                recipe.get("created_at", datetime.utcnow()),
            ))

    def get_recipe(self, recipe_id):
        with self._cursor() as cur:
            self._ensure_table(cur, "recipes")
            cur.execute("SELECT * FROM recipes WHERE id = %s", (recipe_id,))
            row = cur.fetchone()
            return self._row_to_plain(row, json_fields=("items",)) if row else None

    def list_recipes(self, venue_id):
        with self._cursor() as cur:
            self._ensure_table(cur, "recipes")
            cur.execute("SELECT * FROM recipes WHERE venue_id = %s ORDER BY name", (venue_id,))
            return [self._row_to_plain(r, json_fields=("items",)) for r in cur.fetchall()]

    # --- Leave requests ---

    def save_leave_request(self, req):
        with self._cursor() as cur:
            self._ensure_table(cur, "leave_requests")
            cur.execute("""
                INSERT INTO leave_requests (id, venue_id, employee_id, start_date, end_date,
                    reason, status, decided_by, decided_at, decision_note, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now())
                ON CONFLICT (id) DO UPDATE SET
                    status=EXCLUDED.status, decided_by=EXCLUDED.decided_by,
                    decided_at=EXCLUDED.decided_at, decision_note=EXCLUDED.decision_note,
                    updated_at=now()
            """, (
                req["id"], req["venue_id"], req["employee_id"], req["start_date"],
                req["end_date"], req.get("reason"), req.get("status", "pending"),
                req.get("decided_by"), req.get("decided_at"), req.get("decision_note"),
                req.get("created_at", datetime.utcnow()),
            ))

    def get_leave_request(self, req_id):
        with self._cursor() as cur:
            self._ensure_table(cur, "leave_requests")
            cur.execute("SELECT * FROM leave_requests WHERE id = %s", (req_id,))
            row = cur.fetchone()
            return dict(row) if row else None

    def list_leave_requests(self, venue_id):
        with self._cursor() as cur:
            self._ensure_table(cur, "leave_requests")
            cur.execute("""
                SELECT * FROM leave_requests WHERE venue_id = %s ORDER BY created_at DESC
            """, (venue_id,))
            return [dict(r) for r in cur.fetchall()]

    def save_shift_cover(self, cover):
        with self._cursor() as cur:
            self._ensure_table(cur, "shift_covers")
            cur.execute("""
                INSERT INTO shift_covers (id, venue_id, shift_id, shift_date, shift_start,
                    shift_end, role, requested_by, reason, claimed_by, status,
                    decided_by, decided_at, decision_note, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now())
                ON CONFLICT (id) DO UPDATE SET
                    claimed_by=EXCLUDED.claimed_by, status=EXCLUDED.status,
                    decided_by=EXCLUDED.decided_by, decided_at=EXCLUDED.decided_at,
                    decision_note=EXCLUDED.decision_note, updated_at=now()
            """, (
                cover["id"], cover["venue_id"], cover["shift_id"], cover["shift_date"],
                cover.get("shift_start"), cover.get("shift_end"), cover.get("role"),
                cover["requested_by"], cover.get("reason"), cover.get("claimed_by"),
                cover.get("status", "open"), cover.get("decided_by"),
                cover.get("decided_at"), cover.get("decision_note"),
                cover.get("created_at", datetime.utcnow()),
            ))

    def get_shift_cover(self, cover_id):
        with self._cursor() as cur:
            self._ensure_table(cur, "shift_covers")
            cur.execute("SELECT * FROM shift_covers WHERE id = %s", (cover_id,))
            row = cur.fetchone()
            return dict(row) if row else None

    def list_shift_covers(self, venue_id):
        with self._cursor() as cur:
            self._ensure_table(cur, "shift_covers")
            cur.execute("""
                SELECT * FROM shift_covers WHERE venue_id = %s ORDER BY created_at DESC
            """, (venue_id,))
            return [dict(r) for r in cur.fetchall()]

    def save_announcement(self, ann):
        with self._cursor() as cur:
            self._ensure_table(cur, "announcements")
            cur.execute("""
                INSERT INTO announcements (id, venue_id, title, body, author_id,
                    author_name, pinned, sms_result, read_by, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now())
                ON CONFLICT (id) DO UPDATE SET
                    title=EXCLUDED.title, body=EXCLUDED.body, pinned=EXCLUDED.pinned,
                    sms_result=EXCLUDED.sms_result, read_by=EXCLUDED.read_by,
                    updated_at=now()
            """, (
                ann["id"], ann["venue_id"], ann["title"], ann["body"],
                ann.get("author_id"), ann.get("author_name"),
                bool(ann.get("pinned", False)),
                _json(ann["sms_result"]) if ann.get("sms_result") is not None else None,
                _json(list(ann.get("read_by") or [])),
                ann.get("created_at", datetime.utcnow()),
            ))

    def get_announcement(self, ann_id):
        with self._cursor() as cur:
            self._ensure_table(cur, "announcements")
            cur.execute("SELECT * FROM announcements WHERE id = %s", (ann_id,))
            row = cur.fetchone()
            return dict(row) if row else None

    def list_announcements(self, venue_id):
        with self._cursor() as cur:
            self._ensure_table(cur, "announcements")
            cur.execute("""
                SELECT * FROM announcements WHERE venue_id = %s
                ORDER BY pinned DESC, created_at DESC
            """, (venue_id,))
            return [dict(r) for r in cur.fetchall()]

    # --- SOP / JSP document library (procedures + acknowledgements) ---

    def save_sop_document(self, doc):
        with self._cursor() as cur:
            self._ensure_table(cur, "sop_documents")
            cur.execute("""
                INSERT INTO sop_documents (id, venue_id, title, category, body,
                    applies_to, version, requires_ack, active, author_name,
                    created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now())
                ON CONFLICT (id) DO UPDATE SET
                    title=EXCLUDED.title, category=EXCLUDED.category,
                    body=EXCLUDED.body, applies_to=EXCLUDED.applies_to,
                    version=EXCLUDED.version, requires_ack=EXCLUDED.requires_ack,
                    active=EXCLUDED.active, author_name=EXCLUDED.author_name,
                    updated_at=now()
            """, (
                doc["id"], doc["venue_id"], doc["title"], doc.get("category", "sop"),
                doc["body"], _json(list(doc.get("applies_to") or [])),
                int(doc.get("version", 1) or 1), bool(doc.get("requires_ack", True)),
                bool(doc.get("active", True)), doc.get("author_name"),
                doc.get("created_at", datetime.utcnow()),
            ))

    def get_sop_document(self, doc_id):
        with self._cursor() as cur:
            self._ensure_table(cur, "sop_documents")
            cur.execute("SELECT * FROM sop_documents WHERE id = %s", (doc_id,))
            row = cur.fetchone()
            return self._row_to_plain(row, json_fields=("applies_to",)) if row else None

    def delete_sop_document(self, doc_id):
        with self._cursor() as cur:
            self._ensure_table(cur, "sop_documents")
            cur.execute("DELETE FROM sop_documents WHERE id = %s", (doc_id,))

    def list_sop_documents(self, venue_id, include_inactive=False):
        with self._cursor() as cur:
            self._ensure_table(cur, "sop_documents")
            if include_inactive:
                cur.execute("""
                    SELECT * FROM sop_documents WHERE venue_id = %s
                    ORDER BY created_at, title
                """, (venue_id,))
            else:
                cur.execute("""
                    SELECT * FROM sop_documents WHERE venue_id = %s AND active = true
                    ORDER BY created_at, title
                """, (venue_id,))
            return [self._row_to_plain(r, json_fields=("applies_to",)) for r in cur.fetchall()]

    def save_sop_ack(self, ack):
        with self._cursor() as cur:
            self._ensure_table(cur, "sop_acknowledgements")
            cur.execute("""
                INSERT INTO sop_acknowledgements (id, venue_id, doc_id, doc_version,
                    employee_id, employee_name, acknowledged_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (doc_id, doc_version, employee_id) DO NOTHING
            """, (
                ack["id"], ack["venue_id"], ack["doc_id"], int(ack["doc_version"]),
                ack["employee_id"], ack.get("employee_name"),
                ack.get("acknowledged_at", datetime.utcnow()),
            ))

    def list_sop_acks(self, venue_id, doc_id=None):
        with self._cursor() as cur:
            self._ensure_table(cur, "sop_acknowledgements")
            if doc_id is not None:
                cur.execute("""
                    SELECT * FROM sop_acknowledgements
                    WHERE venue_id = %s AND doc_id = %s
                    ORDER BY acknowledged_at
                """, (venue_id, doc_id))
            else:
                cur.execute("""
                    SELECT * FROM sop_acknowledgements WHERE venue_id = %s
                    ORDER BY acknowledged_at
                """, (venue_id,))
            return [self._row_to_plain(r) for r in cur.fetchall()]

    # --- Team feed (two-way posts) ---

    def save_feed_post(self, post):
        with self._cursor() as cur:
            self._ensure_table(cur, "feed_posts")
            cur.execute("""
                INSERT INTO feed_posts (id, venue_id, author_user_id, author_name,
                    author_role, body, pinned, removed, reactions, comments,
                    created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now())
                ON CONFLICT (id) DO UPDATE SET
                    author_name=EXCLUDED.author_name, author_role=EXCLUDED.author_role,
                    body=EXCLUDED.body, pinned=EXCLUDED.pinned, removed=EXCLUDED.removed,
                    reactions=EXCLUDED.reactions, comments=EXCLUDED.comments,
                    updated_at=now()
            """, (
                post["id"], post["venue_id"], post.get("author_user_id"),
                post.get("author_name"), post.get("author_role"), post["body"],
                bool(post.get("pinned", False)), bool(post.get("removed", False)),
                _json(dict(post.get("reactions") or {})),
                json.dumps(list(post.get("comments") or []), default=str),
                post.get("created_at", datetime.utcnow()),
            ))

    def get_feed_post(self, post_id):
        with self._cursor() as cur:
            self._ensure_table(cur, "feed_posts")
            cur.execute("SELECT * FROM feed_posts WHERE id = %s", (post_id,))
            row = cur.fetchone()
            return self._row_to_plain(row, json_fields=("reactions", "comments")) if row else None

    def list_feed_posts(self, venue_id, limit=50):
        with self._cursor() as cur:
            self._ensure_table(cur, "feed_posts")
            cur.execute("""
                SELECT * FROM feed_posts
                WHERE venue_id = %s AND removed = false
                ORDER BY pinned DESC, created_at DESC
                LIMIT %s
            """, (venue_id, int(limit or 50)))
            return [self._row_to_plain(r, json_fields=("reactions", "comments"))
                    for r in cur.fetchall()]

    def append_feed_comment(self, post_id, comment):
        """Atomic append: one statement using jsonb ``||`` so two comments
        posted at the same instant both land (no read-modify-write of the
        whole comments blob)."""
        with self._cursor() as cur:
            self._ensure_table(cur, "feed_posts")
            cur.execute("""
                UPDATE feed_posts
                SET comments = COALESCE(comments, '[]'::jsonb) || %s::jsonb,
                    updated_at = now()
                WHERE id = %s
                RETURNING *
            """, (json.dumps([comment], default=str), post_id))
            row = cur.fetchone()
            return (self._row_to_plain(row, json_fields=("reactions", "comments"))
                    if row else None)

    def toggle_feed_reaction(self, post_id, emoji, user_id):
        """Atomic toggle: lock the row (SELECT ... FOR UPDATE) inside an
        explicit transaction, flip the caller's membership under ``emoji``,
        write it back, commit. The connection runs autocommit, so BEGIN /
        COMMIT are issued explicitly on this one cursor."""
        with self._cursor() as cur:
            self._ensure_table(cur, "feed_posts")
            cur.execute("BEGIN")
            try:
                cur.execute("SELECT * FROM feed_posts WHERE id = %s FOR UPDATE", (post_id,))
                row = cur.fetchone()
                if not row:
                    cur.execute("ROLLBACK")
                    return None, None
                post = self._row_to_plain(row, json_fields=("reactions", "comments"))
                reactions = post.get("reactions")
                if not isinstance(reactions, dict):
                    reactions = {}
                ids = list(reactions.get(emoji) or [])
                if user_id in ids:
                    ids = [i for i in ids if i != user_id]
                    state = "removed"
                else:
                    ids.append(user_id)
                    state = "added"
                if ids:
                    reactions[emoji] = ids
                else:
                    reactions.pop(emoji, None)
                cur.execute("""
                    UPDATE feed_posts
                    SET reactions = %s::jsonb, updated_at = now()
                    WHERE id = %s
                    RETURNING *
                """, (_json(reactions), post_id))
                row = cur.fetchone()
                cur.execute("COMMIT")
            except Exception:
                try:
                    cur.execute("ROLLBACK")
                except Exception:
                    pass
                raise
            return (self._row_to_plain(row, json_fields=("reactions", "comments"))
                    if row else None), state

    def save_stocktake(self, st):
        with self._cursor() as cur:
            self._ensure_table(cur, "stocktakes")
            cur.execute("""
                INSERT INTO stocktakes (id, venue_id, status, items, started_by,
                    started_at, completed_at, total_variance_value, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, now())
                ON CONFLICT (id) DO UPDATE SET
                    status=EXCLUDED.status, items=EXCLUDED.items,
                    completed_at=EXCLUDED.completed_at,
                    total_variance_value=EXCLUDED.total_variance_value, updated_at=now()
            """, (
                st["id"], st["venue_id"], st.get("status", "open"),
                _json(st.get("items", [])), st.get("started_by"),
                st.get("started_at", datetime.utcnow()), st.get("completed_at"),
                st.get("total_variance_value"),
                st.get("created_at", datetime.utcnow()),
            ))

    def get_stocktake(self, st_id):
        with self._cursor() as cur:
            self._ensure_table(cur, "stocktakes")
            cur.execute("SELECT * FROM stocktakes WHERE id = %s", (st_id,))
            row = cur.fetchone()
            return self._row_to_plain(row, json_fields=("items",)) if row else None

    def list_stocktakes(self, venue_id):
        with self._cursor() as cur:
            self._ensure_table(cur, "stocktakes")
            cur.execute("""
                SELECT * FROM stocktakes WHERE venue_id = %s ORDER BY started_at DESC
            """, (venue_id,))
            return [self._row_to_plain(r, json_fields=("items",)) for r in cur.fetchall()]

    def save_supplier_order(self, order):
        with self._cursor() as cur:
            self._ensure_table(cur, "supplier_orders")
            cur.execute("""
                INSERT INTO supplier_orders (id, venue_id, supplier, status, items,
                    total_cost, created_at, ordered_at, received_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, now())
                ON CONFLICT (id) DO UPDATE SET
                    status=EXCLUDED.status, items=EXCLUDED.items,
                    total_cost=EXCLUDED.total_cost, ordered_at=EXCLUDED.ordered_at,
                    received_at=EXCLUDED.received_at, updated_at=now()
            """, (
                order["id"], order["venue_id"], order.get("supplier"),
                order.get("status", "draft"), _json(order.get("items", [])),
                float(order.get("total_cost", 0) or 0),
                order.get("created_at", datetime.utcnow()),
                order.get("ordered_at"), order.get("received_at"),
            ))

    def get_supplier_order(self, order_id):
        with self._cursor() as cur:
            self._ensure_table(cur, "supplier_orders")
            cur.execute("SELECT * FROM supplier_orders WHERE id = %s", (order_id,))
            row = cur.fetchone()
            return self._row_to_plain(row, json_fields=("items",)) if row else None

    def list_supplier_orders(self, venue_id):
        with self._cursor() as cur:
            self._ensure_table(cur, "supplier_orders")
            cur.execute("""
                SELECT * FROM supplier_orders WHERE venue_id = %s ORDER BY created_at DESC
            """, (venue_id,))
            return [self._row_to_plain(r, json_fields=("items",)) for r in cur.fetchall()]

    def transition_supplier_order(self, order_id, from_status, to_status, stamp_field=None):
        # Conditional UPDATE: only one concurrent caller can win the transition,
        # so double-receive/double-order clicks can never book stock twice.
        stamp_sql = f", {stamp_field} = now()" if stamp_field in ("ordered_at", "received_at") else ""
        with self._cursor() as cur:
            self._ensure_table(cur, "supplier_orders")
            cur.execute(f"""
                UPDATE supplier_orders SET status = %s{stamp_sql}, updated_at = now()
                WHERE id = %s AND status = %s
            """, (to_status, order_id, from_status))
            return cur.rowcount > 0

    def increment_ingredient_stock(self, ingredient_id, delta):
        with self._cursor() as cur:
            self._ensure_table(cur, "ingredients")
            cur.execute("""
                UPDATE ingredients
                SET stock_qty = COALESCE(stock_qty, 0) + %s, updated_at = now()
                WHERE id = %s
            """, (float(delta), ingredient_id))

    def save_supplier_invoice(self, inv):
        with self._cursor() as cur:
            self._ensure_table(cur, "supplier_invoices")
            cur.execute("""
                INSERT INTO supplier_invoices (id, venue_id, supplier, invoice_number,
                    invoice_date, order_id, items, total, price_changes, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO NOTHING
            """, (
                inv["id"], inv["venue_id"], inv.get("supplier"), inv["invoice_number"],
                inv.get("invoice_date"), inv.get("order_id"),
                _json(inv.get("items", [])), float(inv.get("total", 0) or 0),
                _json(inv.get("price_changes", [])),
                inv.get("created_at", datetime.utcnow()),
            ))

    def list_supplier_invoices(self, venue_id):
        with self._cursor() as cur:
            self._ensure_table(cur, "supplier_invoices")
            cur.execute("""
                SELECT * FROM supplier_invoices WHERE venue_id = %s ORDER BY created_at DESC
            """, (venue_id,))
            return [self._row_to_plain(r, json_fields=("items", "price_changes"))
                    for r in cur.fetchall()]

    # --- Xero bill push ledger (PK == invoice id: one push per invoice) ---

    def save_xero_bill_push(self, rec):
        with self._cursor() as cur:
            self._ensure_table(cur, "xero_bill_pushes")
            cur.execute("""
                INSERT INTO xero_bill_pushes (id, venue_id, invoice_id, xero_invoice_id,
                    xero_invoice_number, status, pushed_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO NOTHING
            """, (
                rec["id"], rec["venue_id"], rec["invoice_id"],
                rec.get("xero_invoice_id"), rec.get("xero_invoice_number"),
                rec.get("status", "pushed"), rec.get("pushed_at", datetime.utcnow()),
            ))

    def get_xero_bill_push(self, invoice_id):
        with self._cursor() as cur:
            self._ensure_table(cur, "xero_bill_pushes")
            cur.execute("SELECT * FROM xero_bill_pushes WHERE id = %s", (invoice_id,))
            row = cur.fetchone()
            return self._row_to_plain(row) if row else None

    def list_xero_bill_pushes(self, venue_id):
        with self._cursor() as cur:
            self._ensure_table(cur, "xero_bill_pushes")
            cur.execute("""
                SELECT * FROM xero_bill_pushes WHERE venue_id = %s ORDER BY pushed_at DESC
            """, (venue_id,))
            return [self._row_to_plain(r) for r in cur.fetchall()]

    # --- MYOB bill push ledger (PK == invoice id: one push per invoice) ---

    def save_myob_bill_push(self, rec):
        with self._cursor() as cur:
            self._ensure_table(cur, "myob_bill_pushes")
            cur.execute("""
                INSERT INTO myob_bill_pushes (id, venue_id, invoice_id, myob_bill_uid,
                    myob_bill_number, status, pushed_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO NOTHING
            """, (
                rec["id"], rec["venue_id"], rec["invoice_id"],
                rec.get("myob_bill_uid"), rec.get("myob_bill_number"),
                rec.get("status", "pushed"), rec.get("pushed_at", datetime.utcnow()),
            ))

    def get_myob_bill_push(self, invoice_id):
        with self._cursor() as cur:
            self._ensure_table(cur, "myob_bill_pushes")
            cur.execute("SELECT * FROM myob_bill_pushes WHERE id = %s", (invoice_id,))
            row = cur.fetchone()
            return self._row_to_plain(row) if row else None

    def list_myob_bill_pushes(self, venue_id):
        with self._cursor() as cur:
            self._ensure_table(cur, "myob_bill_pushes")
            cur.execute("""
                SELECT * FROM myob_bill_pushes WHERE venue_id = %s ORDER BY pushed_at DESC
            """, (venue_id,))
            return [self._row_to_plain(r) for r in cur.fetchall()]

    def save_waste_entry(self, entry):
        with self._cursor() as cur:
            self._ensure_table(cur, "waste_log")
            cur.execute("""
                INSERT INTO waste_log (id, venue_id, ingredient_id, ingredient_name,
                    waste_date, qty, unit, reason, value, note, logged_by, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO NOTHING
            """, (
                entry["id"], entry["venue_id"], entry["ingredient_id"],
                entry.get("ingredient_name"), entry["waste_date"],
                float(entry.get("qty", 0) or 0), entry.get("unit"),
                entry.get("reason"), float(entry.get("value", 0) or 0),
                entry.get("note"), entry.get("logged_by"),
                entry.get("created_at", datetime.utcnow()),
            ))

    def list_waste_entries(self, venue_id, start_date, end_date):
        with self._cursor() as cur:
            self._ensure_table(cur, "waste_log")
            cur.execute("""
                SELECT * FROM waste_log
                WHERE venue_id = %s AND waste_date BETWEEN %s AND %s
                ORDER BY created_at DESC
            """, (venue_id, start_date, end_date))
            return [self._row_to_plain(r) for r in cur.fetchall()]

    def save_dish_sale(self, sale):
        with self._cursor() as cur:
            self._ensure_table(cur, "dish_sales")
            cur.execute("""
                INSERT INTO dish_sales (id, venue_id, sale_date, recipe_id, recipe_name,
                    qty, revenue_inc_gst, cogs, source, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO NOTHING
            """, (
                sale["id"], sale["venue_id"], sale["sale_date"], sale["recipe_id"],
                sale.get("recipe_name"), float(sale.get("qty", 0)),
                float(sale.get("revenue_inc_gst", 0)), float(sale.get("cogs", 0)),
                sale.get("source", "manual"), sale.get("created_at", datetime.utcnow()),
            ))

    def list_dish_sales(self, venue_id, start_date, end_date):
        with self._cursor() as cur:
            self._ensure_table(cur, "dish_sales")
            cur.execute("""
                SELECT * FROM dish_sales
                WHERE venue_id = %s AND sale_date BETWEEN %s AND %s
                ORDER BY sale_date, recipe_name
            """, (venue_id, start_date, end_date))
            return [self._row_to_plain(r) for r in cur.fetchall()]

    def save_pos_item_map(self, m):
        with self._cursor() as cur:
            self._ensure_table(cur, "pos_item_maps")
            cur.execute("""
                INSERT INTO pos_item_maps (id, venue_id, normalized_name, display_name,
                    recipe_id, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, now())
                ON CONFLICT (id) DO UPDATE SET
                    recipe_id=EXCLUDED.recipe_id, display_name=EXCLUDED.display_name,
                    updated_at=now()
            """, (
                f"{m['venue_id']}:{m['normalized_name']}", m["venue_id"],
                m["normalized_name"], m.get("display_name"), m["recipe_id"],
                m.get("created_at", datetime.utcnow()),
            ))

    def list_pos_item_maps(self, venue_id):
        with self._cursor() as cur:
            self._ensure_table(cur, "pos_item_maps")
            cur.execute("""
                SELECT * FROM pos_item_maps WHERE venue_id = %s ORDER BY normalized_name
            """, (venue_id,))
            return [dict(r) for r in cur.fetchall()]

    def delete_pos_item_map(self, venue_id, normalized_name):
        with self._cursor() as cur:
            self._ensure_table(cur, "pos_item_maps")
            cur.execute("DELETE FROM pos_item_maps WHERE id = %s",
                        (f"{venue_id}:{normalized_name}",))

    def save_import_batch(self, batch):
        with self._cursor() as cur:
            self._ensure_table(cur, "sales_import_batches")
            cur.execute("""
                INSERT INTO sales_import_batches (id, venue_id, sale_date, row_count,
                    revenue, imported_at)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO NOTHING
            """, (
                batch["id"], batch["venue_id"], batch.get("sale_date"),
                batch.get("row_count", 0), float(batch.get("revenue", 0) or 0),
                batch.get("imported_at", datetime.utcnow()),
            ))

    def get_import_batch(self, batch_id):
        with self._cursor() as cur:
            self._ensure_table(cur, "sales_import_batches")
            cur.execute("SELECT * FROM sales_import_batches WHERE id = %s", (batch_id,))
            row = cur.fetchone()
            return dict(row) if row else None

    def update_stocktake_count(self, st_id, ingredient_id, counted):
        # Rewrites ONLY the matching item's count inside the JSONB array in one
        # statement — concurrent counters on different items can't clobber
        # each other the way read-modify-write of the whole row would.
        with self._cursor() as cur:
            self._ensure_table(cur, "stocktakes")
            cur.execute("""
                UPDATE stocktakes SET items = (
                    SELECT jsonb_agg(
                        CASE WHEN elem->>'ingredient_id' = %s
                             THEN jsonb_set(elem, '{counted}', to_jsonb(%s::numeric))
                             ELSE elem END)
                    FROM jsonb_array_elements(items) AS elem
                ), updated_at = now()
                WHERE id = %s AND status = 'open'
                  AND items @> %s::jsonb
            """, (ingredient_id, float(counted), st_id,
                  _json([{"ingredient_id": ingredient_id}])))
            return cur.rowcount > 0

    def get_employees(self, venue_id=None):
        """Active employees, optionally scoped to one venue (AI agent tools)."""
        with self._cursor() as cur:
            if venue_id is not None:
                cur.execute(
                    "SELECT * FROM employees WHERE active = true AND venue_id = %s ORDER BY name",
                    (venue_id,),
                )
            else:
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

        # Derive state: prefer venue config, then employee row, then fallback
        emp_state = None
        venue_id = row.get("venue_id")
        if venue_id:
            venue = self.get_venue(venue_id)
            if venue:
                emp_state = venue.state
        if emp_state is None and row.get("state"):
            try:
                emp_state = State(row["state"])
            except ValueError:
                emp_state = None
        if emp_state is None:
            logger.warning(
                "Employee %s has no venue or state configured, defaulting to 'vic'",
                row["id"],
            )
            emp_state = State("vic")

        return Employee(
            id=row["id"], tanda_id=row.get("tanda_id"),
            venue_id=venue_id,
            name=row["name"],
            employment_type=EmploymentType(row["employment_type"]),
            award_level=AwardLevel(row["award_level"]),
            state=emp_state,
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
            # Saving an existing roster id must be a real update — including its
            # week. (Previously only total_cost updated on conflict, so a
            # re-saved roster kept its old week_start/week_end forever.)
            cur.execute("""
                INSERT INTO rosters (id, venue_id, week_start, week_end, total_cost, created_at)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO UPDATE SET
                    venue_id=EXCLUDED.venue_id, week_start=EXCLUDED.week_start,
                    week_end=EXCLUDED.week_end, total_cost=EXCLUDED.total_cost
            """, (
                roster.id, roster.venue_id, roster.week_start, roster.week_end,
                float(roster.total_cost) if roster.total_cost else None,
                roster.created_at,
            ))
            # Save shifts — re-saving a shift id updates it (DO NOTHING silently
            # kept stale dates/times whenever a roster was re-saved).
            for shift in roster.shifts:
                cur.execute("""
                    INSERT INTO shifts (id, roster_id, employee_id, shift_date, start_time, end_time,
                        break_minutes, status, role, cost, penalty_multiplier)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (id) DO UPDATE SET
                        roster_id=EXCLUDED.roster_id, employee_id=EXCLUDED.employee_id,
                        shift_date=EXCLUDED.shift_date, start_time=EXCLUDED.start_time,
                        end_time=EXCLUDED.end_time, break_minutes=EXCLUDED.break_minutes,
                        status=EXCLUDED.status, role=EXCLUDED.role,
                        cost=EXCLUDED.cost, penalty_multiplier=EXCLUDED.penalty_multiplier
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
        # shifts.roster_id is NOT NULL, and Shift carries no roster_id — this
        # INSERT omitted the column entirely, so EVERY save_shift raised
        # NotNullViolation on Postgres (Deputy shift sync reported success while
        # storing nothing; bid award 500'd). Postgres evaluates NOT NULL before
        # the ON CONFLICT arbiter, so even a pure update went down with it.
        # Updates are the real use (status changes, outcome recording): carry
        # the existing roster_id through, and refuse a genuine insert loudly
        # instead of with a constraint error.
        with self._cursor() as cur:
            cur.execute("SELECT roster_id FROM shifts WHERE id = %s", (shift.id,))
            row = cur.fetchone()
            existing_roster = (row["roster_id"] if isinstance(row, dict) else row[0]) if row else None
            roster_id = getattr(shift, "roster_id", None) or existing_roster
            if not roster_id:
                raise ValueError(
                    f"Cannot save shift {shift.id}: it belongs to no roster. "
                    "Create the roster first (save_roster) — a shift row cannot "
                    "exist without one."
                )
            cur.execute("""
                INSERT INTO shifts
                    (id, roster_id, employee_id, shift_date, start_time, end_time,
                     break_minutes, status, role, cost, penalty_multiplier, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO UPDATE SET
                    employee_id=EXCLUDED.employee_id,
                    shift_date=EXCLUDED.shift_date,
                    start_time=EXCLUDED.start_time,
                    end_time=EXCLUDED.end_time,
                    break_minutes=EXCLUDED.break_minutes,
                    status=EXCLUDED.status,
                    role=EXCLUDED.role,
                    cost=EXCLUDED.cost,
                    penalty_multiplier=EXCLUDED.penalty_multiplier
            """, (
                shift.id,
                roster_id,
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

    def venue_id_for_shift(self, shift_id: str) -> Optional[str]:
        """Venue of a shift via shifts.roster_id -> rosters.venue_id."""
        with self._cursor() as cur:
            cur.execute(
                "SELECT r.venue_id FROM shifts s JOIN rosters r ON r.id = s.roster_id "
                "WHERE s.id = %s", (shift_id,))
            row = cur.fetchone()
            if row:
                return row[0] if not isinstance(row, dict) else row.get("venue_id")
        return None

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
        """Save Xero OAuth credentials (secret columns encrypted at rest).

        client_secret / access_token / refresh_token are routed through
        services.secret_box (Fernet) so they aren't persisted as plaintext —
        mirroring how plugin_installs.tokens are protected. Non-secret columns
        (client_id, tenant_id, token_expires) stay readable.
        """
        from rosteriq.services.secret_box import encrypt_tokens
        secrets = encrypt_tokens({
            "client_secret": credentials_dict.get("client_secret"),
            "access_token": credentials_dict.get("access_token"),
            "refresh_token": credentials_dict.get("refresh_token"),
        })
        with self._cursor() as cur:
            cur.execute("""
                INSERT INTO xero_credentials
                    (venue_id, client_id, client_secret, tenant_id, access_token,
                     refresh_token, token_expires, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (venue_id) DO UPDATE SET
                    client_id=EXCLUDED.client_id,
                    client_secret=EXCLUDED.client_secret,
                    access_token=EXCLUDED.access_token,
                    refresh_token=EXCLUDED.refresh_token,
                    token_expires=EXCLUDED.token_expires,
                    updated_at=EXCLUDED.updated_at
            """, (
                venue_id,
                credentials_dict.get("client_id"),
                secrets.get("client_secret"),
                credentials_dict.get("tenant_id"),
                secrets.get("access_token"),
                secrets.get("refresh_token"),
                credentials_dict.get("token_expires"),
                credentials_dict.get("created_at"),
                credentials_dict.get("updated_at"),
            ))

    def get_xero_credentials(self, venue_id: str) -> Optional[dict]:
        """Retrieve Xero credentials (secret columns decrypted)."""
        from rosteriq.services.secret_box import decrypt_tokens
        with self._cursor() as cur:
            cur.execute(
                "SELECT * FROM xero_credentials WHERE venue_id = %s",
                (venue_id,)
            )
            row = cur.fetchone()
            if row:
                secrets = decrypt_tokens({
                    "client_secret": row["client_secret"],
                    "access_token": row["access_token"],
                    "refresh_token": row["refresh_token"],
                })
                return {
                    "venue_id": row["venue_id"],
                    "client_id": row["client_id"],
                    "client_secret": secrets["client_secret"],
                    "tenant_id": row["tenant_id"],
                    "access_token": secrets["access_token"],
                    "refresh_token": secrets["refresh_token"],
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
                # webhook_events is keyed by webhook_id; there is no `id`
                # column, so this probe used to raise and every inbound Tanda
                # webhook was silently dropped by the caller's except.
                "SELECT webhook_id FROM webhook_events WHERE webhook_id = %s",
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
                INSERT INTO users (id, email, password_hash, name, role, api_key_hash, is_active, created_at, last_login, venue_ids)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO UPDATE SET
                    email=EXCLUDED.email, password_hash=EXCLUDED.password_hash,
                    name=EXCLUDED.name, role=EXCLUDED.role,
                    api_key_hash=EXCLUDED.api_key_hash, is_active=EXCLUDED.is_active,
                    last_login=EXCLUDED.last_login, venue_ids=EXCLUDED.venue_ids
            """, (
                user.get("id"), user.get("email"), user.get("password_hash"),
                user.get("name"), user.get("role"), user.get("api_key_hash", ""),
                user.get("is_active", True), user.get("created_at"), user.get("last_login"),
                _json(user.get("venue_ids", []) or []),
            ))

    @staticmethod
    def _normalize_user(row):
        """Row -> dict with venue_ids guaranteed to be a list (JSONB returns a
        list already, but be defensive about NULL / legacy string values)."""
        if row is None:
            return None
        u = dict(row)
        v = u.get("venue_ids")
        if v is None:
            u["venue_ids"] = []
        elif isinstance(v, str):
            try:
                u["venue_ids"] = json.loads(v)
            except Exception:
                u["venue_ids"] = []
        return u

    def get_user_by_email(self, email):
        """Get user by email."""
        with self._cursor() as cur:
            cur.execute("SELECT * FROM users WHERE email = %s", (email,))
            return self._normalize_user(cur.fetchone())

    def get_user_by_id(self, user_id):
        """Get user by ID."""
        with self._cursor() as cur:
            cur.execute("SELECT * FROM users WHERE id = %s", (user_id,))
            return self._normalize_user(cur.fetchone())

    def list_users(self):
        """List all users."""
        with self._cursor() as cur:
            cur.execute("SELECT * FROM users ORDER BY created_at DESC")
            return [self._normalize_user(row) for row in cur.fetchall()]

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
                INSERT INTO login_attempts (email, ip_address, success, attempted_at)
                VALUES (%s, %s, %s, %s)
            """, (email, ip_address, success, datetime.utcnow()))

    def check_login_rate_limit(self, ip_address, minutes=1):
        """Count failed login attempts from IP in last N minutes."""
        with self._cursor() as cur:
            cutoff = datetime.utcnow() - timedelta(minutes=minutes)
            cur.execute("""
                SELECT COUNT(*) as count FROM login_attempts
                WHERE ip_address = %s AND success = false AND attempted_at > %s
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
            """, (state.get("venue_id"), _json(state)))

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
                    return _jsonb(row["state_data"])
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
                _json(event.get("payload", {})),
                event.get("processed", False),
                event.get("created_at", datetime.utcnow()),
            ))

    def save_plugin_install(self, install: dict) -> None:
        """Save or update a plugin installation record (secrets encrypted at rest)."""
        from rosteriq.services.secret_box import encrypt_tokens
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
                _json(encrypt_tokens(install.get("tokens", {}))),
                install.get("installed_at", datetime.utcnow()),
                install.get("updated_at", datetime.utcnow()),
            ))

    def get_plugin_install(self, organisation_id: str) -> Optional[dict]:
        """Get plugin installation record by organisation ID (secrets decrypted)."""
        from rosteriq.services.secret_box import decrypt_install
        with self._cursor() as cur:
            cur.execute(
                "SELECT * FROM plugin_installs WHERE organisation_id = %s",
                (organisation_id,)
            )
            row = cur.fetchone()
            return decrypt_install(dict(row)) if row else None

    def list_plugin_installs(self) -> list[dict]:
        """List all plugin installations (secrets decrypted)."""
        from rosteriq.services.secret_box import decrypt_install
        with self._cursor() as cur:
            cur.execute("SELECT * FROM plugin_installs ORDER BY updated_at DESC")
            return [decrypt_install(dict(row)) for row in cur.fetchall()]

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
                _json({k: v for k, v in config.items()
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
                    # JSONB: already parsed by psycopg2 (json.loads on a dict
                    # raises TypeError and 500s the feeds page).
                    custom = _jsonb(cfg["custom_params"], {})
                    if isinstance(custom, dict):
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
                    custom = _jsonb(cfg["custom_params"], {})
                    cfg.update(custom)
                    del cfg["custom_params"]
                result.append(cfg)
            return result

    def save_roster_template(self, template: dict) -> None:
        """Save or update a roster template."""
        with self._cursor() as cur:
            self._ensure_table(cur, "roster_templates")
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
                _json(template.get("shift_patterns", []))
            ))

    def get_roster_template(self, template_id: str) -> Optional[dict]:
        """Get a roster template by ID."""
        with self._cursor() as cur:
            self._ensure_table(cur, "roster_templates")
            cur.execute("SELECT * FROM roster_templates WHERE id = %s", (template_id,))
            row = cur.fetchone()
            if not row:
                return None
            data = dict(row)
            # JSONB — already a list when it comes back from psycopg2
            data["shift_patterns"] = _jsonb(data.get("shift_patterns"), [])
            return data

    def list_roster_templates(self, venue_id: str) -> list[dict]:
        """List all roster templates for a venue."""
        with self._cursor() as cur:
            self._ensure_table(cur, "roster_templates")
            cur.execute(
                "SELECT * FROM roster_templates WHERE venue_id = %s ORDER BY created_at DESC",
                (venue_id,)
            )
            result = []
            for row in cur.fetchall():
                data = dict(row)
                if data.get("shift_patterns"):
                    data["shift_patterns"] = _jsonb(data.get("shift_patterns"), [])
                result.append(data)
            return result

    def delete_roster_template(self, template_id: str) -> None:
        """Delete a roster template by ID."""
        with self._cursor() as cur:
            self._ensure_table(cur, "roster_templates")
            cur.execute("DELETE FROM roster_templates WHERE id = %s", (template_id,))

    def save_password_reset_token(self, token_hash: str, user_id: str, expires_at: datetime) -> None:
        """Save a password reset token."""
        with self._cursor() as cur:
            self._ensure_table(cur, "password_reset_tokens")
            cur.execute("""
                INSERT INTO password_reset_tokens (token_hash, user_id, expires_at, created_at)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (token_hash) DO UPDATE SET
                    expires_at=EXCLUDED.expires_at
            """, (token_hash, user_id, expires_at, datetime.utcnow()))

    def get_password_reset_token(self, token_hash: str) -> Optional[dict]:
        """Get a password reset token by hash."""
        with self._cursor() as cur:
            self._ensure_table(cur, "password_reset_tokens")
            cur.execute(
                "SELECT * FROM password_reset_tokens WHERE token_hash = %s",
                (token_hash,)
            )
            row = cur.fetchone()
            return dict(row) if row else None

    def delete_password_reset_token(self, token_hash: str) -> None:
        """Delete a password reset token."""
        with self._cursor() as cur:
            self._ensure_table(cur, "password_reset_tokens")
            cur.execute(
                "DELETE FROM password_reset_tokens WHERE token_hash = %s",
                (token_hash,)
            )

    def save_email_verification_token(self, token_hash: str, user_id: str, expires_at: datetime) -> None:
        """Save an email verification token."""
        with self._cursor() as cur:
            self._ensure_table(cur, "email_verification_tokens")
            cur.execute("""
                INSERT INTO email_verification_tokens (token_hash, user_id, expires_at, created_at)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (token_hash) DO UPDATE SET
                    expires_at=EXCLUDED.expires_at
            """, (token_hash, user_id, expires_at, datetime.utcnow()))

    def get_email_verification_token(self, token_hash: str) -> Optional[dict]:
        """Get an email verification token by hash."""
        with self._cursor() as cur:
            self._ensure_table(cur, "email_verification_tokens")
            cur.execute(
                "SELECT * FROM email_verification_tokens WHERE token_hash = %s",
                (token_hash,)
            )
            row = cur.fetchone()
            return dict(row) if row else None

    def delete_email_verification_token(self, token_hash: str) -> None:
        """Delete an email verification token."""
        with self._cursor() as cur:
            self._ensure_table(cur, "email_verification_tokens")
            cur.execute(
                "DELETE FROM email_verification_tokens WHERE token_hash = %s",
                (token_hash,)
            )

    def save_webhook_subscription(self, subscription: dict) -> None:
        """Save or update a webhook subscription."""
        with self._cursor() as cur:
            self._ensure_table(cur, "webhook_subscriptions")
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
                _json(subscription.get("events", [])),
                subscription.get("secret"),
                subscription.get("active", True),
                subscription.get("created_at"),
                subscription.get("updated_at"),
            ))

    def get_webhook_subscription(self, subscription_id: str) -> Optional[dict]:
        """Get a webhook subscription by ID."""
        with self._cursor() as cur:
            self._ensure_table(cur, "webhook_subscriptions")
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
            self._ensure_table(cur, "webhook_subscriptions")
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
            self._ensure_table(cur, "webhook_subscriptions")
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
        """
        Save a webhook delivery record.

        Persists the FULL delivery (url, payload, headers, venue_id, attempt)
        so a queued webhook survives a restart and can be redelivered — the
        previous version dropped those columns, making durable retry impossible.
        """
        with self._cursor() as cur:
            cur.execute("""
                INSERT INTO webhook_deliveries
                    (id, subscription_id, venue_id, event_type, url, payload,
                     headers, status, attempt, attempts, response_code, error,
                     last_attempt_at, next_retry_at, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO UPDATE SET
                    status=EXCLUDED.status,
                    attempt=EXCLUDED.attempt,
                    attempts=EXCLUDED.attempts,
                    response_code=EXCLUDED.response_code,
                    error=EXCLUDED.error,
                    last_attempt_at=EXCLUDED.last_attempt_at,
                    next_retry_at=EXCLUDED.next_retry_at
            """, (
                delivery.get("id"),
                delivery.get("subscription_id"),
                delivery.get("venue_id"),
                delivery.get("event_type"),
                delivery.get("url"),
                _json(delivery.get("payload")) if delivery.get("payload") is not None else None,
                _json(delivery.get("headers")) if delivery.get("headers") is not None else None,
                delivery.get("status"),
                delivery.get("attempt", 0),
                _json(delivery.get("attempts", [])),
                delivery.get("response_code"),
                delivery.get("error"),
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

    @staticmethod
    def _deserialize_webhook_row(row) -> dict:
        """Normalise a webhook_deliveries row — ensure JSON fields are Python
        objects (defensive: psycopg2 usually parses JSONB, but never rely on it
        for the payload/headers the queue needs to redeliver)."""
        d = dict(row)
        for field in ("payload", "headers", "attempts"):
            val = d.get(field)
            if isinstance(val, (str, bytes)):
                try:
                    d[field] = json.loads(val)
                except (ValueError, TypeError):
                    pass
        return d

    def get_webhook_delivery(self, delivery_id: str) -> Optional[dict]:
        """Get a webhook delivery record by ID."""
        with self._cursor() as cur:
            cur.execute(
                "SELECT * FROM webhook_deliveries WHERE id = %s",
                (delivery_id,)
            )
            row = cur.fetchone()
            return self._deserialize_webhook_row(row) if row else None

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
            return [self._deserialize_webhook_row(row) for row in cur.fetchall()]

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
                _json(dead_letter.get("payload", {})),
                _json(dead_letter.get("headers", {})),
                dead_letter.get("venue_id"),
                dead_letter.get("subscription_id"),
                dead_letter.get("event_type"),
                dead_letter.get("status"),
                dead_letter.get("attempt", 0),
                _json(dead_letter.get("attempts", [])),
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
            self._ensure_table(cur, "shift_swaps")
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
            self._ensure_table(cur, "shift_swaps")
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
            self._ensure_table(cur, "shift_swaps")
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
                RETURNING webhook_id
            """, (before_date,))
            return len(cur.fetchall())

    def purge_old_login_attempts(self, before_date: datetime) -> int:
        """Delete login attempts older than the specified date. Returns count deleted."""
        with self._cursor() as cur:
            cur.execute("""
                DELETE FROM login_attempts
                WHERE attempted_at < %s
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
            self._ensure_table(cur, "privacy_consents")
            cur.execute("""
                INSERT INTO privacy_consents (user_id, consent_type, granted, timestamp)
                VALUES (%s, %s, %s, %s)
            """, (user_id, consent_type, granted, timestamp))

    def get_consents(self, user_id: str) -> list[dict]:
        """Get all consent records for a user."""
        with self._cursor() as cur:
            self._ensure_table(cur, "privacy_consents")
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
            self._ensure_table(cur, "privacy_audit_log")
            cur.execute("""
                INSERT INTO privacy_audit_log (user_id, action, resource_type, details, logged_at)
                VALUES (%s, %s, %s, %s, %s)
            """, (
                entry.get('user_id'),
                entry.get('action'),
                entry.get('resource_type'),
                _json(entry.get('details', {})),
                entry.get('logged_at', datetime.utcnow()),
            ))

    def list_privacy_logs(self, user_id: Optional[str] = None, limit: int = 100) -> list[dict]:
        """List privacy audit logs, optionally filtered by user_id."""
        with self._cursor() as cur:
            self._ensure_table(cur, "privacy_audit_log")
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
            self._ensure_table(cur, "anonymised_employees")
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
            self._ensure_table(cur, "anonymised_employees")
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

            # Materialise the rosters first so the cursor is free to re-query
            # shifts per roster below.
            roster_rows = cur.fetchall()
            rosters = []
            for row in roster_rows:
                # Reconstruct Roster with shifts. Use _row_to_shift so the column
                # mapping (notably shift_date, not 'date') stays correct and DRY.
                cur.execute(
                    "SELECT * FROM shifts WHERE roster_id = %s ORDER BY shift_date, start_time",
                    (row['id'],),
                )
                shifts = [self._row_to_shift(s) for s in cur.fetchall()]

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
            self._ensure_table(cur, "revenue_snapshots")
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
            self._ensure_table(cur, "analytics_snapshots")
            cur.execute("""
                INSERT INTO analytics_snapshots
                    (venue_id, date, metric_type, value, created_at)
                VALUES (%s, %s, %s, %s, %s)
            """, (
                snapshot['venue_id'],
                snapshot.get('date'),
                snapshot.get('metric_type'),
                _json(snapshot.get('value')),
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
            self._ensure_table(cur, "analytics_snapshots")
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
                    "value": _jsonb(row['value']),
                    "created_at": row['created_at'],
                })

            return results

    def save_audit_log(self, entry: dict) -> None:
        """Save an audit log entry."""
        with self._cursor() as cur:
            self._ensure_table(cur, "audit_logs")
            cur.execute("""
                INSERT INTO audit_logs (venue_id, user_id, action, resource_type, resource_id, details, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (
                entry.get('venue_id'),
                entry.get('user_id'),
                entry.get('action'),
                entry.get('resource_type'),
                entry.get('resource_id'),
                _json(entry.get('details', {})),
                entry.get('created_at', datetime.utcnow()),
            ))

    def list_audit_logs(
        self, venue_id: str, limit: int = 100, offset: int = 0
    ) -> list[dict]:
        """List audit logs for a venue."""
        with self._cursor() as cur:
            self._ensure_table(cur, "audit_logs")
            cur.execute("""
                SELECT id, venue_id, user_id, action, resource_type, resource_id, details, created_at
                FROM audit_logs
                WHERE venue_id = %s
                ORDER BY created_at DESC
                LIMIT %s OFFSET %s
            """, (venue_id, limit, offset))

            return [self._event_row(row) for row in cur.fetchall()]

    @staticmethod
    def _event_row(row) -> dict:
        # psycopg2 already parses JSONB to a dict; only json.loads a raw string
        # (the earlier unconditional json.loads(dict) raised TypeError).
        result = dict(row)
        d = result.get("details")
        if isinstance(d, str):
            try:
                result["details"] = json.loads(d)
            except Exception:
                result["details"] = {"raw": d}
        return result

    def list_events(self, venue_id=None, category=None, action_prefix=None,
                    since=None, limit=100, offset=0):
        clauses, params = [], []
        if venue_id is not None:
            clauses.append("venue_id = %s"); params.append(venue_id)
        if category:
            clauses.append("details->>'category' = %s"); params.append(category)
        if action_prefix:
            clauses.append("action LIKE %s"); params.append(action_prefix + "%")
        if since is not None:
            clauses.append("created_at >= %s"); params.append(since)
        where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
        with self._cursor() as cur:
            self._ensure_table(cur, "audit_logs")
            cur.execute(f"""
                SELECT id, venue_id, user_id, action, resource_type, resource_id, details, created_at
                FROM audit_logs {where}
                ORDER BY created_at DESC
                LIMIT %s OFFSET %s
            """, (*params, limit, offset))
            return [self._event_row(row) for row in cur.fetchall()]

    # Keys that may be grouped on. Whitelisted because the value is
    # interpolated into the SQL as a JSON key, never taken raw from a caller.
    _ROLLUP_KEYS = ("action", "fingerprint", "route", "provider", "job",
                    "outcome", "exception", "model")

    def event_rollup(self, since, venue_id=None, category=None, group_by="action",
                     limit=20):
        if group_by not in self._ROLLUP_KEYS:
            group_by = "action"
        expr = "action" if group_by == "action" else "details->>%s"
        clauses, params = ["created_at >= %s"], [since]
        if venue_id is not None:
            clauses.append("venue_id = %s"); params.append(venue_id)
        if category:
            clauses.append("details->>'category' = %s"); params.append(category)
        where = " AND ".join(clauses)
        # Grouping key first in the param list when it is a JSON key
        head = [] if group_by == "action" else [group_by]
        with self._cursor() as cur:
            self._ensure_table(cur, "audit_logs")
            cur.execute(f"""
                SELECT {expr} AS key,
                       COUNT(*) AS count,
                       COUNT(*) FILTER (
                           WHERE details->>'outcome' IN ('failed','error','denied')
                       ) AS failures,
                       MAX(created_at) AS last_seen,
                       -- Guarded cast: ONE legacy row with a non-numeric
                       -- duration must not fail the whole health query.
                       ROUND(PERCENTILE_DISC(0.95) WITHIN GROUP (
                           ORDER BY CASE WHEN details->>'duration_ms' ~ '^[0-9]+([.][0-9]+)?$'
                                    THEN (details->>'duration_ms')::numeric END
                       ), 1) AS p95_ms,
                       ROUND(MAX(CASE WHEN details->>'duration_ms' ~ '^[0-9]+([.][0-9]+)?$'
                                 THEN (details->>'duration_ms')::numeric END), 1) AS max_ms,
                       (ARRAY_AGG(details ORDER BY created_at DESC))[1] AS sample
                FROM audit_logs
                WHERE {where}
                  AND {expr} IS NOT NULL
                GROUP BY 1
                ORDER BY count DESC
                LIMIT %s
            """, (*head, *params, *head, limit))
            out = []
            for row in cur.fetchall():
                r = dict(row)
                sample = r.get("sample")
                if isinstance(sample, str):
                    try:
                        sample = json.loads(sample)
                    except Exception:
                        sample = {"raw": sample}
                r["sample"] = sample
                for k in ("p95_ms", "max_ms"):
                    if r.get(k) is not None:
                        r[k] = float(r[k])
                out.append(r)
            return out

    def ping(self):
        with self._cursor() as cur:
            cur.execute("SELECT 1")
            return cur.fetchone() is not None

    def prune_events(self, before):
        with self._cursor() as cur:
            self._ensure_table(cur, "audit_logs")
            cur.execute("DELETE FROM audit_logs WHERE created_at < %s", (before,))
            return cur.rowcount or 0

    # --- White-Label Theming ---

    def save_theme(self, venue_id: str, theme: dict) -> None:
        """Save or update a theme configuration for a venue."""
        with self._cursor() as cur:
            self._ensure_table(cur, "themes")
            cur.execute("""
                INSERT INTO themes (venue_id, config)
                VALUES (%s, %s)
                ON CONFLICT (venue_id) DO UPDATE SET config = EXCLUDED.config, updated_at = now()
            """, (venue_id, _json(theme)))

    def get_theme(self, venue_id: str) -> Optional[dict]:
        """Get a theme configuration for a venue. Returns None if not set."""
        with self._cursor() as cur:
            self._ensure_table(cur, "themes")
            cur.execute("""
                SELECT config FROM themes WHERE venue_id = %s
            """, (venue_id,))

            row = cur.fetchone()
            if row:
                return _jsonb(row["config"])
            return None

    def delete_theme(self, venue_id: str) -> None:
        """Delete a theme configuration, resetting to defaults."""
        with self._cursor() as cur:
            self._ensure_table(cur, "themes")
            cur.execute("""
                DELETE FROM themes WHERE venue_id = %s
            """, (venue_id,))

    # --- Credential Management (API Keys & Webhook Secrets) ---

    def save_api_key_record(self, record: dict) -> None:
        """Save or update an API key record."""
        with self._cursor() as cur:
            self._ensure_table(cur, "api_key_records")
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
            self._ensure_table(cur, "api_key_records")
            cur.execute("""
                SELECT * FROM api_key_records
                WHERE user_id = %s
                ORDER BY created_at DESC
            """, (user_id,))
            return [dict(row) for row in cur.fetchall()]

    def get_api_key_record(self, key_id: str) -> Optional[dict]:
        """Get an API key record by key ID."""
        with self._cursor() as cur:
            self._ensure_table(cur, "api_key_records")
            cur.execute(
                "SELECT * FROM api_key_records WHERE id = %s",
                (key_id,)
            )
            row = cur.fetchone()
            return dict(row) if row else None

    def save_webhook_secret(self, venue_id: str, secret_record: dict) -> None:
        """Save or update a webhook secret record."""
        with self._cursor() as cur:
            self._ensure_table(cur, "webhook_secrets")
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
            self._ensure_table(cur, "webhook_secrets")
            cur.execute("""
                SELECT * FROM webhook_secrets
                WHERE venue_id = %s
                ORDER BY created_at DESC
            """, (venue_id,))
            return [dict(row) for row in cur.fetchall()]

    def save_preference_profile(self, employee_id: str, profile: dict) -> None:
        """Save or update a preference profile for an employee."""
        with self._cursor() as cur:
            self._ensure_table(cur, "preference_profiles")
            cur.execute("""
                INSERT INTO preference_profiles (employee_id, profile_data, updated_at)
                VALUES (%s, %s, %s)
                ON CONFLICT (employee_id) DO UPDATE
                SET profile_data = EXCLUDED.profile_data, updated_at = EXCLUDED.updated_at
            """, (employee_id, _json(profile), datetime.utcnow()))

    def get_preference_profile(self, employee_id: str) -> Optional[dict]:
        """Get a preference profile for an employee. Returns None if not found."""
        with self._cursor() as cur:
            self._ensure_table(cur, "preference_profiles")
            cur.execute("""
                SELECT profile_data FROM preference_profiles
                WHERE employee_id = %s
            """, (employee_id,))
            row = cur.fetchone()
            if row:
                return _jsonb(row['profile_data'])
            return None

    def list_preference_profiles(self, venue_id: str) -> list[dict]:
        """List all preference profiles for a venue."""
        with self._cursor() as cur:
            self._ensure_table(cur, "preference_profiles")
            cur.execute("""
                SELECT profile_data FROM preference_profiles
                WHERE profile_data->>'venue_id' = %s
                ORDER BY updated_at DESC
            """, (venue_id,))
            return [_jsonb(row['profile_data']) for row in cur.fetchall()]


    # --- A/B Testing ---

    def save_experiment(self, experiment: dict) -> None:
        """Save or update an experiment."""
        with self._cursor() as cur:
            self._ensure_table(cur, "ab_experiments")
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
                _json(experiment.get("control_venues", [])),
                _json(experiment.get("variant_venues", [])),
                experiment.get("minimum_sample_size", 30),
            ))

    def get_experiment(self, experiment_id: str) -> Optional[dict]:
        """Get an experiment by ID."""
        with self._cursor() as cur:
            self._ensure_table(cur, "ab_experiments")
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
            self._ensure_table(cur, "ab_experiments")
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
            self._ensure_table(cur, "ab_experiment_outcomes")
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
            self._ensure_table(cur, "ab_experiment_outcomes")
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
            self._ensure_table(cur, "payroll_batches")
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
                _json(batch),
                batch.get("created_at", datetime.utcnow().isoformat()),
            ))

    def get_payroll_batch(self, batch_id: str) -> Optional[dict]:
        """Get a payroll batch by ID."""
        with self._cursor() as cur:
            self._ensure_table(cur, "payroll_batches")
            cur.execute("""
                SELECT data FROM payroll_batches
                WHERE batch_id = %s
            """, (batch_id,))
            row = cur.fetchone()
            if row:
                # JSONB columns come back already parsed; TEXT comes back raw
                data = row["data"]
                return data if isinstance(data, dict) else json.loads(data)
        return None

    def list_payroll_batches(self, venue_id: str) -> list[dict]:
        """List all payroll batches for a venue."""
        with self._cursor() as cur:
            self._ensure_table(cur, "payroll_batches")
            cur.execute("""
                SELECT data FROM payroll_batches
                WHERE venue_id = %s
                ORDER BY created_at DESC
            """, (venue_id,))
            results = []
            for row in cur.fetchall():
                # JSONB columns come back already parsed; TEXT comes back raw
                data = row["data"]
                results.append(data if isinstance(data, (dict, list)) else json.loads(data))
            return results

    def save_payroll_export(self, export: dict) -> None:
        """Record a payroll export to external service."""
        with self._cursor() as cur:
            self._ensure_table(cur, "payroll_exports")
            cur.execute("""
                INSERT INTO payroll_exports (batch_id, service, status, data, exported_at)
                VALUES (%s, %s, %s, %s, %s)
            """, (
                export.get("batch_id"),
                export.get("service"),
                export.get("status", "success"),
                _json(export),
                export.get("exported_at", datetime.utcnow().isoformat()),
            ))

    def list_payroll_exports(self, venue_id: str, limit: int = 50) -> list[dict]:
        """List payroll exports for a venue."""
        with self._cursor() as cur:
            self._ensure_table(cur, "payroll_batches")
            self._ensure_table(cur, "payroll_exports")
            cur.execute("""
                SELECT pe.data FROM payroll_exports pe
                JOIN payroll_batches pb ON pe.batch_id = pb.batch_id
                WHERE pb.venue_id = %s
                ORDER BY pe.exported_at DESC
                LIMIT %s
            """, (venue_id, limit))
            results = []
            for row in cur.fetchall():
                # JSONB columns come back already parsed; TEXT comes back raw
                data = row["data"]
                results.append(data if isinstance(data, (dict, list)) else json.loads(data))
            return results

    # --- Notification Preferences ---

    def save_notification_preferences(self, user_id: str, prefs: dict) -> None:
        """Save or update notification preferences for a user."""
        with self._cursor() as cur:
            self._ensure_table(cur, "notification_preferences")
            cur.execute("""
                INSERT INTO notification_preferences (user_id, preferences, created_at, updated_at)
                VALUES (%s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                ON CONFLICT (user_id) DO UPDATE
                SET preferences=EXCLUDED.preferences, updated_at=CURRENT_TIMESTAMP
            """, (user_id, _json(prefs)))

    def get_notification_preferences(self, user_id: str) -> Optional[dict]:
        """Get notification preferences for a user."""
        with self._cursor() as cur:
            self._ensure_table(cur, "notification_preferences")
            cur.execute("""
                SELECT preferences FROM notification_preferences WHERE user_id = %s
            """, (user_id,))
            row = cur.fetchone()
            if row:
                return _jsonb(row["preferences"])
        return None

    # --- Approval Workflow ---

    def save_approval_request(self, request: dict) -> None:
        """Save or update an approval request."""
        with self._cursor() as cur:
            self._ensure_table(cur, "approval_requests")
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
                _json(request.get("auto_approved_by_rules", [])),
                _json(request.get("failed_rules", [])),
                _json(request),
                datetime.utcnow(),
            ))

    def get_approval_request(self, request_id: str) -> Optional[dict]:
        """Get an approval request by ID."""
        with self._cursor() as cur:
            self._ensure_table(cur, "approval_requests")
            cur.execute("""
                SELECT data FROM approval_requests WHERE request_id = %s
            """, (request_id,))
            row = cur.fetchone()
            if row:
                return _jsonb(row["data"])
        return None

    def list_approval_requests(
        self,
        venue_id: Optional[str] = None,
        roster_id: Optional[str] = None,
        status: Optional[str] = None,
    ) -> list[dict]:
        """List approval requests with optional filters."""
        with self._cursor() as cur:
            self._ensure_table(cur, "approval_requests")
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
                # JSONB columns come back already parsed; TEXT comes back raw
                data = row["data"]
                results.append(data if isinstance(data, (dict, list)) else json.loads(data))
            return results

    # --- Roster publishing state machine ---

    def get_roster_state(self, roster_id: str) -> str:
        with self._cursor() as cur:
            self._ensure_table(cur, "roster_states")
            cur.execute("SELECT state FROM roster_states WHERE roster_id = %s", (roster_id,))
            row = cur.fetchone()
            return row["state"] if row else "draft"

    def update_roster_state(self, roster_id: str, new_state: str, reason: str,
                            actor_id: str = "system") -> None:
        with self._cursor() as cur:
            self._ensure_table(cur, "roster_states")
            self._ensure_table(cur, "roster_state_history")
            cur.execute("""
                INSERT INTO roster_states (roster_id, state, updated_at)
                VALUES (%s, %s, CURRENT_TIMESTAMP)
                ON CONFLICT (roster_id) DO UPDATE SET state = EXCLUDED.state,
                    updated_at = CURRENT_TIMESTAMP
            """, (roster_id, new_state))
            cur.execute("""
                INSERT INTO roster_state_history (roster_id, state, reason, actor_id)
                VALUES (%s, %s, %s, %s)
            """, (roster_id, new_state, reason, actor_id))

    def get_roster_state_history(self, roster_id: str) -> list[dict]:
        with self._cursor() as cur:
            self._ensure_table(cur, "roster_state_history")
            cur.execute("""
                SELECT roster_id, state, reason, actor_id, at FROM roster_state_history
                WHERE roster_id = %s ORDER BY at ASC, id ASC
            """, (roster_id,))
            out = []
            for row in cur.fetchall():
                d = dict(row)
                if hasattr(d.get("at"), "isoformat"):
                    d["at"] = d["at"].isoformat()
                out.append(d)
            return out

    def save_publication_event(self, event: dict) -> None:
        with self._cursor() as cur:
            self._ensure_table(cur, "publication_events")
            cur.execute(
                "INSERT INTO publication_events (venue_id, event) VALUES (%s, %s)",
                (event.get("venue_id"), json.dumps(event, default=str)),
            )

    def get_publication_history(self, venue_id: str, limit: int = 50) -> list[dict]:
        with self._cursor() as cur:
            self._ensure_table(cur, "publication_events")
            cur.execute("""
                SELECT event FROM publication_events WHERE venue_id = %s
                ORDER BY created_at DESC LIMIT %s
            """, (venue_id, limit))
            return [json.loads(row["event"]) if isinstance(row["event"], str) else row["event"]
                    for row in cur.fetchall()]

    def save_roster_revision(self, revision: dict) -> None:
        """Save a roster revision with change tracking."""
        import uuid
        with self._cursor() as cur:
            self._ensure_table(cur, "roster_revisions")
            cur.execute("""
                INSERT INTO roster_revisions (
                    revision_id, roster_id, revision_number, changes, created_at, data
                ) VALUES (%s, %s, %s, %s, %s, %s)
            """, (
                revision.get("id", str(uuid.uuid4())),
                revision.get("roster_id"),
                revision.get("revision_number"),
                _json(revision.get("changes", {})),
                revision.get("created_at", datetime.utcnow()),
                _json(revision),
            ))

    def list_roster_revisions(self, roster_id: str) -> list[dict]:
        """List all revisions for a roster."""
        with self._cursor() as cur:
            self._ensure_table(cur, "roster_revisions")
            cur.execute("""
                SELECT data FROM roster_revisions
                WHERE roster_id = %s
                ORDER BY revision_number ASC
            """, (roster_id,))
            results = []
            for row in cur.fetchall():
                # JSONB columns come back already parsed; TEXT comes back raw
                data = row["data"]
                results.append(data if isinstance(data, (dict, list)) else json.loads(data))
            return results

    # --- Push Notifications ---

    def save_push_subscription(self, user_id: str, subscription: dict) -> None:
        """Save or update a push notification subscription for a user."""
        with self._cursor() as cur:
            self._ensure_table(cur, "push_subscriptions")
            cur.execute("""
                INSERT INTO push_subscriptions (user_id, venue_id, subscription_data, created_at, updated_at)
                VALUES (%s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                ON CONFLICT (user_id) DO UPDATE
                SET subscription_data=EXCLUDED.subscription_data, updated_at=CURRENT_TIMESTAMP
            """, (user_id, subscription.get("venue_id"), _json(subscription)))

    def get_push_subscription(self, user_id: str) -> Optional[dict]:
        """Get push notification subscription for a user."""
        with self._cursor() as cur:
            self._ensure_table(cur, "push_subscriptions")
            cur.execute("""
                SELECT subscription_data FROM push_subscriptions WHERE user_id = %s
            """, (user_id,))
            row = cur.fetchone()
            if row:
                return _jsonb(row["subscription_data"])
        return None

    def delete_push_subscription(self, user_id: str) -> None:
        """Delete push notification subscription for a user."""
        with self._cursor() as cur:
            self._ensure_table(cur, "push_subscriptions")
            cur.execute("""
                DELETE FROM push_subscriptions WHERE user_id = %s
            """, (user_id,))

    def list_push_subscriptions(self, venue_id: str) -> list[dict]:
        """List all push subscriptions for staff at a venue."""
        with self._cursor() as cur:
            self._ensure_table(cur, "push_subscriptions")
            cur.execute("""
                SELECT subscription_data FROM push_subscriptions WHERE venue_id = %s
            """, (venue_id,))
            results = []
            for row in cur.fetchall():
                results.append(_jsonb(row["subscription_data"]))
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
            """, (venue_id, _json(model)))

    def get_revenue_model(self, venue_id: str) -> Optional[dict]:
        """Get trained revenue model for a venue."""
        with self._cursor() as cur:
            cur.execute("""
                SELECT model_data FROM revenue_models
                WHERE venue_id = %s
            """, (venue_id,))
            row = cur.fetchone()
            if row:
                return _jsonb(row['model_data'])
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
            """, (venue_id, date, _json(revenue)))

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
                record = _jsonb(row['revenue_data'])
                record['venue_id'] = row['venue_id']
                record['date'] = row['date'].isoformat() if hasattr(row['date'], 'isoformat') else row['date']
                results.append(record)
            return results

    def save_direct_bookings(self, venue_id: str, bookings: list[dict]) -> int:
        with self._cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS direct_bookings (
                    id SERIAL PRIMARY KEY,
                    venue_id TEXT,
                    booking_date DATE,
                    party_size NUMERIC DEFAULT 0,
                    booking_time TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            # Idempotent: skip exact-duplicate rows so re-uploading the same CSV
            # doesn't double-count. Pre-read existing keys for this venue.
            cur.execute(
                "SELECT booking_date, party_size, booking_time FROM direct_bookings WHERE venue_id = %s",
                (venue_id,),
            )
            existing = {
                (str(r["booking_date"]), float(r["party_size"]) if r["party_size"] is not None else 0.0,
                 r["booking_time"])
                for r in cur.fetchall()
            }
            stored = 0
            for b in bookings or []:
                d = b.get("date")
                if not d:
                    continue
                party = float(b.get("party_size") or b.get("covers") or 0)
                time_v = b.get("time")
                key = (str(d), party, time_v)
                if key in existing:
                    continue
                cur.execute("""
                    INSERT INTO direct_bookings (venue_id, booking_date, party_size, booking_time)
                    VALUES (%s, %s, %s, %s)
                """, (venue_id, d, party, time_v))
                existing.add(key)
                stored += 1
            return stored

    def get_direct_bookings(
        self, venue_id: str, start: str, end: str
    ) -> list[dict]:
        with self._cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS direct_bookings (
                    id SERIAL PRIMARY KEY,
                    venue_id TEXT,
                    booking_date DATE,
                    party_size NUMERIC DEFAULT 0,
                    booking_time TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            cur.execute("""
                SELECT booking_date, party_size, booking_time FROM direct_bookings
                WHERE venue_id = %s AND booking_date BETWEEN %s AND %s
                ORDER BY booking_date ASC
            """, (venue_id, start, end))
            results = []
            for row in cur.fetchall():
                bd = row['booking_date']
                results.append({
                    "venue_id": venue_id,
                    "date": bd.isoformat() if hasattr(bd, 'isoformat') else bd,
                    "party_size": float(row['party_size']) if row['party_size'] is not None else 0,
                    "time": row['booking_time'],
                })
            return results

    def count_direct_bookings(self, venue_id: str) -> int:
        with self._cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS direct_bookings (
                    id SERIAL PRIMARY KEY,
                    venue_id TEXT,
                    booking_date DATE,
                    party_size NUMERIC DEFAULT 0,
                    booking_time TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            cur.execute("SELECT COUNT(*) AS c FROM direct_bookings WHERE venue_id = %s", (venue_id,))
            row = cur.fetchone()
            return int(row['c']) if row else 0

    # --- Shift Bidding Marketplace ---

    def save_open_shift(self, shift: dict) -> None:
        """Save or update an open shift."""
        with self._cursor() as cur:
            self._ensure_table(cur, "open_shifts")
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
                _json(shift.get("skills_required", [])),
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
            self._ensure_table(cur, "open_shifts")
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
                "skills_required": _jsonb(row["skills_required"], []),
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
            self._ensure_table(cur, "open_shifts")
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
                    "skills_required": _jsonb(row["skills_required"], []),
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
            self._ensure_table(cur, "bids")
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
            self._ensure_table(cur, "bids")
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
            self._ensure_table(cur, "bids")
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
        allow_fallback = os.environ.get("ALLOW_MEMORY_FALLBACK", "").lower() == "true"
        if DATABASE_URL:
            try:
                _instance = PostgresStore(DATABASE_URL)
                logger.info("Using PostgreSQL store")
            except Exception as e:
                if allow_fallback:
                    logger.warning(
                        "PostgreSQL unavailable (%s), falling back to in-memory "
                        "(ALLOW_MEMORY_FALLBACK=true)", e,
                    )
                    _instance = MemoryStore()
                else:
                    raise RuntimeError(
                        "DATABASE_URL is configured but PostgreSQL connection failed. "
                        "Refusing to fall back to MemoryStore in production. "
                        f"Original error: {e}"
                    ) from e
        else:
            logger.warning("No DATABASE_URL set — using in-memory store (dev/demo only)")
            _instance = MemoryStore()
    return _instance


def reset_db():
    """Reset the database instance (for testing)."""
    global _instance
    _instance = None

# TEMPORARY: These methods will be moved to MemoryStore.__init__ in next update
