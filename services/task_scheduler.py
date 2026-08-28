"""
RosterIQ Async Task Scheduler

Lightweight background task scheduler using asyncio for recurring jobs.
Runs inside the FastAPI process without external dependencies like Celery or APScheduler.

Supported job types:
- daily_digest: Send morning digest emails to venue managers
- cert_expiry_check: Check for certifications expiring within 14 days
- revenue_sync: Sync revenue data from Xero
- variance_monitor: Check shift actuals vs roster plan
"""

import asyncio
import logging
from datetime import datetime, timedelta, time
from typing import Dict, Optional, Callable, Any
from enum import Enum

from rosteriq.database import get_db
from rosteriq.services.notifications import get_notification_service


logger = logging.getLogger(__name__)


def _record_job(job_id, job, outcome, duration_ms, **extra):
    """Best-effort durable record of one scheduled run. Never raises."""
    try:
        from rosteriq.services.events import job as _job_event
        _job_event(str(getattr(job, "job_type", "") or job_id),
                   outcome=outcome, duration_ms=duration_ms,
                   job_id=job_id, **extra)
    except Exception:  # noqa: BLE001 — recording must never break the scheduler
        pass


class JobType(str, Enum):
    """Supported background job types."""
    DAILY_DIGEST = "daily_digest"
    WEEKLY_DIGEST = "weekly_digest"
    CERT_EXPIRY_CHECK = "cert_expiry_check"
    REVENUE_SYNC = "revenue_sync"
    VARIANCE_MONITOR = "variance_monitor"
    EVENT_RETENTION = "event_retention"


class JobConfig:
    """Configuration for a scheduled job."""

    def __init__(
        self,
        job_type: JobType,
        venue_id: Optional[str] = None,
        enabled: bool = True,
        run_time: Optional[time] = None,
        interval_hours: Optional[int] = None,
        next_run: Optional[datetime] = None,
    ):
        """
        Initialize job configuration.

        Args:
            job_type: Type of job to run
            venue_id: Venue ID if job is venue-specific (None for global jobs)
            enabled: Whether this job is enabled
            run_time: Specific time of day to run (for daily jobs, e.g., 6:00 AM)
            interval_hours: Interval in hours between runs (for recurring jobs)
            next_run: Override next run time
        """
        self.job_type = job_type
        self.venue_id = venue_id
        self.enabled = enabled
        self.run_time = run_time
        self.interval_hours = interval_hours
        self.last_run: Optional[datetime] = None
        self.last_error: Optional[str] = None
        if next_run is not None:
            self.next_run = next_run
        else:
            # Defaulting to now() made every enabled job fire on every boot,
            # in both uvicorn workers — a deploy re-sent the weekly digest.
            # First run is computed the same way every later run is.
            self.next_run = datetime.now() + timedelta(minutes=5)
            self.update_next_run()

    def should_run(self) -> bool:
        """Check if this job should run now."""
        if not self.enabled:
            return False
        return datetime.now() >= self.next_run

    def update_next_run(self):
        """Calculate next run time based on job configuration.

        run_time is a VENUE wall-clock time: "the 6 AM digest" means 6 AM
        where the venues are (services/clock DEFAULT_TZ), not 6 AM on the
        UTC host — which delivered the morning digest at 2 pm Perth.
        """
        if self.interval_hours is not None:
            self.next_run = datetime.now() + timedelta(hours=max(self.interval_hours, 1))
        elif self.run_time is not None:
            try:
                from zoneinfo import ZoneInfo
                from rosteriq.services.clock import DEFAULT_TZ
                tz = ZoneInfo(DEFAULT_TZ)
                now_local = datetime.now(tz)
                candidate = datetime.combine(now_local.date(), self.run_time, tzinfo=tz)
                if candidate <= now_local:
                    candidate += timedelta(days=1)
                # scheduler compares against naive host-clock datetimes
                self.next_run = candidate.astimezone().replace(tzinfo=None)
            except Exception:  # noqa: BLE001 — a bad tz must not kill scheduling
                now = datetime.now()
                candidate = datetime.combine(now.date(), self.run_time)
                if candidate <= now:
                    candidate += timedelta(days=1)
                self.next_run = candidate
        else:
            self.next_run = datetime.now() + timedelta(hours=1)

    def log_run(self, error: Optional[str] = None):
        """Log a run attempt."""
        self.last_run = datetime.now()
        if error:
            self.last_error = error
        self.update_next_run()


class AsyncTaskScheduler:
    """
    Lightweight async background task scheduler.

    Runs recurring jobs inside FastAPI using asyncio. Each job type has a
    handler that is called at its scheduled time.
    """

    def __init__(self):
        """Initialize the scheduler."""
        self._jobs: Dict[str, JobConfig] = {}
        self._running = False
        self._task: Optional[asyncio.Task] = None
        self._db = get_db()
        self._notification_service = get_notification_service()

    def register_job(self, job: JobConfig, job_id: Optional[str] = None) -> str:
        """
        Register a job to be scheduled.

        Args:
            job: JobConfig instance
            job_id: Optional custom job ID. Auto-generated if not provided.

        Returns:
            Job ID
        """
        if job_id is None:
            job_id = f"{job.job_type.value}"
            if job.venue_id:
                job_id += f"_{job.venue_id}"
            if job_id in self._jobs:
                job_id += f"_{len(self._jobs)}"

        self._jobs[job_id] = job
        logger.info(f"Registered job: {job_id} (type={job.job_type.value})")
        return job_id

    async def start(self):
        """Start the task scheduler."""
        if self._running:
            logger.warning("Scheduler already running")
            return

        self._running = True
        logger.info("Task scheduler started")

        # Setup default jobs if none registered
        self._setup_default_jobs()

        # Start the main loop
        self._task = asyncio.create_task(self._run_loop())

    async def stop(self):
        """Stop the task scheduler."""
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("Task scheduler stopped")

    def _setup_default_jobs(self):
        """Setup default jobs if none are registered."""
        if self._jobs:
            return

        # Daily digest at 6:00 AM AEST
        daily_digest = JobConfig(
            job_type=JobType.DAILY_DIGEST,
            enabled=True,
            run_time=time(6, 0),  # 6:00 AM
        )
        self.register_job(daily_digest, "daily_digest")

        # Weekly digest — once a week (the WeeklyDigest service was complete but
        # was never scheduled, so it only ran when a human hit the endpoint).
        weekly_digest = JobConfig(
            job_type=JobType.WEEKLY_DIGEST,
            enabled=True,
            interval_hours=168,  # 7 days
        )
        self.register_job(weekly_digest, "weekly_digest")

        # The following three jobs are registered but DISABLED by default: their
        # handlers are currently log-only placeholders (no cert notifications, no
        # variance alerts, no revenue pull), so running them on a timer would imply
        # a live feature that does nothing. Enable each only once its handler is
        # implemented (cert-expiry needs certification fields on Employee first).
        cert_check = JobConfig(
            job_type=JobType.CERT_EXPIRY_CHECK,
            enabled=False,
            run_time=time(7, 0),  # 7:00 AM
        )
        self.register_job(cert_check, "cert_expiry_check")

        revenue_sync = JobConfig(
            job_type=JobType.REVENUE_SYNC,
            enabled=False,
            interval_hours=4,
        )
        self.register_job(revenue_sync, "revenue_sync")

        variance_monitor = JobConfig(
            job_type=JobType.VARIANCE_MONITOR,
            enabled=False,
            interval_hours=0,  # Will be treated as 30 min in handler
        )
        self.register_job(variance_monitor, "variance_monitor")

        # Retention keeps the event log affordable. Enabled by default: it is
        # the one job whose absence quietly costs money (an ever-growing table
        # nobody notices until a query times out).
        event_retention = JobConfig(
            job_type=JobType.EVENT_RETENTION,
            enabled=True,
            interval_hours=24,
            # next_run defaults to "now", which makes every enabled job fire on
            # every boot — and with two uvicorn workers, twice. Retention is
            # idempotent, but there is no reason to spend a DELETE on each
            # deploy: start the clock an hour out.
            next_run=datetime.now() + timedelta(hours=1),
        )
        self.register_job(event_retention, "event_retention")

    async def _run_loop(self):
        """Main scheduler loop — runs jobs as scheduled."""
        while self._running:
            try:
                for job_id, job in list(self._jobs.items()):
                    if job.should_run():
                        await self._run_job(job_id, job)
            except Exception as e:
                logger.error(f"Scheduler loop error: {e}", exc_info=True)

            # Check every 60 seconds
            await asyncio.sleep(60)

    async def _run_job(self, job_id: str, job: JobConfig):
        """Execute a single job with error handling.

        Every run is recorded as a ``job`` event. Without it a scheduled job
        that quietly stopped running (or has been failing for a week) is
        invisible — stdout scrolls away, and the in-process job log dies with
        the worker on every deploy.
        """
        import time as _time
        _t0 = _time.perf_counter()
        try:
            logger.info(f"Running job: {job_id}")
            handler = self._get_job_handler(job.job_type)
            await handler(job)
            job.log_run()
            logger.info(f"Job completed: {job_id}")
            _record_job(job_id, job, "ok", (_time.perf_counter() - _t0) * 1000)
        except Exception as e:
            error_msg = str(e)
            job.log_run(error=error_msg)
            logger.error(f"Job failed: {job_id} - {error_msg}", exc_info=True)
            _record_job(job_id, job, "failed", (_time.perf_counter() - _t0) * 1000,
                        exception=type(e).__name__, message=error_msg[:300])

    def _get_job_handler(self, job_type: JobType) -> Callable:
        """Get the handler function for a job type."""
        handlers = {
            JobType.DAILY_DIGEST: self._handle_daily_digest,
            JobType.WEEKLY_DIGEST: self._handle_weekly_digest,
            JobType.CERT_EXPIRY_CHECK: self._handle_cert_expiry_check,
            JobType.REVENUE_SYNC: self._handle_revenue_sync,
            JobType.VARIANCE_MONITOR: self._handle_variance_monitor,
            JobType.EVENT_RETENTION: self._handle_event_retention,
        }
        return handlers.get(job_type, self._handle_noop)

    async def _handle_event_retention(self, job: JobConfig):
        """Trim the event log to EVENT_RETENTION_DAYS (default 90).

        Traffic-driven categories (perf/integration/ai) are what make the
        table grow; without a trim the log becomes an expensive way to store
        last year's noise. Audit rows worth keeping longer should be exported
        before they age out — 90 days is a deliberate default, not a limit of
        the design.
        """
        import os
        from datetime import datetime, timedelta

        days = int(os.environ.get("EVENT_RETENTION_DAYS", "90"))
        cutoff = datetime.utcnow() - timedelta(days=days)
        from rosteriq.database import get_db

        removed = get_db().prune_events(cutoff)
        logger.info(f"Event retention: removed {removed} events older than {days}d")
        return removed

    async def _handle_daily_digest(self, job: JobConfig):
        """
        Send daily digest emails to all venue managers.

        Gets active venues from database and sends morning digest with
        today's roster, expected covers, and any alerts.
        """
        venues = self._db.list_venues()
        if not venues:
            logger.info("No venues found for daily digest")
            return

        for venue in venues:
            try:
                # Get today's shifts from roster
                rosters = self._db.list_rosters()
                today = datetime.now().date()

                venue_shifts = []
                for roster in rosters:
                    for shift in roster.shifts:
                        if shift.date == today:
                            venue_shifts.append(shift)

                if venue.manager_email:
                    await self._notification_service.send_daily_digest(
                        venue_id=venue.id,
                        venue=venue,
                        manager_email=venue.manager_email,
                        roster_shifts=venue_shifts,
                        expected_covers={},
                        weather_forecast=None,
                        events=None,
                        compliance_alerts=None,
                    )
                    logger.info(f"Sent daily digest to {venue.name}")
            except Exception as e:
                logger.error(f"Failed to send daily digest for {venue.id}: {e}")

    async def _handle_weekly_digest(self, job: JobConfig):
        """
        Send the weekly digest email to each venue's manager.

        Uses the (already complete) WeeklyDigest service, which was previously
        only reachable via a manual endpoint and never actually scheduled.
        """
        venues = self._db.list_venues()
        if not venues:
            logger.info("No venues found for weekly digest")
            return

        from rosteriq.services.weekly_digest import WeeklyDigestGenerator
        generator = WeeklyDigestGenerator()
        today = datetime.now().date()
        week_start = today - timedelta(days=today.weekday())  # Monday of this week

        for venue in venues:
            try:
                manager_email = getattr(venue, "manager_email", None)
                if manager_email:
                    await generator.send_digest(venue.id, week_start, [manager_email])
                    logger.info(f"Sent weekly digest to {venue.name}")
            except Exception as e:
                logger.error(f"Failed to send weekly digest for {venue.id}: {e}")

    async def _handle_cert_expiry_check(self, job: JobConfig):
        """
        Check for certifications expiring within 14 days.

        Scans all employees for expiring certifications and sends
        certification_expiry notifications.
        """
        employees = self._db.list_employees()
        if not employees:
            logger.info("No employees found for cert expiry check")
            return

        today = datetime.now().date()
        expiry_window_start = today
        expiry_window_end = today + timedelta(days=14)

        for employee in employees:
            try:
                # Note: Employee model doesn't currently have certification fields
                # This is a placeholder for when certifications are added to the model
                # For now, just log that we checked
                logger.debug(f"Checked certifications for {employee.name}")
            except Exception as e:
                logger.error(f"Failed to check certs for {employee.id}: {e}")

    async def _handle_revenue_sync(self, job: JobConfig):
        """
        Sync revenue data from Xero for connected venues.

        Placeholder job that logs intent. Actual Xero sync is handled
        by xero_integration.py module.
        """
        venues = self._db.list_venues()
        synced_count = 0

        for venue in venues:
            try:
                # Check if Xero is connected for this venue
                # (would check venue.xero_credentials or similar)
                # For now, just log
                logger.debug(f"Revenue sync placeholder for {venue.name}")
                synced_count += 1
            except Exception as e:
                logger.error(f"Revenue sync failed for {venue.id}: {e}")

        logger.info(f"Revenue sync job completed (checked {synced_count} venues)")

    async def _handle_variance_monitor(self, job: JobConfig):
        """
        Monitor shift actuals vs roster plan.

        Checks current shift actuals against roster predictions and triggers
        variance_alert notifications if drift exceeds threshold (e.g., 20%).
        """
        venues = self._db.list_venues()
        if not venues:
            logger.info("No venues found for variance monitoring")
            return

        variance_threshold = 0.20  # 20% threshold
        monitored_count = 0

        for venue in venues:
            try:
                # In a real implementation, would:
                # 1. Get current active shifts
                # 2. Compare to roster plan
                # 3. Calculate variance
                # 4. Trigger alert if > threshold
                # For now, just log
                logger.debug(f"Variance monitoring for {venue.name}")
                monitored_count += 1
            except Exception as e:
                logger.error(f"Variance monitoring failed for {venue.id}: {e}")

        logger.info(f"Variance monitor job completed ({monitored_count} venues)")

    async def _handle_noop(self, job: JobConfig):
        """Placeholder handler for unimplemented job types."""
        logger.warning(f"No handler for job type: {job.job_type.value}")

    def get_job_status(self, job_id: str) -> Optional[Dict[str, Any]]:
        """Get status of a specific job."""
        job = self._jobs.get(job_id)
        if not job:
            return None

        return {
            "job_id": job_id,
            "type": job.job_type.value,
            "venue_id": job.venue_id,
            "enabled": job.enabled,
            "next_run": job.next_run.isoformat() if job.next_run else None,
            "last_run": job.last_run.isoformat() if job.last_run else None,
            "last_error": job.last_error,
        }

    def list_jobs(self) -> Dict[str, Dict[str, Any]]:
        """Get status of all registered jobs."""
        return {job_id: self.get_job_status(job_id) for job_id in self._jobs.keys()}

    def enable_job(self, job_id: str):
        """Enable a specific job."""
        if job_id in self._jobs:
            self._jobs[job_id].enabled = True
            logger.info(f"Enabled job: {job_id}")

    def disable_job(self, job_id: str):
        """Disable a specific job."""
        if job_id in self._jobs:
            self._jobs[job_id].enabled = False
            logger.info(f"Disabled job: {job_id}")

    def reschedule_job(self, job_id: str, next_run: datetime):
        """Reschedule a job to run at a specific time."""
        if job_id in self._jobs:
            self._jobs[job_id].next_run = next_run
            logger.info(f"Rescheduled job {job_id} to {next_run.isoformat()}")


# Singleton instance
_scheduler: Optional[AsyncTaskScheduler] = None


def get_scheduler() -> AsyncTaskScheduler:
    """Get or create the global scheduler singleton."""
    global _scheduler
    if _scheduler is None:
        _scheduler = AsyncTaskScheduler()
    return _scheduler


def setup_scheduler(app):
    """
    Setup scheduler startup and shutdown hooks for FastAPI.

    Call this in api.py during app initialization:
        from rosteriq.services.task_scheduler import setup_scheduler
        setup_scheduler(app)

    Args:
        app: FastAPI application instance
    """
    scheduler = get_scheduler()

    @app.on_event("startup")
    async def startup():
        await scheduler.start()

    @app.on_event("shutdown")
    async def shutdown():
        await scheduler.stop()

    logger.info("Scheduler setup complete")
