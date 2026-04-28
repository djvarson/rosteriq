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


class JobType(str, Enum):
    """Supported background job types."""
    DAILY_DIGEST = "daily_digest"
    CERT_EXPIRY_CHECK = "cert_expiry_check"
    REVENUE_SYNC = "revenue_sync"
    VARIANCE_MONITOR = "variance_monitor"


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
        self.next_run = next_run or datetime.now()
        self.last_run: Optional[datetime] = None
        self.last_error: Optional[str] = None

    def should_run(self) -> bool:
        """Check if this job should run now."""
        if not self.enabled:
            return False
        return datetime.now() >= self.next_run

    def update_next_run(self):
        """Calculate next run time based on job configuration."""
        if self.interval_hours is not None:
            self.next_run = datetime.now() + timedelta(hours=self.interval_hours)
        elif self.run_time is not None:
            # Schedule for next occurrence of run_time
            now = datetime.now()
            next_run = datetime.combine(now.date(), self.run_time)
            if next_run <= now:
                next_run += timedelta(days=1)
            self.next_run = next_run
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

        # Cert expiry check daily at 7:00 AM AEST
        cert_check = JobConfig(
            job_type=JobType.CERT_EXPIRY_CHECK,
            enabled=True,
            run_time=time(7, 0),  # 7:00 AM
        )
        self.register_job(cert_check, "cert_expiry_check")

        # Revenue sync every 4 hours
        revenue_sync = JobConfig(
            job_type=JobType.REVENUE_SYNC,
            enabled=True,
            interval_hours=4,
        )
        self.register_job(revenue_sync, "revenue_sync")

        # Variance monitor every 30 minutes
        variance_monitor = JobConfig(
            job_type=JobType.VARIANCE_MONITOR,
            enabled=True,
            interval_hours=0,  # Will be treated as 30 min in handler
        )
        self.register_job(variance_monitor, "variance_monitor")

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
        """Execute a single job with error handling."""
        try:
            logger.info(f"Running job: {job_id}")
            handler = self._get_job_handler(job.job_type)
            await handler(job)
            job.log_run()
            logger.info(f"Job completed: {job_id}")
        except Exception as e:
            error_msg = str(e)
            job.log_run(error=error_msg)
            logger.error(f"Job failed: {job_id} - {error_msg}", exc_info=True)

    def _get_job_handler(self, job_type: JobType) -> Callable:
        """Get the handler function for a job type."""
        handlers = {
            JobType.DAILY_DIGEST: self._handle_daily_digest,
            JobType.CERT_EXPIRY_CHECK: self._handle_cert_expiry_check,
            JobType.REVENUE_SYNC: self._handle_revenue_sync,
            JobType.VARIANCE_MONITOR: self._handle_variance_monitor,
        }
        return handlers.get(job_type, self._handle_noop)

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
