"""Scheduled brief runner — asyncio-based cron for brief dispatch.

Runs a background task that wakes up every 60 seconds and checks if
any briefs are due to fire based on local timezone schedules.

Schedules:
- Morning brief: daily at 06:30 local time
- Weekly digest: Monday at 07:00 local time
- Portfolio recap: Sunday at 18:00 local time (owners only)

Deduplication: per-subscription per-date to avoid re-sending the same
brief within 24 hours.

No APScheduler, no Celery — pure asyncio + zoneinfo stdlib.
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, date, timezone, timedelta
from typing import Dict, Optional, List, Any
from zoneinfo import ZoneInfo

logger = logging.getLogger("rosteriq.scheduled_brief_runner")


# Dedup tracking: (subscription_id, brief_type, date) -> timestamp sent
_SENT_CACHE: Dict[tuple, float] = {}


class BriefScheduler:
    """Background task runner for scheduled briefs."""

    def __init__(self):
        self._task: Optional[asyncio.Task] = None
        self._running = False

    async def start(self):
        """Start the scheduler background task."""
        if self._running:
            return
        self._running = True
        logger.info("BriefScheduler: starting")
        self._task = asyncio.create_task(self._run_loop())

    async def stop(self):
        """Stop the scheduler background task."""
        self._running = False
        if self._task:
            try:
                self._task.cancel()
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("BriefScheduler: stopped")

    async def _run_loop(self):
        """Main scheduler loop — wakes every 60 seconds."""
        while self._running:
            try:
                await self._tick()
            except Exception:
                logger.exception("BriefScheduler: tick failed")
            await asyncio.sleep(60)

    async def _tick(self):
        """Check all subscriptions for due briefs."""
        from rosteriq import brief_subscriptions
        from rosteriq import brief_dispatcher

        sub_store = brief_subscriptions.get_subscription_store()
        subs = sub_store.all()

        for sub in subs:
            if not sub.enabled:
                continue

            now = datetime.now(ZoneInfo(sub.local_tz))
            today_local = now.date()

            # Morning brief: daily at 06:30
            if "morning" in sub.brief_types and "email" in sub.delivery_channels:
                if self._should_send_morning(now):
                    cache_key = (sub.subscription_id, "morning", str(today_local))
                    if not self._is_dedup(cache_key):
                        try:
                            await brief_dispatcher.dispatch_morning_brief_with_delivery(
                                sub.venue_id,
                                target_date=str(today_local - timedelta(days=1)),
                                venue_label=sub.venue_id,
                            )
                            self._mark_sent(cache_key)
                        except Exception:
                            logger.exception(
                                "BriefScheduler: morning brief dispatch failed for %s",
                                sub.subscription_id,
                            )

            # Weekly digest: Monday at 07:00
            if "weekly" in sub.brief_types and "email" in sub.delivery_channels:
                if self._should_send_weekly(now):
                    # Use last Sunday as week_ending
                    days_back = (now.weekday() + 1) % 7
                    if days_back == 0:
                        days_back = 7
                    week_ending = today_local - timedelta(days=days_back)
                    cache_key = (sub.subscription_id, "weekly", str(week_ending))
                    if not self._is_dedup(cache_key):
                        try:
                            await brief_dispatcher.dispatch_weekly_digest_with_delivery(
                                sub.venue_id,
                                week_ending=str(week_ending),
                                venue_label=sub.venue_id,
                            )
                            self._mark_sent(cache_key)
                        except Exception:
                            logger.exception(
                                "BriefScheduler: weekly digest dispatch failed for %s",
                                sub.subscription_id,
                            )

            # Portfolio recap: Sunday at 18:00 (owners only)
            if (sub.user_role == "owner" and
                "portfolio" in sub.brief_types and
                "email" in sub.delivery_channels):
                if self._should_send_portfolio(now):
                    cache_key = (sub.subscription_id, "portfolio", str(today_local))
                    if not self._is_dedup(cache_key):
                        try:
                            await brief_dispatcher.dispatch_portfolio_recap_with_delivery(
                                [sub.venue_id],
                                target_date=str(today_local),
                            )
                            self._mark_sent(cache_key)
                        except Exception:
                            logger.exception(
                                "BriefScheduler: portfolio recap dispatch failed for %s",
                                sub.subscription_id,
                            )

    def _should_send_morning(self, now: datetime) -> bool:
        """Check if it's time to send the morning brief (06:30 local)."""
        # Fire between 06:30 and 06:59
        return now.hour == 6 and now.minute >= 30

    def _should_send_weekly(self, now: datetime) -> bool:
        """Check if it's time to send the weekly digest (Monday 07:00 local)."""
        # Monday is weekday 0
        return (now.weekday() == 0 and
                now.hour == 7 and
                now.minute < 60)

    def _should_send_portfolio(self, now: datetime) -> bool:
        """Check if it's time to send the portfolio recap (Sunday 18:00 local)."""
        # Sunday is weekday 6
        return (now.weekday() == 6 and
                now.hour == 18 and
                now.minute < 60)

    def _is_dedup(self, cache_key: tuple) -> bool:
        """Check if this brief was already sent today."""
        if cache_key not in _SENT_CACHE:
            return False
        # If it was sent less than 24 hours ago, skip
        sent_time = _SENT_CACHE[cache_key]
        now_ts = datetime.now(timezone.utc).timestamp()
        return (now_ts - sent_time) < 86400  # 24 hours

    def _mark_sent(self, cache_key: tuple):
        """Mark a brief as sent."""
        _SENT_CACHE[cache_key] = datetime.now(timezone.utc).timestamp()


# ---------------------------------------------------------------------------
# Module singleton
# ---------------------------------------------------------------------------

_scheduler: Optional[BriefScheduler] = None


def get_brief_scheduler() -> BriefScheduler:
    """Return the module-level singleton BriefScheduler."""
    global _scheduler
    if _scheduler is None:
        _scheduler = BriefScheduler()
    return _scheduler


def reset_brief_scheduler_for_tests():
    """Reset singleton and clear dedup cache. Used by tests."""
    global _scheduler
    _SENT_CACHE.clear()
    if _scheduler:
        try:
            asyncio.run(_scheduler.stop())
        except Exception:
            pass
    _scheduler = None
