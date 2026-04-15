"""Brief lifecycle hooks — startup and shutdown for FastAPI integration.

These functions are called from the lifespan context manager in api_v2.py.

The brief scheduler runs in an asyncio background task and checks every 60 seconds
if any briefs are due to fire based on subscription timezones.
"""
from __future__ import annotations

import logging

logger = logging.getLogger("rosteriq.brief_lifecycle")


async def start_briefs_on_startup():
    """Start the brief scheduler on app startup."""
    try:
        from rosteriq.scheduled_brief_runner import get_brief_scheduler
        scheduler = get_brief_scheduler()
        await scheduler.start()
        logger.info("Brief scheduler started on startup")
    except Exception:
        logger.exception("Failed to start brief scheduler")


async def stop_briefs_on_shutdown():
    """Stop the brief scheduler on app shutdown."""
    try:
        from rosteriq.scheduled_brief_runner import get_brief_scheduler
        scheduler = get_brief_scheduler()
        await scheduler.stop()
        logger.info("Brief scheduler stopped on shutdown")
    except Exception:
        logger.exception("Failed to stop brief scheduler")
