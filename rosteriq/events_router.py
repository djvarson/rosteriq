"""
Events Router for RosterIQ API
=============================

Provides REST endpoints for fetching events data.

Endpoints:
  GET /api/v1/events/{venue_id}?from=YYYY-MM-DD&to=YYYY-MM-DD
    - Query params: from (default today), to (default today+7)
    - Returns: {"venue_id": str, "source": str, "events": [VenueEvent, ...]}
    - Error 400: to < from
    - Error 502: adapter failure
"""

import logging
import os
from datetime import datetime, date, timedelta, timezone
from typing import Optional

from fastapi import APIRouter, HTTPException, Query

from rosteriq.data_feeds.events import (
    DemoEventsAdapter,
    PerthIsOKAdapter,
    StadiumScheduleAdapter,
    CompositeEventsAdapter,
    EventsAdapterError,
    VenueEvent,
)

logger = logging.getLogger("rosteriq.events_router")

AU_TZ = timezone(timedelta(hours=10))

router = APIRouter(tags=["events"])

# Global adapter instance (lazy-loaded)
_adapter: Optional[CompositeEventsAdapter] = None


def _get_adapter() -> CompositeEventsAdapter:
    """Get or create the composite events adapter."""
    global _adapter
    if _adapter is None:
        data_mode = os.getenv("ROSTERIQ_DATA_MODE", "demo").lower()

        adapters = []

        if data_mode == "live":
            try:
                adapters.append(PerthIsOKAdapter())
            except ImportError:
                logger.warning("httpx not available; skipping PerthIsOKAdapter")

            # Stadium config (example; would be loaded from config/env)
            stadium_config = {
                # "optus_stadium": {
                #     "name": "Optus Stadium",
                #     "lat": -31.945,
                #     "lon": 115.836,
                #     "schedule_url": "https://...",
                #     "attendance_capacity": 60000,
                # },
            }
            if stadium_config:
                try:
                    adapters.append(StadiumScheduleAdapter(stadium_config))
                except ImportError:
                    logger.warning("httpx not available; skipping StadiumScheduleAdapter")

        # Always include demo as fallback
        adapters.append(DemoEventsAdapter())

        _adapter = CompositeEventsAdapter(adapters)

    return _adapter


@router.get("/api/v1/events/{venue_id}")
async def get_events(
    venue_id: str,
    from_date: Optional[str] = Query(None, alias="from"),
    to_date: Optional[str] = Query(None, alias="to"),
) -> dict:
    """
    Fetch events for a venue within a date range.

    Args:
        venue_id: RosterIQ venue identifier
        from_date: Start date (YYYY-MM-DD, default today)
        to_date: End date (YYYY-MM-DD, default today+7)

    Returns:
        {
            "venue_id": "...",
            "source": "composite",
            "events": [
                {
                    "event_id": "...",
                    "title": "...",
                    "start_time": "ISO datetime",
                    "end_time": "ISO datetime or null",
                    "location_name": "...",
                    "lat": float or null,
                    "lon": float or null,
                    "distance_km_from_venue": float or null,
                    "expected_attendance": int or null,
                    "category": "stadium|concert|festival|comedy|community|other",
                    "source": "perthisok|stadium|demo"
                },
                ...
            ]
        }

    Raises:
        400: to < from
        502: Adapter failure
    """
    # Parse dates
    today = date.today()

    if from_date:
        try:
            window_start = datetime.strptime(from_date, "%Y-%m-%d").replace(tzinfo=AU_TZ)
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid from_date format (use YYYY-MM-DD)")
    else:
        window_start = datetime.combine(today, datetime.min.time()).replace(tzinfo=AU_TZ)

    if to_date:
        try:
            window_end = datetime.strptime(to_date, "%Y-%m-%d").replace(tzinfo=AU_TZ)
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid to_date format (use YYYY-MM-DD)")
    else:
        window_end = datetime.combine(today + timedelta(days=7), datetime.min.time()).replace(tzinfo=AU_TZ)

    # Extend to end of day
    window_end = window_end.replace(hour=23, minute=59, second=59)

    # Validate window
    if window_end < window_start:
        raise HTTPException(status_code=400, detail="to_date must be >= from_date")

    # Fetch events
    try:
        adapter = _get_adapter()
        events = await adapter.get_events(venue_id, window_start, window_end)

        # Convert VenueEvent objects to dicts
        event_dicts = []
        for event in events:
            event_dicts.append({
                "event_id": event.event_id,
                "title": event.title,
                "start_time": event.start_time.isoformat(),
                "end_time": event.end_time.isoformat() if event.end_time else None,
                "location_name": event.location_name,
                "lat": event.lat,
                "lon": event.lon,
                "distance_km_from_venue": event.distance_km_from_venue,
                "expected_attendance": event.expected_attendance,
                "category": event.category,
                "source": event.source,
            })

        return {
            "venue_id": venue_id,
            "source": "composite",
            "events": event_dicts,
        }

    except EventsAdapterError as e:
        logger.error(f"Events adapter error for {venue_id}: {e}")
        raise HTTPException(status_code=502, detail=f"Events adapter failed: {str(e)}")
    except Exception as e:
        logger.error(f"Unexpected error fetching events for {venue_id}: {e}")
        raise HTTPException(status_code=502, detail="Unexpected error fetching events")
