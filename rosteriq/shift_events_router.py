"""FastAPI router for shift event logging and pattern queries.

Exports endpoints:
  POST /api/v1/shift-events — record a new event
  GET /api/v1/shift-events/{venue_id} — list events
  GET /api/v1/shift-events/{venue_id}/patterns — learn patterns
  GET /api/v1/shift-events/{venue_id}/predict — predict applicable patterns
"""

from datetime import datetime, date, timezone
from typing import Optional, List

from fastapi import APIRouter, HTTPException, Query, Request
from pydantic import BaseModel

from rosteriq.shift_events import (
    ShiftEvent,
    EventCategory,
    EVENT_CATEGORY_LABELS,
    ShiftEventStore,
    PatternLearner,
)

# Auth gating — fall back to no-op in demo/sandbox when auth stack unavailable
try:
    from rosteriq.auth import require_access, AccessLevel  # type: ignore
except Exception:  # pragma: no cover — demo/sandbox path
    require_access = None  # type: ignore
    AccessLevel = None  # type: ignore


async def _gate(request: Request, level_name: str) -> None:
    """Apply role gating if auth stack is present; no-op in demo."""
    if require_access is None or AccessLevel is None:
        return
    level = getattr(AccessLevel, level_name)
    await require_access(level)(request=request)


# ---------------------------------------------------------------------------
# Pydantic models for request/response
# ---------------------------------------------------------------------------


class RecordEventRequest(BaseModel):
    """Request to record a new shift event."""

    venue_id: str
    category: str  # validated against EventCategory enum
    description: str
    headcount_at_time: Optional[int] = None
    logged_by: Optional[str] = None
    shift_date: Optional[str] = None  # ISO format, defaults to today
    weather_condition: Optional[str] = None
    active_event_ids: Optional[List[str]] = None
    tags: Optional[List[str]] = None


class EventResponse(BaseModel):
    """Serialized shift event for API response."""

    event_id: str
    venue_id: str
    category: str
    description: str
    timestamp: str
    headcount_at_time: Optional[int]
    logged_by: Optional[str]
    shift_date: str
    day_of_week: int
    hour_of_day: int
    weather_condition: Optional[str]
    active_event_ids: List[str]
    tags: List[str]


class PatternResponse(BaseModel):
    """Serialized learned pattern."""

    description: str
    category: str
    weekday: int
    hour_window: tuple
    occurrences: int
    confidence: float


# ---------------------------------------------------------------------------
# Module-level store singleton (like headcount_store)
# ---------------------------------------------------------------------------

_shift_event_store = ShiftEventStore()


# ---------------------------------------------------------------------------
# FastAPI router
# ---------------------------------------------------------------------------

router = APIRouter(tags=["shift-events"])


@router.post("/api/v1/shift-events")
async def record_event(req: RecordEventRequest, request: Request) -> EventResponse:
    """
    Record a new shift event.

    Validates category, auto-populates timestamp, day_of_week, hour_of_day.
    Returns the recorded event.
    """
    await _gate(request, "L1_SUPERVISOR")
    # Validate category
    try:
        category = EventCategory(req.category)
    except ValueError:
        valid_cats = [c.value for c in EventCategory]
        raise HTTPException(
            status_code=400,
            detail=f"Invalid category '{req.category}'. Valid: {valid_cats}",
        )

    # Parse shift_date or default to today
    if req.shift_date:
        try:
            shift_dt = datetime.fromisoformat(req.shift_date)
            shift_date = shift_dt.date() if hasattr(shift_dt, 'date') else date.fromisoformat(req.shift_date)
        except (ValueError, TypeError):
            raise HTTPException(
                status_code=400,
                detail=f"Invalid shift_date '{req.shift_date}'. Use ISO format (YYYY-MM-DD).",
            )
    else:
        shift_date = datetime.now(timezone.utc).date()

    # Get current UTC time for timestamp
    now = datetime.now(timezone.utc)
    day_of_week = now.weekday()
    hour_of_day = now.hour

    # Create and record event
    event = ShiftEvent(
        event_id=f"evt_{__import__('uuid').uuid4().hex[:12]}",
        venue_id=req.venue_id,
        category=category,
        description=req.description,
        timestamp=now,
        headcount_at_time=req.headcount_at_time,
        logged_by=req.logged_by,
        shift_date=shift_date,
        day_of_week=day_of_week,
        hour_of_day=hour_of_day,
        weather_condition=req.weather_condition,
        active_event_ids=req.active_event_ids or [],
        tags=req.tags or [],
    )

    _shift_event_store.record(event)

    return EventResponse(**event.to_dict())


@router.get("/api/v1/shift-events/{venue_id}")
async def list_events(
    venue_id: str,
    request: Request,
    since: Optional[str] = Query(None),
    hours: Optional[int] = Query(None),
) -> dict:
    """
    List shift events for a venue.

    Filters:
      ?since=ISO-8601 — events after this timestamp
      ?hours=N — events from last N hours (default 24 if not specified)
    """
    await _gate(request, "L1_SUPERVISOR")
    if hours and not since:
        # Recent hours filter
        events = _shift_event_store.recent(venue_id, hours=hours)
    elif since:
        # Since filter
        try:
            since_dt = datetime.fromisoformat(since)
        except (ValueError, TypeError):
            raise HTTPException(
                status_code=400,
                detail=f"Invalid since '{since}'. Use ISO-8601 format.",
            )
        events = _shift_event_store.for_venue(venue_id, since=since_dt)
    else:
        # Default: last 24 hours
        events = _shift_event_store.recent(venue_id, hours=24)

    return {
        "venue_id": venue_id,
        "count": len(events),
        "events": [EventResponse(**e.to_dict()) for e in events],
    }


@router.get("/api/v1/shift-events/{venue_id}/patterns")
async def get_patterns(venue_id: str, request: Request) -> dict:
    """
    Learn patterns from a venue's event history.

    Runs PatternLearner on all events for the venue and returns patterns
    sorted by confidence (descending).
    """
    await _gate(request, "L1_SUPERVISOR")
    venue_events = _shift_event_store.for_venue(venue_id)
    patterns = PatternLearner.analyse(venue_events)

    return {
        "venue_id": venue_id,
        "count": len(patterns),
        "patterns": [PatternResponse(**p.to_dict()) for p in patterns],
    }


@router.get("/api/v1/shift-events/{venue_id}/predict")
async def predict_patterns(
    venue_id: str,
    request: Request,
    date_str: str = Query(..., alias="date"),
    hour: int = Query(...),
) -> dict:
    """
    Predict applicable patterns for a specific date and hour.

    Query params:
      date=YYYY-MM-DD (required)
      hour=N (0–23, required)

    Returns patterns that apply to that weekday + hour for the venue.
    """
    await _gate(request, "L1_SUPERVISOR")
    try:
        target_date = date.fromisoformat(date_str)
    except (ValueError, TypeError, AttributeError):
        raise HTTPException(
            status_code=400,
            detail=f"Invalid date '{date_str}'. Use YYYY-MM-DD format.",
        )

    if not (0 <= hour <= 23):
        raise HTTPException(
            status_code=400,
            detail=f"Invalid hour {hour}. Must be 0–23.",
        )

    all_venue_events = _shift_event_store.for_venue(venue_id)
    patterns = PatternLearner.predict_for(venue_id, target_date, hour, all_venue_events)

    return {
        "venue_id": venue_id,
        "date": target_date.isoformat(),
        "hour": hour,
        "count": len(patterns),
        "patterns": [PatternResponse(**p.to_dict()) for p in patterns],
    }
