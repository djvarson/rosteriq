"""
Function Tracker event/function management API routes.

Endpoints for connecting, syncing, and querying event/function data
from Function Tracker (functiontracker.com) — a cloud-based venue and
event management system used by Australian hospitality venues.

Unlike table-level reservations, Function Tracker events are large-format
(weddings, conferences, corporate dinners) with significant staffing impact.

Routes:
    POST /api/function-tracker/install       -- Connect Function Tracker
    POST /api/function-tracker/uninstall     -- Disconnect Function Tracker
    GET  /api/function-tracker/status        -- Check connection status
    POST /api/function-tracker/sync          -- Pull events for a date range
    GET  /api/function-tracker/upcoming      -- Get upcoming events (next N days)
    GET  /api/function-tracker/forecast      -- Get event-based demand forecast
    GET  /api/function-tracker/event/{id}    -- Get single event detail
"""

import logging
from datetime import date, datetime, timedelta
from typing import Optional

from fastapi import APIRouter, HTTPException, Path, Query
from pydantic import BaseModel, Field

from rosteriq.database import get_db
from rosteriq.data_feeds.function_tracker import FunctionTrackerAdapter

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/function-tracker", tags=["function-tracker"])

# Plugin install key prefix
FT_PREFIX = "function_tracker_"
DISPLAY_NAME = "Function Tracker"


# ============================================================================
# Pydantic Request Models
# ============================================================================


class FTInstallRequest(BaseModel):
    venue_id: str = Field(..., description="RosterIQ venue ID")
    api_key: str = Field(..., description="Function Tracker API key or access token")
    ft_venue_id: Optional[str] = Field(
        default=None,
        description="Function Tracker venue ID (if different from RosterIQ venue ID)",
    )


class FTUninstallRequest(BaseModel):
    venue_id: str = Field(..., description="RosterIQ venue ID to disconnect")


class FTSyncRequest(BaseModel):
    venue_id: str = Field(..., description="RosterIQ venue ID")
    start_date: date = Field(..., description="Start date (YYYY-MM-DD)")
    end_date: date = Field(..., description="End date (YYYY-MM-DD)")


# ============================================================================
# Helpers
# ============================================================================


def _org_key(venue_id: str) -> str:
    """Generate the organisation_id key for plugin_installs."""
    return f"{FT_PREFIX}{venue_id}"


def _get_install_or_404(venue_id: str) -> dict:
    """Fetch the install record for a venue, or raise 404."""
    db = get_db()
    install = db.get_plugin_install(_org_key(venue_id))
    if not install or install.get("status") == "uninstalled":
        raise HTTPException(
            status_code=404,
            detail=f"No active {DISPLAY_NAME} connection for venue {venue_id}",
        )
    return install


def _build_adapter(api_key: str) -> FunctionTrackerAdapter:
    """Instantiate a Function Tracker adapter with the given API key."""
    return FunctionTrackerAdapter(api_key=api_key)


def _serialize_signal(signal) -> dict:
    """Convert a FeedSignal to a JSON-serialisable dict for Function Tracker."""
    signal_dict = {
        "date": signal.signal_date.isoformat() if hasattr(signal.signal_date, "isoformat") else str(signal.signal_date),
        "hour": signal.signal_hour,
        "strength": signal.strength.value if hasattr(signal.strength, "value") else str(signal.strength),
        "confidence": signal.confidence,
        "description": signal.description,
        "value": signal.value,
    }
    if hasattr(signal, "raw_data") and signal.raw_data:
        snapshot = signal.raw_data.get("snapshot", {})
        signal_dict["total_events"] = snapshot.get("total_events", 0)
        signal_dict["total_guests"] = snapshot.get("total_guests", 0)
        signal_dict["event_types"] = snapshot.get("event_types", [])
        signal_dict["largest_event_guests"] = snapshot.get("largest_event_guests", 0)
        signal_dict["estimated_extra_staff"] = snapshot.get("estimated_extra_staff", 0)
        signal_dict["events"] = snapshot.get("events", [])
    return signal_dict


# ============================================================================
# Install (Connect)
# ============================================================================


@router.post("/install")
async def install(body: FTInstallRequest) -> dict:
    """
    Connect Function Tracker for a venue.

    Stores API credentials and verifies connectivity.
    """
    db = get_db()
    org_key = _org_key(body.venue_id)

    # Verify connectivity
    adapter = _build_adapter(body.api_key)
    try:
        available = await adapter.is_available()
    except Exception as e:
        logger.error(f"{DISPLAY_NAME} connectivity check failed for venue {body.venue_id}: {e}")
        available = False
    finally:
        if hasattr(adapter, "__aexit__"):
            await adapter.__aexit__(None, None, None)

    # Save the install record
    ft_venue_id = body.ft_venue_id or body.venue_id
    install_record = {
        "organisation_id": org_key,
        "venue_id": body.venue_id,
        "provider": "function_tracker",
        "status": "active",
        "tokens": {
            "api_key": body.api_key,
            "ft_venue_id": ft_venue_id,
        },
        "installed_at": datetime.utcnow(),
        "updated_at": datetime.utcnow(),
    }
    db.save_plugin_install(install_record)

    logger.info(f"{DISPLAY_NAME} connected for venue {body.venue_id} (verified: {available})")
    return {
        "status": "success",
        "provider": "function_tracker",
        "venue_id": body.venue_id,
        "ft_venue_id": ft_venue_id,
        "connectivity_verified": available,
        "message": f"{DISPLAY_NAME} connected successfully.",
    }


# ============================================================================
# Uninstall (Disconnect)
# ============================================================================


@router.post("/uninstall")
async def uninstall(body: FTUninstallRequest) -> dict:
    """Remove the Function Tracker connection for a venue."""
    install = _get_install_or_404(body.venue_id)

    install["status"] = "uninstalled"
    install["tokens"] = {}
    install["updated_at"] = datetime.utcnow()

    db = get_db()
    db.save_plugin_install(install)

    logger.info(f"{DISPLAY_NAME} uninstalled for venue {body.venue_id}")
    return {
        "status": "success",
        "provider": "function_tracker",
        "venue_id": body.venue_id,
        "message": f"{DISPLAY_NAME} connection removed.",
    }


# ============================================================================
# Status Check
# ============================================================================


@router.get("/status")
async def get_status(
    venue_id: str = Query(..., description="RosterIQ venue ID"),
) -> dict:
    """Check whether a venue has an active Function Tracker connection."""
    db = get_db()
    install = db.get_plugin_install(_org_key(venue_id))

    if not install:
        return {
            "connected": False,
            "provider": "function_tracker",
            "provider_name": DISPLAY_NAME,
            "venue_id": venue_id,
            "status": "not_installed",
        }

    tokens = install.get("tokens", {})
    return {
        "connected": install.get("status") == "active",
        "provider": "function_tracker",
        "provider_name": DISPLAY_NAME,
        "venue_id": venue_id,
        "status": install.get("status", "unknown"),
        "ft_venue_id": tokens.get("ft_venue_id"),
        "installed_at": install.get("installed_at"),
        "updated_at": install.get("updated_at"),
    }


# ============================================================================
# Sync — Pull event data for a date range
# ============================================================================


@router.post("/sync")
async def sync_events(body: FTSyncRequest) -> dict:
    """
    Pull event/function data from Function Tracker for a date range.

    Returns demand signals based on event size, type, and timing.
    """
    install = _get_install_or_404(body.venue_id)

    tokens = install.get("tokens", {})
    api_key = tokens.get("api_key", "")
    ft_venue_id = tokens.get("ft_venue_id", body.venue_id)

    if not api_key:
        raise HTTPException(
            status_code=401,
            detail=f"{DISPLAY_NAME} API key missing. Please re-install.",
        )

    adapter = _build_adapter(api_key)
    try:
        from rosteriq.data_feeds.base import Location

        location = Location(latitude=0, longitude=0)
        signals = await adapter.fetch_signals(
            location=location,
            start_date=body.start_date,
            end_date=body.end_date,
            venue_id=ft_venue_id,
        )
    except Exception as e:
        logger.error(
            f"{DISPLAY_NAME} sync failed for venue {body.venue_id} "
            f"({body.start_date} to {body.end_date}): {e}"
        )
        raise HTTPException(status_code=502, detail=f"{DISPLAY_NAME} API error: {e}")
    finally:
        if hasattr(adapter, "__aexit__"):
            await adapter.__aexit__(None, None, None)

    signal_data = [_serialize_signal(s) for s in signals]

    logger.info(
        f"Synced {len(signals)} event signals from {DISPLAY_NAME} for venue {body.venue_id} "
        f"({body.start_date} to {body.end_date})"
    )
    return {
        "status": "success",
        "provider": "function_tracker",
        "venue_id": body.venue_id,
        "start_date": body.start_date.isoformat(),
        "end_date": body.end_date.isoformat(),
        "count": len(signals),
        "signals": signal_data,
    }


# ============================================================================
# Upcoming — Get upcoming events for the next N days
# ============================================================================


@router.get("/upcoming")
async def get_upcoming(
    venue_id: str = Query(..., description="RosterIQ venue ID"),
    days: int = Query(default=14, ge=1, le=90, description="Number of days to look ahead"),
) -> dict:
    """
    Get upcoming events/functions for the next N days.

    Returns a list of events with guest counts, types, and estimated
    staffing impact. Useful for the demand intelligence panel.
    """
    install = _get_install_or_404(venue_id)

    tokens = install.get("tokens", {})
    api_key = tokens.get("api_key", "")
    ft_venue_id = tokens.get("ft_venue_id", venue_id)

    if not api_key:
        raise HTTPException(
            status_code=401,
            detail=f"{DISPLAY_NAME} API key missing. Please re-install.",
        )

    adapter = _build_adapter(api_key)
    try:
        events = await adapter.fetch_upcoming_events(ft_venue_id, days=days)
    except Exception as e:
        logger.error(f"{DISPLAY_NAME} upcoming fetch failed for venue {venue_id}: {e}")
        raise HTTPException(status_code=502, detail=f"{DISPLAY_NAME} API error: {e}")
    finally:
        if hasattr(adapter, "__aexit__"):
            await adapter.__aexit__(None, None, None)

    return {
        "status": "success",
        "provider": "function_tracker",
        "venue_id": venue_id,
        "days": days,
        "count": len(events),
        "events": events,
    }


# ============================================================================
# Forecast — Get event-based demand forecast
# ============================================================================


@router.get("/forecast")
async def get_forecast(
    venue_id: str = Query(..., description="RosterIQ venue ID"),
    days: int = Query(default=14, ge=1, le=90, description="Number of days to forecast"),
) -> dict:
    """
    Get event-based demand forecast for the next N days.

    Returns per-day summaries with estimated staffing impact based on
    event size and type. Feeds directly into roster generation.
    """
    install = _get_install_or_404(venue_id)

    tokens = install.get("tokens", {})
    api_key = tokens.get("api_key", "")
    ft_venue_id = tokens.get("ft_venue_id", venue_id)

    if not api_key:
        raise HTTPException(
            status_code=401,
            detail=f"{DISPLAY_NAME} API key missing. Please re-install.",
        )

    today = date.today()
    end_date = today + timedelta(days=days)

    adapter = _build_adapter(api_key)
    try:
        from rosteriq.data_feeds.base import Location

        location = Location(latitude=0, longitude=0)
        signals = await adapter.fetch_signals(
            location=location,
            start_date=today,
            end_date=end_date,
            venue_id=ft_venue_id,
        )
    except Exception as e:
        logger.error(f"{DISPLAY_NAME} forecast fetch failed for venue {venue_id}: {e}")
        raise HTTPException(status_code=502, detail=f"{DISPLAY_NAME} API error: {e}")
    finally:
        if hasattr(adapter, "__aexit__"):
            await adapter.__aexit__(None, None, None)

    # Group signals by date for daily forecast
    daily_forecast = {}
    for signal in signals:
        signal_date = signal.signal_date.isoformat()
        raw = signal.raw_data or {} if hasattr(signal, "raw_data") else {}
        snapshot = raw.get("snapshot", {})

        if signal_date not in daily_forecast:
            daily_forecast[signal_date] = {
                "date": signal_date,
                "total_events": snapshot.get("total_events", 0),
                "total_guests": snapshot.get("total_guests", 0),
                "estimated_extra_staff": snapshot.get("estimated_extra_staff", 0),
                "event_types": snapshot.get("event_types", []),
                "confidence": signal.confidence,
                "strength": signal.strength.value if hasattr(signal.strength, "value") else str(signal.strength),
                "description": signal.description,
            }

    forecast_days = sorted(daily_forecast.values(), key=lambda x: x["date"])

    return {
        "status": "success",
        "provider": "function_tracker",
        "venue_id": venue_id,
        "start_date": today.isoformat(),
        "end_date": end_date.isoformat(),
        "days_with_events": len(forecast_days),
        "forecast": forecast_days,
    }


# ============================================================================
# Event Detail — Get single event detail
# ============================================================================


@router.get("/event/{event_id}")
async def get_event_detail(
    event_id: str = Path(..., description="Function Tracker event ID"),
    venue_id: str = Query(..., description="RosterIQ venue ID"),
) -> dict:
    """
    Get detailed information for a single event/function.

    Returns full event data including catering, beverage, equipment,
    running sheet, and staff requirements.
    """
    install = _get_install_or_404(venue_id)

    tokens = install.get("tokens", {})
    api_key = tokens.get("api_key", "")
    ft_venue_id = tokens.get("ft_venue_id", venue_id)

    if not api_key:
        raise HTTPException(
            status_code=401,
            detail=f"{DISPLAY_NAME} API key missing. Please re-install.",
        )

    adapter = _build_adapter(api_key)
    try:
        event = await adapter.fetch_event_detail(ft_venue_id, event_id)
    except Exception as e:
        logger.error(f"{DISPLAY_NAME} event detail fetch failed: {e}")
        raise HTTPException(status_code=502, detail=f"{DISPLAY_NAME} API error: {e}")
    finally:
        if hasattr(adapter, "__aexit__"):
            await adapter.__aexit__(None, None, None)

    return {
        "status": "success",
        "provider": "function_tracker",
        "venue_id": venue_id,
        "event_id": event_id,
        "event": event,
    }
