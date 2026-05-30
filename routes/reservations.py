"""
Reservation/booking system integration API routes.

Unified endpoints for connecting, syncing, and querying reservation data
from NowBookIt, ResDiary, OpenTable, and BookitLive.

Routes:
    POST /api/reservations/{provider}/install     -- Connect a reservation provider
    POST /api/reservations/{provider}/uninstall   -- Disconnect a reservation provider
    GET  /api/reservations/{provider}/status       -- Check connection status
    POST /api/reservations/{provider}/sync         -- Pull reservations for a date range
    GET  /api/reservations/{provider}/today        -- Get today's reservations
    GET  /api/reservations/{provider}/forecast     -- Get upcoming reservation forecast
"""

import logging
from datetime import date, datetime, timedelta
from typing import Optional

from fastapi import APIRouter, HTTPException, Path, Query
from pydantic import BaseModel, Field

from rosteriq.database import get_db
from rosteriq.data_feeds.reservations import (
    NowBookItAdapter,
    ResDiaryAdapter,
    OpenTableAdapter,
    BookitLiveAdapter,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/reservations", tags=["reservations"])

# Valid reservation providers
VALID_PROVIDERS = {"nowbookit", "resdiary", "opentable", "bookitlive"}

# Key prefix for plugin_installs table
RESERVATION_PREFIX_MAP = {
    "nowbookit": "nowbookit_reservations_",
    "resdiary": "resdiary_reservations_",
    "opentable": "opentable_reservations_",
    "bookitlive": "bookitlive_reservations_",
}

# Human-readable provider names
PROVIDER_DISPLAY_NAMES = {
    "nowbookit": "NowBookIt",
    "resdiary": "ResDiary",
    "opentable": "OpenTable",
    "bookitlive": "BookitLive",
}


# ============================================================================
# Pydantic Request Models
# ============================================================================


class ReservationInstallRequest(BaseModel):
    venue_id: str = Field(..., description="RosterIQ venue ID")
    api_key: str = Field(..., description="API key or access token for the provider")
    provider_venue_id: Optional[str] = Field(
        default=None,
        description="Provider-specific venue/restaurant ID (if different from RosterIQ venue ID)",
    )


class ReservationUninstallRequest(BaseModel):
    venue_id: str = Field(..., description="RosterIQ venue ID to disconnect")


class ReservationSyncRequest(BaseModel):
    venue_id: str = Field(..., description="RosterIQ venue ID")
    start_date: date = Field(..., description="Start date (YYYY-MM-DD)")
    end_date: date = Field(..., description="End date (YYYY-MM-DD)")


# ============================================================================
# Helpers
# ============================================================================


def _validate_provider(provider: str) -> str:
    """Validate that the provider is supported."""
    provider = provider.lower()
    if provider not in VALID_PROVIDERS:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid provider '{provider}'. Must be one of: {', '.join(sorted(VALID_PROVIDERS))}",
        )
    return provider


def _org_key(provider: str, venue_id: str) -> str:
    """Generate the organisation_id key used in plugin_installs for a reservation provider."""
    return f"{RESERVATION_PREFIX_MAP[provider]}{venue_id}"


def _get_install_or_404(provider: str, venue_id: str) -> dict:
    """Fetch the install record for a provider/venue, or raise 404."""
    db = get_db()
    install = db.get_plugin_install(_org_key(provider, venue_id))
    if not install or install.get("status") == "uninstalled":
        display_name = PROVIDER_DISPLAY_NAMES.get(provider, provider)
        raise HTTPException(
            status_code=404,
            detail=f"No active {display_name} connection for venue {venue_id}",
        )
    return install


def _build_adapter(provider: str, api_key: str):
    """Instantiate the correct adapter based on provider name."""
    adapter_map = {
        "nowbookit": NowBookItAdapter,
        "resdiary": ResDiaryAdapter,
        "opentable": OpenTableAdapter,
        "bookitlive": BookitLiveAdapter,
    }
    adapter_cls = adapter_map[provider]
    return adapter_cls(api_key=api_key)


# ============================================================================
# Install (Connect)
# ============================================================================


@router.post("/{provider}/install")
async def install(
    body: ReservationInstallRequest,
    provider: str = Path(..., description="Reservation provider"),
) -> dict:
    """
    Connect a reservation provider for a venue.

    Stores API credentials and verifies connectivity by checking
    the provider's health endpoint.
    """
    provider = _validate_provider(provider)
    display_name = PROVIDER_DISPLAY_NAMES[provider]
    db = get_db()
    org_key = _org_key(provider, body.venue_id)

    # Verify connectivity
    adapter = _build_adapter(provider, body.api_key)
    try:
        available = await adapter.is_available()
    except Exception as e:
        logger.error(f"{display_name} connectivity check failed for venue {body.venue_id}: {e}")
        available = False
    finally:
        if hasattr(adapter, '__aexit__'):
            await adapter.__aexit__(None, None, None)

    # Save the install record
    provider_venue_id = body.provider_venue_id or body.venue_id
    install_record = {
        "organisation_id": org_key,
        "venue_id": body.venue_id,
        "provider": provider,
        "status": "active",
        "tokens": {
            "api_key": body.api_key,
            "provider_venue_id": provider_venue_id,
        },
        "installed_at": datetime.utcnow(),
        "updated_at": datetime.utcnow(),
    }
    db.save_plugin_install(install_record)

    logger.info(f"{display_name} connected for venue {body.venue_id} (verified: {available})")
    return {
        "status": "success",
        "provider": provider,
        "venue_id": body.venue_id,
        "provider_venue_id": provider_venue_id,
        "connectivity_verified": available,
        "message": f"{display_name} connected successfully.",
    }


# ============================================================================
# Uninstall (Disconnect)
# ============================================================================


@router.post("/{provider}/uninstall")
async def uninstall(
    body: ReservationUninstallRequest,
    provider: str = Path(..., description="Reservation provider"),
) -> dict:
    """
    Remove the reservation provider connection for a venue.
    """
    provider = _validate_provider(provider)
    display_name = PROVIDER_DISPLAY_NAMES[provider]
    install = _get_install_or_404(provider, body.venue_id)

    install["status"] = "uninstalled"
    install["tokens"] = {}
    install["updated_at"] = datetime.utcnow()

    db = get_db()
    db.save_plugin_install(install)

    logger.info(f"{display_name} uninstalled for venue {body.venue_id}")
    return {
        "status": "success",
        "provider": provider,
        "venue_id": body.venue_id,
        "message": f"{display_name} connection removed.",
    }


# ============================================================================
# Status Check
# ============================================================================


@router.get("/{provider}/status")
async def get_status(
    provider: str = Path(..., description="Reservation provider"),
    venue_id: str = Query(..., description="RosterIQ venue ID"),
) -> dict:
    """
    Check whether a venue has an active reservation provider connection.

    Returns connection status and metadata.
    """
    provider = _validate_provider(provider)
    display_name = PROVIDER_DISPLAY_NAMES[provider]
    db = get_db()
    install = db.get_plugin_install(_org_key(provider, venue_id))

    if not install:
        return {
            "connected": False,
            "provider": provider,
            "provider_name": display_name,
            "venue_id": venue_id,
            "status": "not_installed",
        }

    tokens = install.get("tokens", {})
    return {
        "connected": install.get("status") == "active",
        "provider": provider,
        "provider_name": display_name,
        "venue_id": venue_id,
        "status": install.get("status", "unknown"),
        "provider_venue_id": tokens.get("provider_venue_id"),
        "installed_at": install.get("installed_at"),
        "updated_at": install.get("updated_at"),
    }


# ============================================================================
# Sync — Pull reservation data for a date range
# ============================================================================


@router.post("/{provider}/sync")
async def sync_reservations(
    body: ReservationSyncRequest,
    provider: str = Path(..., description="Reservation provider"),
) -> dict:
    """
    Pull reservation data from the provider for a date range.

    Returns demand signals (bookings, covers, party sizes) aggregated per day.
    """
    provider = _validate_provider(provider)
    display_name = PROVIDER_DISPLAY_NAMES[provider]
    install = _get_install_or_404(provider, body.venue_id)

    tokens = install.get("tokens", {})
    api_key = tokens.get("api_key", "")
    provider_venue_id = tokens.get("provider_venue_id", body.venue_id)

    if not api_key:
        raise HTTPException(
            status_code=401,
            detail=f"{display_name} API key missing. Please re-install.",
        )

    adapter = _build_adapter(provider, api_key)
    try:
        from rosteriq.data_feeds.base import Location
        location = Location(lat=0, lon=0)  # Location not needed for API-based fetches

        signals = await adapter.fetch_signals(
            location=location,
            start_date=body.start_date,
            end_date=body.end_date,
            venue_id=provider_venue_id,
        )
    except Exception as e:
        logger.error(
            f"{display_name} sync failed for venue {body.venue_id} "
            f"({body.start_date} to {body.end_date}): {e}"
        )
        raise HTTPException(status_code=502, detail=f"{display_name} API error: {e}")
    finally:
        if hasattr(adapter, '__aexit__'):
            await adapter.__aexit__(None, None, None)

    # Convert signals to serialisable dicts
    signal_data = []
    for signal in signals:
        signal_dict = {
            "date": signal.signal_date.isoformat() if hasattr(signal.signal_date, 'isoformat') else str(signal.signal_date),
            "hour": signal.signal_hour,
            "strength": signal.strength.value if hasattr(signal.strength, 'value') else str(signal.strength),
            "confidence": signal.confidence,
            "description": signal.description,
            "value": signal.value,
        }
        if hasattr(signal, 'raw_data') and signal.raw_data:
            snapshot = signal.raw_data.get("snapshot", {})
            signal_dict["total_bookings"] = snapshot.get("total_bookings", 0)
            signal_dict["total_covers"] = snapshot.get("total_covers", 0)
            signal_dict["avg_party_size"] = snapshot.get("avg_party_size", 0)
            signal_dict["large_parties"] = snapshot.get("large_parties", 0)
            signal_dict["no_show_adjusted_covers"] = snapshot.get("no_show_adjusted_covers", 0)
        signal_data.append(signal_dict)

    logger.info(
        f"Synced {len(signals)} signals from {display_name} for venue {body.venue_id} "
        f"({body.start_date} to {body.end_date})"
    )
    return {
        "status": "success",
        "provider": provider,
        "venue_id": body.venue_id,
        "start_date": body.start_date.isoformat(),
        "end_date": body.end_date.isoformat(),
        "count": len(signals),
        "signals": signal_data,
    }


# ============================================================================
# Today — Get today's reservations
# ============================================================================


@router.get("/{provider}/today")
async def get_today(
    provider: str = Path(..., description="Reservation provider"),
    venue_id: str = Query(..., description="RosterIQ venue ID"),
) -> dict:
    """
    Get today's reservations (covers, times, party sizes).

    Convenience endpoint that fetches signals for today only.
    """
    provider = _validate_provider(provider)
    display_name = PROVIDER_DISPLAY_NAMES[provider]
    install = _get_install_or_404(provider, venue_id)

    tokens = install.get("tokens", {})
    api_key = tokens.get("api_key", "")
    provider_venue_id = tokens.get("provider_venue_id", venue_id)

    if not api_key:
        raise HTTPException(
            status_code=401,
            detail=f"{display_name} API key missing. Please re-install.",
        )

    today = date.today()
    adapter = _build_adapter(provider, api_key)
    try:
        from rosteriq.data_feeds.base import Location
        location = Location(lat=0, lon=0)

        signals = await adapter.fetch_signals(
            location=location,
            start_date=today,
            end_date=today,
            venue_id=provider_venue_id,
        )
    except Exception as e:
        logger.error(f"{display_name} today fetch failed for venue {venue_id}: {e}")
        raise HTTPException(status_code=502, detail=f"{display_name} API error: {e}")
    finally:
        if hasattr(adapter, '__aexit__'):
            await adapter.__aexit__(None, None, None)

    # Aggregate today's data
    total_bookings = 0
    total_covers = 0
    total_large_parties = 0
    hourly_breakdown = []

    for signal in signals:
        raw = signal.raw_data or {} if hasattr(signal, 'raw_data') else {}
        snapshot = raw.get("snapshot", {})
        total_bookings += snapshot.get("total_bookings", 0)
        total_covers += snapshot.get("total_covers", 0)
        total_large_parties += snapshot.get("large_parties", 0)

        hourly_breakdown.append({
            "hour": signal.signal_hour,
            "bookings": snapshot.get("total_bookings", 0),
            "covers": snapshot.get("total_covers", 0),
            "avg_party_size": round(snapshot.get("avg_party_size", 0), 1),
        })

    return {
        "status": "success",
        "provider": provider,
        "venue_id": venue_id,
        "date": today.isoformat(),
        "total_bookings": total_bookings,
        "total_covers": total_covers,
        "large_parties": total_large_parties,
        "avg_party_size": round(total_covers / total_bookings, 1) if total_bookings > 0 else 0,
        "hourly_breakdown": hourly_breakdown,
    }


# ============================================================================
# Forecast — Get upcoming reservation forecast
# ============================================================================


@router.get("/{provider}/forecast")
async def get_forecast(
    provider: str = Path(..., description="Reservation provider"),
    venue_id: str = Query(..., description="RosterIQ venue ID"),
    days: int = Query(default=7, ge=1, le=30, description="Number of days to forecast"),
) -> dict:
    """
    Get upcoming reservation forecast for the next N days.

    Fetches signals for the next `days` days and returns per-day summaries
    with confidence scores and demand indicators.
    """
    provider = _validate_provider(provider)
    display_name = PROVIDER_DISPLAY_NAMES[provider]
    install = _get_install_or_404(provider, venue_id)

    tokens = install.get("tokens", {})
    api_key = tokens.get("api_key", "")
    provider_venue_id = tokens.get("provider_venue_id", venue_id)

    if not api_key:
        raise HTTPException(
            status_code=401,
            detail=f"{display_name} API key missing. Please re-install.",
        )

    today = date.today()
    end_date = today + timedelta(days=days)

    adapter = _build_adapter(provider, api_key)
    try:
        from rosteriq.data_feeds.base import Location
        location = Location(lat=0, lon=0)

        signals = await adapter.fetch_signals(
            location=location,
            start_date=today,
            end_date=end_date,
            venue_id=provider_venue_id,
        )
    except Exception as e:
        logger.error(f"{display_name} forecast fetch failed for venue {venue_id}: {e}")
        raise HTTPException(status_code=502, detail=f"{display_name} API error: {e}")
    finally:
        if hasattr(adapter, '__aexit__'):
            await adapter.__aexit__(None, None, None)

    # Group signals by date for daily forecast
    daily_forecast = {}
    for signal in signals:
        signal_date = signal.signal_date.isoformat() if hasattr(signal.signal_date, 'isoformat') else str(signal.signal_date)
        if signal_date not in daily_forecast:
            daily_forecast[signal_date] = {
                "date": signal_date,
                "total_bookings": 0,
                "total_covers": 0,
                "confidence": signal.confidence,
                "strength": signal.strength.value if hasattr(signal.strength, 'value') else str(signal.strength),
                "description": signal.description,
            }

        raw = signal.raw_data or {} if hasattr(signal, 'raw_data') else {}
        snapshot = raw.get("snapshot", {})
        daily_forecast[signal_date]["total_bookings"] += snapshot.get("total_bookings", 0)
        daily_forecast[signal_date]["total_covers"] += snapshot.get("total_covers", 0)

    forecast_days = sorted(daily_forecast.values(), key=lambda x: x["date"])

    return {
        "status": "success",
        "provider": provider,
        "venue_id": venue_id,
        "start_date": today.isoformat(),
        "end_date": end_date.isoformat(),
        "days_count": len(forecast_days),
        "forecast": forecast_days,
    }
