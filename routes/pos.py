"""
POS system integration API routes.

Unified connect/sync endpoints for all supported POS providers:
SwiftPOS, Lightspeed, and Kounta.

Routes:
    POST /api/pos/{provider}/install      -- Initiate connection (API key or OAuth)
    POST /api/pos/{provider}/uninstall    -- Disconnect POS
    GET  /api/pos/{provider}/status       -- Check connection status
    POST /api/pos/{provider}/sync/sales   -- Pull sales data for a date range
    GET  /api/pos/{provider}/live         -- Get current live sales data
    POST /api/pos/{provider}/import/csv   -- Upload CSV sales data as fallback
"""

import base64
import csv
import io
import logging
from datetime import date, datetime
from enum import Enum
from typing import Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from rosteriq.database import get_db
from rosteriq.data_feeds.swiftpos import SwiftPOSAdapter
from rosteriq.data_feeds.lightspeed import LightspeedAdapter
from rosteriq.data_feeds.kounta import KountaAdapter
from rosteriq.data_feeds.base import Location

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/pos", tags=["pos"])


# ============================================================================
# Enums
# ============================================================================


class PosProvider(str, Enum):
    swiftpos = "swiftpos"
    lightspeed = "lightspeed"
    kounta = "kounta"


# Key prefix for plugin_installs table, per provider
PROVIDER_PREFIX = {
    PosProvider.swiftpos: "swiftpos_",
    PosProvider.lightspeed: "lightspeed_",
    PosProvider.kounta: "kounta_",
}


# ============================================================================
# Pydantic Request Models
# ============================================================================


class PosInstallRequest(BaseModel):
    """Request to connect a POS provider to a venue."""
    provider: PosProvider = Field(..., description="POS provider name")
    venue_id: str = Field(..., description="RosterIQ venue ID")
    api_key: Optional[str] = Field(default=None, description="API key (for Kounta or SwiftPOS)")
    client_id: Optional[str] = Field(default=None, description="OAuth client ID (for SwiftPOS or Lightspeed)")
    client_secret: Optional[str] = Field(default=None, description="OAuth client secret")
    refresh_token: Optional[str] = Field(default=None, description="OAuth refresh token (for Lightspeed)")


class PosUninstallRequest(BaseModel):
    """Request to disconnect a POS provider from a venue."""
    provider: PosProvider = Field(..., description="POS provider name")
    venue_id: str = Field(..., description="RosterIQ venue ID")


class PosSyncRequest(BaseModel):
    """Request to pull sales data for a date range."""
    venue_id: str = Field(..., description="RosterIQ venue ID")
    start_date: date = Field(..., description="Start date (YYYY-MM-DD)")
    end_date: date = Field(..., description="End date (YYYY-MM-DD)")


class PosCsvImportRequest(BaseModel):
    """Request to import CSV sales data as a fallback."""
    venue_id: str = Field(..., description="RosterIQ venue ID")
    csv_data: str = Field(..., description="Base64-encoded CSV sales data")


# ============================================================================
# Helpers
# ============================================================================


def _org_key(provider: PosProvider, venue_id: str) -> str:
    """Generate the organisation_id key used in plugin_installs for a POS provider."""
    return f"{PROVIDER_PREFIX[provider]}{venue_id}"


def _get_install_or_404(provider: PosProvider, venue_id: str) -> dict:
    """Fetch the POS install record for a provider+venue, or raise 404."""
    db = get_db()
    install = db.get_plugin_install(_org_key(provider, venue_id))
    if not install or install.get("status") == "uninstalled":
        raise HTTPException(
            status_code=404,
            detail=f"No active {provider.value} connection for venue {venue_id}",
        )
    return install


def _build_adapter(provider: PosProvider, install: dict):
    """
    Instantiate the correct POS adapter from stored credentials.

    Returns an adapter instance ready for use as an async context manager.
    """
    tokens = install.get("tokens", {})

    if provider == PosProvider.swiftpos:
        return SwiftPOSAdapter(
            client_id=tokens.get("client_id"),
            client_secret=tokens.get("client_secret"),
        )
    elif provider == PosProvider.lightspeed:
        return LightspeedAdapter(
            client_id=tokens.get("client_id"),
            client_secret=tokens.get("client_secret"),
            refresh_token=tokens.get("refresh_token"),
        )
    elif provider == PosProvider.kounta:
        return KountaAdapter(
            api_key=tokens.get("api_key"),
        )
    else:
        raise HTTPException(status_code=400, detail=f"Unsupported provider: {provider.value}")


# ============================================================================
# Install (Connect)
# ============================================================================


@router.post("/{provider}/install")
async def install(provider: PosProvider, body: PosInstallRequest) -> dict:
    """
    Initiate a POS connection for a venue.

    Accepts API key or OAuth credentials depending on the provider.
    Validates connectivity before saving.
    """
    # Build tokens dict from supplied credentials
    tokens: dict = {}

    if provider == PosProvider.kounta:
        if not body.api_key:
            raise HTTPException(
                status_code=400,
                detail="Kounta requires an api_key",
            )
        tokens["api_key"] = body.api_key

    elif provider == PosProvider.swiftpos:
        if not body.client_id or not body.client_secret:
            raise HTTPException(
                status_code=400,
                detail="SwiftPOS requires client_id and client_secret",
            )
        tokens["client_id"] = body.client_id
        tokens["client_secret"] = body.client_secret

    elif provider == PosProvider.lightspeed:
        if not body.client_id or not body.client_secret:
            raise HTTPException(
                status_code=400,
                detail="Lightspeed requires client_id and client_secret",
            )
        tokens["client_id"] = body.client_id
        tokens["client_secret"] = body.client_secret
        if body.refresh_token:
            tokens["refresh_token"] = body.refresh_token

    # Validate connectivity
    test_install = {"tokens": tokens}
    adapter = _build_adapter(provider, test_install)
    try:
        async with adapter:
            available = await adapter.is_available()
    except Exception as e:
        logger.error(f"{provider.value} connectivity check failed for venue {body.venue_id}: {e}")
        available = False

    # Save the install record regardless (mark as active if connected, pending otherwise)
    db = get_db()
    install_record = {
        "organisation_id": _org_key(provider, body.venue_id),
        "venue_id": body.venue_id,
        "provider": provider.value,
        "status": "active" if available else "pending",
        "tokens": tokens,
        "installed_at": datetime.utcnow(),
        "updated_at": datetime.utcnow(),
    }
    db.save_plugin_install(install_record)

    status_msg = "connected" if available else "credentials saved but connectivity check failed"
    logger.info(f"{provider.value} install for venue {body.venue_id}: {status_msg}")

    return {
        "status": "active" if available else "pending",
        "provider": provider.value,
        "venue_id": body.venue_id,
        "connected": available,
        "message": f"{provider.value} {status_msg}.",
    }


# ============================================================================
# Uninstall (Disconnect)
# ============================================================================


@router.post("/{provider}/uninstall")
async def uninstall(provider: PosProvider, body: PosUninstallRequest) -> dict:
    """
    Remove the POS connection for a venue.
    """
    install = _get_install_or_404(provider, body.venue_id)

    install["status"] = "uninstalled"
    install["tokens"] = {}
    install["updated_at"] = datetime.utcnow()

    db = get_db()
    db.save_plugin_install(install)

    logger.info(f"{provider.value} uninstalled for venue {body.venue_id}")
    return {
        "status": "success",
        "provider": provider.value,
        "venue_id": body.venue_id,
        "message": f"{provider.value} connection removed.",
    }


# ============================================================================
# Status
# ============================================================================


@router.get("/{provider}/status")
async def get_status(
    provider: PosProvider,
    venue_id: str = Query(..., description="RosterIQ venue ID"),
) -> dict:
    """
    Check whether a venue has an active POS connection for a given provider.
    """
    db = get_db()
    install = db.get_plugin_install(_org_key(provider, venue_id))

    if not install:
        return {
            "connected": False,
            "provider": provider.value,
            "venue_id": venue_id,
            "status": "not_installed",
        }

    return {
        "connected": install.get("status") == "active",
        "provider": provider.value,
        "venue_id": venue_id,
        "status": install.get("status", "unknown"),
        "installed_at": install.get("installed_at"),
        "updated_at": install.get("updated_at"),
    }


# ============================================================================
# Sync Sales
# ============================================================================


@router.post("/{provider}/sync/sales")
async def sync_sales(provider: PosProvider, body: PosSyncRequest) -> dict:
    """
    Pull sales data from a POS provider for a date range.

    Returns demand signals generated from the sales data.
    """
    install = _get_install_or_404(provider, body.venue_id)
    adapter = _build_adapter(provider, install)

    # Build a minimal Location for the adapter
    location = Location(latitude=-37.8136, longitude=144.9631)  # Default Melbourne

    try:
        async with adapter:
            signals = await adapter.fetch_signals(
                location=location,
                start_date=body.start_date,
                end_date=body.end_date,
                venue_id=body.venue_id,
            )
    except Exception as e:
        logger.error(
            f"{provider.value} sales sync failed for venue {body.venue_id} "
            f"({body.start_date} to {body.end_date}): {e}"
        )
        raise HTTPException(
            status_code=502,
            detail=f"{provider.value} API error: {e}",
        )

    logger.info(
        f"Synced {len(signals)} sales signals from {provider.value} for venue {body.venue_id} "
        f"({body.start_date} to {body.end_date})"
    )
    return {
        "status": "success",
        "provider": provider.value,
        "venue_id": body.venue_id,
        "start_date": body.start_date.isoformat(),
        "end_date": body.end_date.isoformat(),
        "count": len(signals),
        "signals": [
            {
                "date": s.signal_date.isoformat(),
                "hour": s.signal_hour,
                "strength": s.strength.value if hasattr(s.strength, "value") else str(s.strength),
                "description": s.description,
                "confidence": s.confidence,
            }
            for s in signals
        ],
    }


# ============================================================================
# Live Sales
# ============================================================================


@router.get("/{provider}/live")
async def get_live_sales(
    provider: PosProvider,
    venue_id: str = Query(..., description="RosterIQ venue ID"),
) -> dict:
    """
    Get current live sales data from the POS provider.

    Fetches today's sales data for real-time dashboard display.
    """
    install = _get_install_or_404(provider, venue_id)
    adapter = _build_adapter(provider, install)

    today = date.today()
    location = Location(latitude=-37.8136, longitude=144.9631)

    try:
        async with adapter:
            signals = await adapter.fetch_signals(
                location=location,
                start_date=today,
                end_date=today,
                venue_id=venue_id,
            )
    except Exception as e:
        logger.error(f"{provider.value} live sales fetch failed for venue {venue_id}: {e}")
        raise HTTPException(
            status_code=502,
            detail=f"{provider.value} API error: {e}",
        )

    # Summarise today's signals
    total_revenue = 0.0
    total_items = 0
    total_transactions = 0
    hourly_breakdown = []

    for s in signals:
        raw = s.raw_data.get("snapshot", {}) if s.raw_data else {}
        total_revenue += raw.get("total_revenue", 0.0)
        total_items += raw.get("total_items", 0)
        total_transactions += raw.get("transactions", 0)
        hourly_breakdown.append({
            "hour": s.signal_hour,
            "revenue": raw.get("total_revenue", 0.0),
            "items": raw.get("total_items", 0),
            "transactions": raw.get("transactions", 0),
        })

    return {
        "status": "success",
        "provider": provider.value,
        "venue_id": venue_id,
        "date": today.isoformat(),
        "total_revenue": round(total_revenue, 2),
        "total_items": total_items,
        "total_transactions": total_transactions,
        "hourly": hourly_breakdown,
    }


# ============================================================================
# CSV Import (Fallback)
# ============================================================================


@router.post("/{provider}/import/csv")
async def import_csv(provider: PosProvider, body: PosCsvImportRequest) -> dict:
    """
    Import CSV sales data as a fallback when the POS API is unavailable.

    Expects base64-encoded CSV with columns: date, hour, revenue, items, transactions.
    """
    # Decode CSV
    try:
        csv_bytes = base64.b64decode(body.csv_data)
        csv_text = csv_bytes.decode("utf-8")
    except Exception as e:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid base64 CSV data: {e}",
        )

    # Parse CSV
    reader = csv.DictReader(io.StringIO(csv_text))
    records = []
    row_count = 0

    required_columns = {"date", "hour", "revenue"}
    if reader.fieldnames:
        missing = required_columns - set(reader.fieldnames)
        if missing:
            raise HTTPException(
                status_code=400,
                detail=f"CSV missing required columns: {', '.join(missing)}. "
                       f"Expected columns: date, hour, revenue, items, transactions",
            )

    for row in reader:
        row_count += 1
        try:
            record = {
                "date": row.get("date", ""),
                "hour": int(row.get("hour", 0)),
                "revenue": float(row.get("revenue", 0.0)),
                "items": int(row.get("items", 0)),
                "transactions": int(row.get("transactions", 0)),
            }
            records.append(record)
        except (ValueError, TypeError) as e:
            logger.warning(f"Skipping CSV row {row_count}: {e}")
            continue

    if not records:
        raise HTTPException(
            status_code=400,
            detail="No valid records found in CSV data",
        )

    # Store the imported data in the database as a feed config entry
    db = get_db()
    import_record = {
        "organisation_id": _org_key(provider, body.venue_id),
        "venue_id": body.venue_id,
        "provider": provider.value,
        "import_type": "csv",
        "imported_at": datetime.utcnow(),
        "record_count": len(records),
        "records": records,
    }
    # Save as a supplementary data import
    try:
        db.save_feed_config(body.venue_id, f"{provider.value}_csv_import", import_record)
    except Exception as e:
        logger.error(f"Failed to save CSV import for {provider.value}: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to save imported data: {e}",
        )

    logger.info(
        f"Imported {len(records)} CSV records for {provider.value} "
        f"at venue {body.venue_id}"
    )
    return {
        "status": "success",
        "provider": provider.value,
        "venue_id": body.venue_id,
        "records_imported": len(records),
        "rows_processed": row_count,
        "message": f"Imported {len(records)} sales records from CSV.",
    }
