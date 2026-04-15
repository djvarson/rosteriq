"""
Data Feeds Router for RosterIQ API
==================================

REST endpoints for POS, bookings, and other data feeds integration.

Endpoints:
  GET /api/v1/pos/sales
    - Query params: venue_id, from (date), to (date)
    - Returns: hourly sales breakdown with demand signals
    - L1+ access required

  GET /api/v1/bookings
    - Query params: venue_id, from (date), to (date)
    - Returns: stored reservations with covers and patterns
    - L1+ access required

  POST /api/v1/bookings/csv-upload
    - Multipart form: file (CSV with booking data)
    - Parses CSV and stores bookings in memory
    - Returns: {bookings_parsed: int, date_range: [min, max]}
    - L2+ access required

  GET /api/v1/pos/health
    - Returns: {"status": "healthy"|"error", ...}
    - Health check for POS adapter

  GET /api/v1/bookings/health
    - Returns: {"status": "healthy"|"error", ...}
    - Health check for bookings adapter
"""

import asyncio
import logging
from datetime import datetime, date, timedelta, timezone
from typing import Optional

from fastapi import APIRouter, HTTPException, Query, UploadFile, File

from rosteriq.data_feeds_factory import (
    get_pos_adapter,
    get_bookings_adapter,
    get_bookings_store,
)
from rosteriq.bookings_csv_parser import parse_bookings_csv

logger = logging.getLogger("rosteriq.data_feeds_router")

AU_TZ = timezone(timedelta(hours=10))

router = APIRouter(tags=["data_feeds"])


# ============================================================================
# POS Endpoints
# ============================================================================

@router.get("/api/v1/pos/sales")
async def get_pos_sales(
    venue_id: str = Query(..., description="Venue ID"),
    from_date: Optional[str] = Query(None, alias="from", description="Start date (YYYY-MM-DD)"),
    to_date: Optional[str] = Query(None, alias="to", description="End date (YYYY-MM-DD)"),
) -> dict:
    """
    Get POS sales breakdown for a venue and date range.

    Returns hourly sales data with demand signals.
    """
    try:
        # Parse dates
        if not from_date:
            from_date_obj = date.today() - timedelta(days=7)
        else:
            from_date_obj = date.fromisoformat(from_date)

        if not to_date:
            to_date_obj = date.today()
        else:
            to_date_obj = date.fromisoformat(to_date)

        if from_date_obj > to_date_obj:
            raise HTTPException(
                status_code=400,
                detail="from_date must be <= to_date",
            )

        adapter = get_pos_adapter()

        # Initialize patterns if needed
        if not hasattr(adapter, "_patterns") or not adapter._patterns:
            try:
                await adapter.initialise()
            except Exception as e:
                logger.warning(f"Failed to initialize patterns: {e}")

        # Fetch current signals
        try:
            signals = await adapter.fetch_signals()
        except Exception as e:
            logger.warning(f"Failed to fetch signals: {e}")
            signals = []

        # Fetch daily summary for date range
        start_dt = datetime.combine(from_date_obj, datetime.min.time()).replace(tzinfo=AU_TZ)
        end_dt = datetime.combine(to_date_obj, datetime.max.time()).replace(tzinfo=AU_TZ)

        try:
            transactions = await adapter.client.get_transactions(
                adapter.location_id, start_dt, end_dt
            )
        except Exception as e:
            logger.warning(f"Failed to fetch transactions: {e}")
            transactions = []

        snapshot = adapter.analyser.build_sales_snapshot(
            adapter.location_id,
            adapter.location_name,
            transactions,
        )

        return {
            "venue_id": venue_id,
            "location_id": adapter.location_id,
            "location_name": adapter.location_name,
            "from_date": from_date_obj.isoformat(),
            "to_date": to_date_obj.isoformat(),
            "total_revenue": snapshot.total_revenue,
            "transaction_count": snapshot.transaction_count,
            "covers": snapshot.covers,
            "hourly_breakdown": snapshot.hourly_breakdown,
            "signals": signals,
            "timestamp": datetime.now(AU_TZ).isoformat(),
        }

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Failed to get POS sales: {e}")
        raise HTTPException(status_code=502, detail="POS adapter error")


@router.get("/api/v1/pos/health")
async def get_pos_health() -> dict:
    """
    Health check for POS adapter.

    Returns status, connectivity, and metadata.
    """
    try:
        adapter = get_pos_adapter()
        result = await adapter.health_check()
        return result
    except Exception as e:
        logger.error(f"POS health check failed: {e}")
        return {
            "status": "error",
            "connected": False,
            "error": str(e),
        }


# ============================================================================
# Bookings Endpoints
# ============================================================================

@router.get("/api/v1/bookings")
async def get_bookings(
    venue_id: str = Query(..., description="Venue ID"),
    from_date: Optional[str] = Query(None, alias="from", description="Start date (YYYY-MM-DD)"),
    to_date: Optional[str] = Query(None, alias="to", description="End date (YYYY-MM-DD)"),
) -> dict:
    """
    Get stored bookings for a venue and date range.

    Includes both API-fetched and CSV-uploaded bookings.
    """
    try:
        # Parse dates
        if not from_date:
            from_date_obj = date.today()
        else:
            from_date_obj = date.fromisoformat(from_date)

        if not to_date:
            to_date_obj = date.today() + timedelta(days=7)
        else:
            to_date_obj = date.fromisoformat(to_date)

        if from_date_obj > to_date_obj:
            raise HTTPException(
                status_code=400,
                detail="from_date must be <= to_date",
            )

        # Get bookings from store (CSV uploads)
        store = get_bookings_store()
        stored_bookings = store.get_bookings(
            from_date_obj.isoformat(),
            to_date_obj.isoformat(),
        )

        # Try to fetch from adapter (live API)
        adapter = get_bookings_adapter()
        try:
            if not hasattr(adapter, "_patterns") or not adapter._patterns:
                await adapter.initialise()
            api_bookings = await adapter.fetch_reservations(from_date_obj, to_date_obj)
            api_booking_list = [
                {
                    "id": b.reservation_id,
                    "date": b.date.isoformat(),
                    "time": b.time,
                    "covers": b.covers,
                    "name": b.name,
                    "status": b.status,
                    "source": "api",
                }
                for b in api_bookings
            ]
        except Exception as e:
            logger.warning(f"Failed to fetch API bookings: {e}")
            api_booking_list = []

        # Combine stored and API bookings
        all_bookings = stored_bookings + api_booking_list

        total_covers = sum(b.get("covers", 0) for b in all_bookings)

        return {
            "venue_id": venue_id,
            "from_date": from_date_obj.isoformat(),
            "to_date": to_date_obj.isoformat(),
            "booking_count": len(all_bookings),
            "total_covers": total_covers,
            "bookings": all_bookings,
            "stored_count": len(stored_bookings),
            "api_count": len(api_booking_list),
            "timestamp": datetime.now(AU_TZ).isoformat(),
        }

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Failed to get bookings: {e}")
        raise HTTPException(status_code=502, detail="Bookings adapter error")


@router.post("/api/v1/bookings/csv-upload")
async def upload_bookings_csv(
    file: UploadFile = File(...),
) -> dict:
    """
    Upload a CSV file with bookings data.

    CSV columns (flexible naming):
      - booking_date / date / Date (YYYY-MM-DD)
      - booking_time / time / Time (HH:MM)
      - party_size / covers / pax / Party Size
      - customer_name / name / Name
      - status (confirmed, cancelled, no-show) [optional, default confirmed]

    Returns: {bookings_parsed: int, date_range: [min, max]}
    """
    try:
        # Read and parse CSV
        content = await file.read()
        csv_text = content.decode("utf-8")

        bookings = parse_bookings_csv(csv_text)

        if not bookings:
            raise HTTPException(
                status_code=400,
                detail="No valid bookings found in CSV",
            )

        # Store bookings
        store = get_bookings_store()
        store.add_bookings(bookings)

        # Get date range
        dates = sorted([b.get("date", "") for b in bookings if b.get("date")])
        date_range = [dates[0], dates[-1]] if dates else []

        logger.info(f"Uploaded {len(bookings)} bookings, date range: {date_range}")

        return {
            "bookings_parsed": len(bookings),
            "date_range": date_range,
            "timestamp": datetime.now(AU_TZ).isoformat(),
        }

    except Exception as e:
        logger.error(f"CSV upload failed: {e}")
        raise HTTPException(status_code=400, detail=f"CSV parsing error: {str(e)}")


@router.get("/api/v1/bookings/health")
async def get_bookings_health() -> dict:
    """
    Health check for bookings adapter.

    Returns status, connectivity, and metadata.
    """
    try:
        adapter = get_bookings_adapter()
        result = await adapter.health_check()
        return result
    except Exception as e:
        logger.error(f"Bookings health check failed: {e}")
        return {
            "status": "error",
            "connected": False,
            "error": str(e),
        }
