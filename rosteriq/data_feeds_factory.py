"""
Data Feeds Factory for RosterIQ
==============================

Factory functions to instantiate live or demo adapters based on environment
configuration. Provides single entry point for POS, bookings, and other data feeds.

Env vars:
  - ROSTERIQ_POS_BACKEND (swiftpos | demo, default demo)
  - SWIFTPOS_API_URL
  - SWIFTPOS_CLERK_ID
  - SWIFTPOS_CLIENT_ID
  - SWIFTPOS_CUSTOMER_ID
  - SWIFTPOS_LOCATION_ID
  - SWIFTPOS_LOCATION_NAME

  - ROSTERIQ_BOOKINGS_BACKEND (nowbookit_api | demo, default demo)
  - NOWBOOKIT_API_KEY
  - NOWBOOKIT_VENUE_ID
  - NOWBOOKIT_BASE_URL (optional)
"""

import logging
import os
from typing import Optional, Dict, Any

logger = logging.getLogger("rosteriq.data_feeds_factory")

AU_TZ_OFFSET = 10  # AEST


# ============================================================================
# POS Adapter Factory
# ============================================================================

def get_pos_adapter():
    """
    Get POS adapter instance based on ROSTERIQ_POS_BACKEND env var.

    Returns:
        SwiftPOSAdapter (live) or SwiftPOSLiveAdapter (with demo fallback)
    """
    from rosteriq.data_feeds.swiftpos import (
        create_swiftpos_adapter,
        SwiftPOSAdapter,
    )

    backend = os.getenv("ROSTERIQ_POS_BACKEND", "demo").lower()

    if backend == "swiftpos":
        api_url = os.getenv("SWIFTPOS_API_URL")
        clerk_id = os.getenv("SWIFTPOS_CLERK_ID")
        client_id = os.getenv("SWIFTPOS_CLIENT_ID")
        customer_id = os.getenv("SWIFTPOS_CUSTOMER_ID")
        location_id = os.getenv("SWIFTPOS_LOCATION_ID", "LOC001")
        location_name = os.getenv("SWIFTPOS_LOCATION_NAME", "Venue")

        if not all([api_url, clerk_id, client_id, customer_id]):
            logger.warning(
                "SwiftPOS backend requested but credentials incomplete; falling back to demo"
            )
            return _get_demo_pos_adapter()

        try:
            adapter = create_swiftpos_adapter(
                api_url=api_url,
                clerk_id=clerk_id,
                client_id=client_id,
                customer_id=customer_id,
                location_id=location_id,
                location_name=location_name,
            )
            logger.info(f"Using live SwiftPOS adapter for {location_name} ({location_id})")
            return adapter
        except Exception as e:
            logger.warning(f"Failed to initialize SwiftPOS adapter: {e}; falling back to demo")
            return _get_demo_pos_adapter()

    return _get_demo_pos_adapter()


def _get_demo_pos_adapter():
    """Get demo POS adapter with realistic Brisbane hospitality data."""
    from rosteriq.data_feeds.swiftpos import create_swiftpos_adapter

    # Create a demo adapter by passing a dummy URL and letting it fall back
    adapter = create_swiftpos_adapter(
        api_url="https://demo.swiftpos.local/v1",  # Won't be used
        clerk_id="DEMO",
        client_id="DEMO",
        customer_id="DEMO",
        location_id="DEMO001",
        location_name="Demo Venue",
    )
    logger.info("Using demo POS adapter")
    return adapter


# ============================================================================
# Bookings Adapter Factory
# ============================================================================

def get_bookings_adapter():
    """
    Get bookings adapter instance based on ROSTERIQ_BOOKINGS_BACKEND env var.

    Returns:
        NowBookItAdapter or demo bookings adapter
    """
    from rosteriq.data_feeds.nowbookit import (
        create_nowbookit_adapter,
        NowBookItAdapter,
    )

    backend = os.getenv("ROSTERIQ_BOOKINGS_BACKEND", "demo").lower()

    if backend in ["nowbookit", "nowbookit_api"]:
        api_key = os.getenv("NOWBOOKIT_API_KEY")
        venue_id = os.getenv("NOWBOOKIT_VENUE_ID")
        base_url = os.getenv("NOWBOOKIT_BASE_URL")

        if not api_key or not venue_id:
            logger.warning(
                "NowBookIt backend requested but credentials incomplete; falling back to demo"
            )
            return _get_demo_bookings_adapter()

        try:
            kwargs = {
                "api_key": api_key,
                "venue_id": venue_id,
            }
            if base_url:
                kwargs["base_url"] = base_url

            adapter = create_nowbookit_adapter(**kwargs)
            logger.info(f"Using live NowBookIt adapter for venue {venue_id}")
            return adapter
        except Exception as e:
            logger.warning(f"Failed to initialize NowBookIt adapter: {e}; falling back to demo")
            return _get_demo_bookings_adapter()

    return _get_demo_bookings_adapter()


def _get_demo_bookings_adapter():
    """Get demo bookings adapter with realistic reservation patterns."""
    from rosteriq.data_feeds.nowbookit import create_nowbookit_adapter

    adapter = create_nowbookit_adapter(
        api_key="demo_key",
        venue_id="demo_venue",
    )
    logger.info("Using demo bookings adapter")
    return adapter


# ============================================================================
# In-Memory Stores for CSV-Uploaded Data
# ============================================================================

class BookingsStore:
    """In-memory store for bookings (e.g., from CSV upload)."""

    def __init__(self):
        self._bookings: list[Dict[str, Any]] = []

    def add_bookings(self, bookings: list[Dict[str, Any]]) -> None:
        """Add bookings to store."""
        self._bookings.extend(bookings)

    def get_bookings(self, from_date: str = None, to_date: str = None) -> list[Dict[str, Any]]:
        """Get bookings, optionally filtered by date range."""
        if not from_date or not to_date:
            return self._bookings

        result = []
        for booking in self._bookings:
            booking_date = booking.get("date", "")
            if from_date <= booking_date <= to_date:
                result.append(booking)
        return result

    def clear(self) -> None:
        """Clear all stored bookings."""
        self._bookings.clear()

    def count(self) -> int:
        """Get count of stored bookings."""
        return len(self._bookings)


# Global store instance
_bookings_store: Optional[BookingsStore] = None


def get_bookings_store() -> BookingsStore:
    """Get the global in-memory bookings store."""
    global _bookings_store
    if _bookings_store is None:
        _bookings_store = BookingsStore()
    return _bookings_store
