"""
Availability API Router for RosterIQ.

Exposes employee availability windows from Tanda via:
- GET /api/v1/tanda/availability/{venue_id}

Supports optional employee_id query parameter to filter by employee.

Data mode (live vs demo) is determined by ROSTERIQ_DATA_MODE environment variable.
"""

import logging
import os
from typing import List, Optional, Dict, Any

from fastapi import APIRouter, HTTPException, Query, Request
from pydantic import BaseModel

from rosteriq.tanda_availability import (
    AvailabilityWindow,
    TandaAvailabilityReader,
    DemoAvailabilityReader,
)
from rosteriq.tanda_adapter import get_tanda_adapter, DemoTandaAdapter, TandaAdapter

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

logger = logging.getLogger(__name__)

router = APIRouter(prefix="", tags=["tanda"])


# ============================================================================
# Response Models
# ============================================================================

class AvailabilityWindowResponse(BaseModel):
    """Serializable availability window."""
    employee_id: str
    day_of_week: int
    start_time: str
    end_time: str
    recurring: bool = True
    valid_from: Optional[str] = None
    valid_until: Optional[str] = None
    notes: Optional[str] = None

    class Config:
        """Pydantic config."""
        from_attributes = True


class GetAvailabilityResponse(BaseModel):
    """Response for availability endpoint."""
    venue_id: str
    source: str  # "tanda" or "demo"
    windows: List[AvailabilityWindowResponse]


# ============================================================================
# Helpers
# ============================================================================

async def _get_availability_reader():
    """
    Get appropriate availability reader based on data mode.

    Returns:
        TandaAvailabilityReader or DemoAvailabilityReader
    """
    data_mode = os.getenv("ROSTERIQ_DATA_MODE", "demo").lower()
    adapter = get_tanda_adapter()

    if isinstance(adapter, DemoTandaAdapter):
        logger.debug("Using DemoAvailabilityReader")
        return DemoAvailabilityReader(adapter), "demo"
    else:
        logger.debug("Using TandaAvailabilityReader")
        return TandaAvailabilityReader(adapter), "live"


# ============================================================================
# Endpoints
# ============================================================================

@router.get("/api/v1/tanda/availability/{venue_id}", response_model=GetAvailabilityResponse)
async def get_availability(
    venue_id: str,
    request: Request,
    employee_id: Optional[str] = Query(None, description="Optional employee ID to filter by"),
) -> GetAvailabilityResponse:
    """
    Retrieve employee availability windows for a venue.

    Args:
        venue_id: Venue/organization ID
        employee_id: Optional employee ID to filter (if not provided, returns all)

    Returns:
        GetAvailabilityResponse with availability windows and data source

    Raises:
        HTTPException 400: If employee_id format is invalid
        HTTPException 502: If adapter raises an exception
    """
    await _gate(request, "L1_SUPERVISOR")
    try:
        # Validate employee_id format if provided
        if employee_id and not isinstance(employee_id, str):
            raise HTTPException(
                status_code=400,
                detail=f"Invalid employee_id format: {employee_id}",
            )

        # Get appropriate reader
        reader, source = await _get_availability_reader()

        # Fetch availability
        windows: List[AvailabilityWindow] = await reader.get_availability(
            org_id=venue_id,
            employee_id=employee_id,
        )

        # Convert to response model
        window_responses = [
            AvailabilityWindowResponse(
                employee_id=w.employee_id,
                day_of_week=w.day_of_week,
                start_time=w.start_time,
                end_time=w.end_time,
                recurring=w.recurring,
                valid_from=w.valid_from.isoformat() if w.valid_from else None,
                valid_until=w.valid_until.isoformat() if w.valid_until else None,
                notes=w.notes,
            )
            for w in windows
        ]

        return GetAvailabilityResponse(
            venue_id=venue_id,
            source=source,
            windows=window_responses,
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error retrieving availability: {e}")
        raise HTTPException(
            status_code=502,
            detail=f"Availability service error: {str(e)}",
        )
