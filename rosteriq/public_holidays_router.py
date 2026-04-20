"""
Public Holidays API Router for RosterIQ

Exposes public holiday management endpoints:
- GET /api/v1/holidays/{state}/{year} — Full holiday calendar for state/year (L1+)
- GET /api/v1/holidays/check/{state}/{date} — Is this date a public holiday? (L1+)
- GET /api/v1/holidays/{state}/upcoming — Upcoming holidays (L1+)
- GET /api/v1/holidays/penalty/{state}/{date} — Penalty multiplier for a date (L1+)
- POST /api/v1/holidays/custom — Add custom venue holiday (L2+)
- DELETE /api/v1/holidays/custom/{holiday_id} — Remove custom holiday (L2+)

All endpoints follow RosterIQ auth patterns with L1 (read) and L2 (write) gates.
"""

from __future__ import annotations

import logging
from datetime import date
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query, Request

try:
    from rosteriq.auth import require_access
except ImportError:
    require_access = None

from rosteriq.public_holidays import (
    get_holidays_for_year,
    is_public_holiday,
    get_penalty_multiplier,
    get_upcoming_holidays,
    get_store,
    PublicHoliday,
    HolidayCalendar,
)

logger = logging.getLogger("rosteriq.public_holidays_router")

# ─────────────────────────────────────────────────────────────────────────────
# Router Setup
# ─────────────────────────────────────────────────────────────────────────────

router = APIRouter(prefix="/api/v1/holidays", tags=["holidays"])


# ─────────────────────────────────────────────────────────────────────────────
# Helper: Auth gating
# ─────────────────────────────────────────────────────────────────────────────


def _gate(request: Request, level_name: str) -> None:
    """
    Gate access based on auth level (if auth is enabled).

    Args:
        request: FastAPI Request object
        level_name: Access level name ("L1" for read, "L2" for write, etc.)

    Raises:
        HTTPException(403) if access is denied
    """
    if require_access:
        try:
            require_access(request, level_name)
        except Exception as e:
            logger.warning(f"Access gate failed for level {level_name}: {e}")
            raise HTTPException(status_code=403, detail="Access denied")


# ─────────────────────────────────────────────────────────────────────────────
# Response Models
# ─────────────────────────────────────────────────────────────────────────────


@router.get("/{state}/{year}")
async def get_holidays(
    state: str,
    year: int,
    request: Request,
) -> Dict[str, Any]:
    """
    Get full holiday calendar for a state and year.

    Query Parameters:
        state (path): State abbreviation (QLD, NSW, VIC, etc.) or "ALL" for national only
        year (path): Year (e.g., 2026)

    Returns:
        {
            "year": int,
            "state": str,
            "holidays": [
                {
                    "holiday_id": str,
                    "name": str,
                    "date": "YYYY-MM-DD",
                    "state": str,
                    "holiday_type": "national" | "state" | "custom",
                    "is_gazetted": bool,
                    "substitute_date": "YYYY-MM-DD" | null,
                    "penalty_multiplier": float
                },
                ...
            ]
        }

    Access Level: L1 (read)

    Examples:
        GET /api/v1/holidays/QLD/2026
        GET /api/v1/holidays/NSW/2025
        GET /api/v1/holidays/ALL/2026
    """
    _gate(request, "L1")

    try:
        # Validate year is reasonable
        if year < 1900 or year > 2100:
            raise HTTPException(status_code=400, detail="Year must be between 1900 and 2100")

        state = state.upper()
        calendar = get_holidays_for_year(year, state)

        return calendar.to_dict()

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching holidays for {state}/{year}: {e}")
        raise HTTPException(status_code=500, detail="Failed to fetch holiday calendar")


@router.get("/check/{state}/{check_date}")
async def check_holiday(
    state: str,
    check_date: str,
    request: Request,
) -> Dict[str, Any]:
    """
    Check if a given date is a public holiday in a state.

    Query Parameters:
        state (path): State abbreviation (QLD, NSW, VIC, etc.)
        check_date (path): Date in YYYY-MM-DD format

    Returns:
        {
            "is_holiday": bool,
            "date": "YYYY-MM-DD",
            "state": str,
            "holiday": {
                "holiday_id": str,
                "name": str,
                "date": "YYYY-MM-DD",
                ...
            } | null
        }

    Access Level: L1 (read)

    Examples:
        GET /api/v1/holidays/check/QLD/2026-01-01
        GET /api/v1/holidays/check/NSW/2026-04-25
    """
    _gate(request, "L1")

    try:
        # Parse date
        check_dt = date.fromisoformat(check_date)
        state = state.upper()

        is_holiday, holiday = is_public_holiday(check_dt, state)

        return {
            "is_holiday": is_holiday,
            "date": check_date,
            "state": state,
            "holiday": holiday.to_dict() if holiday else None,
        }

    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"Invalid date format: {check_date}")
    except Exception as e:
        logger.error(f"Error checking holiday {state}/{check_date}: {e}")
        raise HTTPException(status_code=500, detail="Failed to check holiday")


@router.get("/{state}/upcoming")
async def get_upcoming(
    state: str,
    days_ahead: int = Query(90, ge=1, le=365),
    request: Request = None,
) -> Dict[str, Any]:
    """
    Get upcoming public holidays for a state.

    Query Parameters:
        state (path): State abbreviation (QLD, NSW, VIC, etc.)
        days_ahead (query): Number of days to look ahead (default 90, max 365)

    Returns:
        {
            "state": str,
            "days_ahead": int,
            "upcoming_holidays": [
                {
                    "holiday_id": str,
                    "name": str,
                    "date": "YYYY-MM-DD",
                    ...
                },
                ...
            ],
            "count": int
        }

    Access Level: L1 (read)

    Examples:
        GET /api/v1/holidays/QLD/upcoming
        GET /api/v1/holidays/NSW/upcoming?days_ahead=180
    """
    if request:
        _gate(request, "L1")

    try:
        state = state.upper()
        holidays = get_upcoming_holidays(state, days_ahead=days_ahead)

        return {
            "state": state,
            "days_ahead": days_ahead,
            "upcoming_holidays": [h.to_dict() for h in holidays],
            "count": len(holidays),
        }

    except Exception as e:
        logger.error(f"Error fetching upcoming holidays for {state}: {e}")
        raise HTTPException(status_code=500, detail="Failed to fetch upcoming holidays")


@router.get("/penalty/{state}/{penalty_date}")
async def get_penalty(
    state: str,
    penalty_date: str,
    employment_type: str = Query("casual", regex="^(casual|full_time|part_time)$"),
    request: Request = None,
) -> Dict[str, Any]:
    """
    Get penalty multiplier for a date.

    Query Parameters:
        state (path): State abbreviation (QLD, NSW, VIC, etc.)
        penalty_date (path): Date in YYYY-MM-DD format
        employment_type (query): "casual", "full_time", or "part_time" (default: casual)

    Returns:
        {
            "date": "YYYY-MM-DD",
            "state": str,
            "employment_type": str,
            "is_holiday": bool,
            "penalty_multiplier": float,
            "holiday_name": str | null
        }

    Access Level: L1 (read)

    Examples:
        GET /api/v1/holidays/penalty/QLD/2026-12-25
        GET /api/v1/holidays/penalty/NSW/2026-12-25?employment_type=full_time
    """
    if request:
        _gate(request, "L1")

    try:
        penalty_dt = date.fromisoformat(penalty_date)
        state = state.upper()

        is_holiday, holiday = is_public_holiday(penalty_dt, state)
        multiplier = get_penalty_multiplier(penalty_dt, state, employment_type)

        return {
            "date": penalty_date,
            "state": state,
            "employment_type": employment_type,
            "is_holiday": is_holiday,
            "penalty_multiplier": multiplier,
            "holiday_name": holiday.name if holiday else None,
        }

    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"Invalid date format: {penalty_date}")
    except Exception as e:
        logger.error(f"Error fetching penalty for {state}/{penalty_date}: {e}")
        raise HTTPException(status_code=500, detail="Failed to fetch penalty multiplier")


@router.post("/custom")
async def add_custom_holiday(
    body: Dict[str, Any],
    request: Request,
) -> Dict[str, Any]:
    """
    Add a custom venue-specific public holiday.

    Request Body:
        {
            "venue_id": str,  # Required: venue identifier
            "name": str,      # Required: holiday name
            "date": str,      # Required: date in YYYY-MM-DD format
            "penalty_multiplier": float  # Optional: default 2.5 (250%)
        }

    Returns:
        {
            "holiday_id": str,
            "venue_id": str,
            "name": str,
            "date": "YYYY-MM-DD",
            "penalty_multiplier": float,
            "created_at": "ISO datetime"
        }

    Access Level: L2 (write)

    Examples:
        POST /api/v1/holidays/custom
        {
            "venue_id": "brisbane_bar_123",
            "name": "Staff Appreciation Day",
            "date": "2026-06-15",
            "penalty_multiplier": 2.5
        }
    """
    _gate(request, "L2")

    try:
        # Validate required fields
        venue_id = body.get("venue_id", "").strip()
        name = body.get("name", "").strip()
        date_str = body.get("date", "").strip()
        penalty_multiplier = body.get("penalty_multiplier", 2.5)

        if not venue_id:
            raise HTTPException(status_code=400, detail="venue_id is required")
        if not name:
            raise HTTPException(status_code=400, detail="name is required")
        if not date_str:
            raise HTTPException(status_code=400, detail="date is required")

        holiday_date = date.fromisoformat(date_str)
        store = get_store()
        holiday = store.add_custom_holiday(venue_id, name, holiday_date, penalty_multiplier)

        # Return with created_at (we'll use the ISO format without milliseconds)
        from rosteriq.persistence import now_iso
        created_at = now_iso() if now_iso else date.today().isoformat()

        return {
            "holiday_id": holiday.holiday_id,
            "venue_id": venue_id,
            "name": name,
            "date": date_str,
            "penalty_multiplier": penalty_multiplier,
            "created_at": created_at,
        }

    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"Invalid date format: {str(e)}")
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error adding custom holiday: {e}")
        raise HTTPException(status_code=500, detail="Failed to add custom holiday")


@router.delete("/custom/{holiday_id}")
async def delete_custom_holiday(
    holiday_id: str,
    request: Request,
) -> Dict[str, Any]:
    """
    Delete a custom venue-specific public holiday.

    Path Parameters:
        holiday_id (path): The holiday ID

    Returns:
        {
            "success": bool,
            "holiday_id": str,
            "message": str
        }

    Access Level: L2 (write)

    Examples:
        DELETE /api/v1/holidays/custom/CUSTOM_brisbane_bar_123_2026-06-15_abc12345
    """
    _gate(request, "L2")

    try:
        store = get_store()
        success = store.delete_custom_holiday(holiday_id)

        if not success:
            raise HTTPException(status_code=404, detail=f"Holiday {holiday_id} not found")

        return {
            "success": True,
            "holiday_id": holiday_id,
            "message": "Custom holiday deleted successfully",
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error deleting custom holiday {holiday_id}: {e}")
        raise HTTPException(status_code=500, detail="Failed to delete custom holiday")
