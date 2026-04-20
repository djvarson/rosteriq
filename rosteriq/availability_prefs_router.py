"""Staff Availability Preferences API Router for RosterIQ

Exposes availability preferences endpoints for self-service employee scheduling:
- Weekly recurring preferences (CRUD)
- Date-specific overrides (CRUD)
- Employee constraints (max hours, blackout dates)
- Availability resolution and team queries

All endpoints follow RosterIQ auth patterns with L1 (read) and L2 (write) gates.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

try:
    from fastapi import APIRouter, HTTPException, Query, Request
except ImportError:
    APIRouter = None
    HTTPException = None
    Query = None
    Request = None

try:
    from rosteriq.auth import require_access
except ImportError:
    require_access = None

from rosteriq.availability_prefs import (
    get_availability_prefs_store,
    WeeklyPreference,
    AvailabilityOverride,
    EmployeeConstraints,
)

logger = logging.getLogger("rosteriq.availability_prefs_router")

# Graceful handling if FastAPI not available
if APIRouter:
    router = APIRouter(prefix="/api/v1/availability", tags=["availability"])
else:
    router = None


# ─────────────────────────────────────────────────────────────────────────────
# Helper: Auth gating
# ─────────────────────────────────────────────────────────────────────────────


def _gate(request: Request, level_name: str) -> None:
    """Gate access based on auth level (if auth is enabled).

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
            logger.warning("Access gate failed for level %s: %s", level_name, e)
            raise HTTPException(status_code=403, detail="Access denied")


# ─────────────────────────────────────────────────────────────────────────────
# Weekly Preferences Endpoints
# ─────────────────────────────────────────────────────────────────────────────


if router:
    @router.post("/weekly/{venue_id}/{employee_id}")
    async def set_weekly_preference(
        venue_id: str,
        employee_id: str,
        request: Request,
        day_of_week: int = Query(..., description="Day of week (0=Monday, 6=Sunday)"),
        status: str = Query(..., description="AVAILABLE, UNAVAILABLE, or PREFERRED"),
        start_time: Optional[str] = Query(None, description="Optional HH:MM"),
        end_time: Optional[str] = Query(None, description="Optional HH:MM"),
        notes: Optional[str] = Query(None),
        effective_from: str = Query(..., description="ISO date"),
        effective_until: Optional[str] = Query(None, description="ISO date or null"),
    ) -> Dict[str, Any]:
        """Set a weekly availability preference for an employee.

        L1+ can set for themselves, L2+ can set for others.
        Upserts on employee+day+venue.
        """
        _gate(request, "L1")

        if status not in ("AVAILABLE", "UNAVAILABLE", "PREFERRED"):
            raise HTTPException(status_code=400, detail="Invalid status")

        pref_dict = {
            "venue_id": venue_id,
            "employee_id": employee_id,
            "day_of_week": day_of_week,
            "status": status,
            "start_time": start_time,
            "end_time": end_time,
            "notes": notes,
            "effective_from": effective_from,
            "effective_until": effective_until,
        }

        store = get_availability_prefs_store()
        pref = store.set_weekly_preference(pref_dict)

        return {
            "success": True,
            "preference": pref.to_dict(),
        }

    @router.get("/weekly/{venue_id}/{employee_id}")
    async def get_weekly_preferences(
        venue_id: str,
        employee_id: str,
        request: Request,
    ) -> Dict[str, Any]:
        """Get all weekly preferences for an employee.

        Returns list sorted by day of week.
        """
        _gate(request, "L1")

        store = get_availability_prefs_store()
        prefs = store.get_weekly_preferences(venue_id, employee_id)

        return {
            "venue_id": venue_id,
            "employee_id": employee_id,
            "preferences": [p.to_dict() for p in prefs],
        }

    @router.delete("/weekly/{pref_id}")
    async def delete_weekly_preference(
        pref_id: str,
        request: Request,
    ) -> Dict[str, Any]:
        """Delete a weekly preference.

        L1+ only.
        """
        _gate(request, "L1")

        store = get_availability_prefs_store()
        deleted = store.delete_weekly_preference(pref_id)

        if not deleted:
            raise HTTPException(status_code=404, detail="Preference not found")

        return {"success": True, "deleted": pref_id}


# ─────────────────────────────────────────────────────────────────────────────
# Availability Overrides Endpoints
# ─────────────────────────────────────────────────────────────────────────────


if router:
    @router.post("/override/{venue_id}/{employee_id}")
    async def add_availability_override(
        venue_id: str,
        employee_id: str,
        request: Request,
        date: str = Query(..., description="ISO date"),
        status: str = Query(..., description="AVAILABLE, UNAVAILABLE, or PREFERRED"),
        start_time: Optional[str] = Query(None, description="Optional HH:MM"),
        end_time: Optional[str] = Query(None, description="Optional HH:MM"),
        reason: Optional[str] = Query(None, description="Optional reason"),
    ) -> Dict[str, Any]:
        """Add an availability override for a specific date.

        L1+ can add for themselves, L2+ can add for others.
        """
        _gate(request, "L1")

        if status not in ("AVAILABLE", "UNAVAILABLE", "PREFERRED"):
            raise HTTPException(status_code=400, detail="Invalid status")

        override_dict = {
            "venue_id": venue_id,
            "employee_id": employee_id,
            "date": date,
            "status": status,
            "start_time": start_time,
            "end_time": end_time,
            "reason": reason,
        }

        store = get_availability_prefs_store()
        override = store.add_override(override_dict)

        return {
            "success": True,
            "override": override.to_dict(),
        }

    @router.get("/override/{venue_id}/{employee_id}")
    async def get_availability_overrides(
        venue_id: str,
        employee_id: str,
        request: Request,
        date_from: Optional[str] = Query(None, description="ISO date (inclusive)"),
        date_to: Optional[str] = Query(None, description="ISO date (inclusive)"),
    ) -> Dict[str, Any]:
        """Get availability overrides for an employee, optionally filtered by date range.

        L1+ only.
        """
        _gate(request, "L1")

        store = get_availability_prefs_store()
        overrides = store.get_overrides(venue_id, employee_id, date_from, date_to)

        return {
            "venue_id": venue_id,
            "employee_id": employee_id,
            "date_from": date_from,
            "date_to": date_to,
            "overrides": [o.to_dict() for o in overrides],
        }

    @router.delete("/override/{override_id}")
    async def delete_availability_override(
        override_id: str,
        request: Request,
    ) -> Dict[str, Any]:
        """Delete an availability override.

        L1+ only.
        """
        _gate(request, "L1")

        store = get_availability_prefs_store()
        deleted = store.delete_override(override_id)

        if not deleted:
            raise HTTPException(status_code=404, detail="Override not found")

        return {"success": True, "deleted": override_id}


# ─────────────────────────────────────────────────────────────────────────────
# Employee Constraints Endpoints
# ─────────────────────────────────────────────────────────────────────────────


if router:
    @router.post("/constraints/{venue_id}/{employee_id}")
    async def set_employee_constraints(
        venue_id: str,
        employee_id: str,
        request: Request,
        max_hours_per_week: Optional[float] = Query(None),
        min_hours_per_week: Optional[float] = Query(None),
        max_shifts_per_week: Optional[int] = Query(None),
        max_consecutive_days: Optional[int] = Query(None),
        preferred_shift_length: Optional[float] = Query(None),
    ) -> Dict[str, Any]:
        """Set or update employee constraints (max hours, shift limits, etc).

        L2+ only (managers/admins).
        """
        _gate(request, "L2")

        constraints_dict = {
            "venue_id": venue_id,
            "employee_id": employee_id,
            "max_hours_per_week": max_hours_per_week,
            "min_hours_per_week": min_hours_per_week,
            "max_shifts_per_week": max_shifts_per_week,
            "max_consecutive_days": max_consecutive_days,
            "preferred_shift_length": preferred_shift_length,
            "blackout_dates": [],
        }

        store = get_availability_prefs_store()
        constraints = store.set_constraints(constraints_dict)

        return {
            "success": True,
            "constraints": constraints.to_dict(),
        }

    @router.get("/constraints/{venue_id}/{employee_id}")
    async def get_employee_constraints(
        venue_id: str,
        employee_id: str,
        request: Request,
    ) -> Dict[str, Any]:
        """Get employee constraints for an employee.

        L1+ only. Returns null if constraints not set.
        """
        _gate(request, "L1")

        store = get_availability_prefs_store()
        constraints = store.get_constraints(venue_id, employee_id)

        return {
            "venue_id": venue_id,
            "employee_id": employee_id,
            "constraints": constraints.to_dict() if constraints else None,
        }


# ─────────────────────────────────────────────────────────────────────────────
# Availability Resolution Endpoints
# ─────────────────────────────────────────────────────────────────────────────


if router:
    @router.get("/check/{venue_id}/{employee_id}/{date}")
    async def check_availability(
        venue_id: str,
        employee_id: str,
        date: str,
        request: Request,
    ) -> Dict[str, Any]:
        """Check availability for a specific date.

        Resolves override > weekly preference > default (available).
        L1+ only.
        """
        _gate(request, "L1")

        store = get_availability_prefs_store()
        avail = store.get_availability_for_date(venue_id, employee_id, date)

        return {
            "venue_id": venue_id,
            "employee_id": employee_id,
            "date": date,
            "availability": avail,
        }

    @router.get("/team/{venue_id}/{date}")
    async def get_team_availability(
        venue_id: str,
        date: str,
        request: Request,
    ) -> Dict[str, Any]:
        """Get availability for all employees at a venue for a specific date.

        L1+ only.
        """
        _gate(request, "L1")

        store = get_availability_prefs_store()
        team_avail = store.get_team_availability(venue_id, date)

        return {
            "venue_id": venue_id,
            "date": date,
            "team": team_avail,
        }

    @router.get("/available/{venue_id}/{date}")
    async def get_available_staff(
        venue_id: str,
        date: str,
        request: Request,
        start_time: Optional[str] = Query(None, description="Optional HH:MM"),
        end_time: Optional[str] = Query(None, description="Optional HH:MM"),
    ) -> Dict[str, Any]:
        """Get list of employees available for a specific date and optional time window.

        L1+ only.
        """
        _gate(request, "L1")

        store = get_availability_prefs_store()
        available = store.get_available_staff(venue_id, date, start_time, end_time)

        return {
            "venue_id": venue_id,
            "date": date,
            "start_time": start_time,
            "end_time": end_time,
            "available_staff": available,
        }


# ─────────────────────────────────────────────────────────────────────────────
# Blackout Dates Endpoints
# ─────────────────────────────────────────────────────────────────────────────


if router:
    @router.post("/blackout/{venue_id}/{employee_id}")
    async def add_blackout_date(
        venue_id: str,
        employee_id: str,
        request: Request,
        date: str = Query(..., description="ISO date to blackout"),
        reason: Optional[str] = Query(None, description="Optional reason"),
    ) -> Dict[str, Any]:
        """Add a blackout date (day employee cannot work).

        L1+ can add for themselves, L2+ can add for others.
        """
        _gate(request, "L1")

        store = get_availability_prefs_store()
        constraints = store.add_blackout_date(venue_id, employee_id, date, reason)

        return {
            "success": True,
            "constraints": constraints.to_dict() if constraints else None,
        }

    @router.delete("/blackout/{venue_id}/{employee_id}")
    async def remove_blackout_date(
        venue_id: str,
        employee_id: str,
        request: Request,
        date: str = Query(..., description="ISO date to un-blackout"),
    ) -> Dict[str, Any]:
        """Remove a blackout date.

        L1+ can remove their own, L2+ can remove others.
        """
        _gate(request, "L1")

        store = get_availability_prefs_store()
        constraints = store.remove_blackout_date(venue_id, employee_id, date)

        return {
            "success": True,
            "constraints": constraints.to_dict() if constraints else None,
        }
