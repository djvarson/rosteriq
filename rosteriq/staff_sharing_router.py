"""REST endpoints for multi-venue staff sharing — mounted at /api/v1/sharing."""
from __future__ import annotations

try:
    from fastapi import APIRouter, HTTPException, Request
    from pydantic import BaseModel, Field
    from typing import List, Optional
    router = APIRouter()
except ImportError:
    router = None

from datetime import date, datetime


def _gate(request, level):
    """Auth gating (placeholder for access control)."""
    pass


if router:
    @router.post("/request")
    async def request_transfer(request: Request, body: dict):
        """Create a new transfer request."""
        _gate(request, "L2_ROSTER_MAKER")
        from rosteriq.staff_sharing import request_transfer
        req = request_transfer(
            employee_id=body["employee_id"],
            employee_name=body["employee_name"],
            from_venue_id=body["from_venue_id"],
            from_venue_name=body["from_venue_name"],
            to_venue_id=body["to_venue_id"],
            to_venue_name=body["to_venue_name"],
            requested_by=body.get("requested_by", "unknown"),
            start_date=date.fromisoformat(body["start_date"]),
            end_date=date.fromisoformat(body["end_date"]),
            reason=body.get("reason", ""),
            notes=body.get("notes", ""),
        )
        return {"status": "ok", "request": req.to_dict()}

    @router.post("/approve/{request_id}")
    async def approve_endpoint(request: Request, request_id: str, body: dict):
        """Approve a transfer request."""
        _gate(request, "L3_PORTFOLIO_MANAGER")
        from rosteriq.staff_sharing import approve_transfer
        req = approve_transfer(request_id, body.get("approved_by", "unknown"))
        return {"status": "ok", "request": req.to_dict()}

    @router.post("/reject/{request_id}")
    async def reject_endpoint(request: Request, request_id: str):
        """Reject a transfer request."""
        _gate(request, "L3_PORTFOLIO_MANAGER")
        from rosteriq.staff_sharing import reject_transfer
        req = reject_transfer(request_id)
        return {"status": "ok", "request": req.to_dict()}

    @router.post("/cancel/{request_id}")
    async def cancel_endpoint(request: Request, request_id: str):
        """Cancel a transfer request."""
        _gate(request, "L2_ROSTER_MAKER")
        from rosteriq.staff_sharing import cancel_transfer
        req = cancel_transfer(request_id)
        return {"status": "ok", "request": req.to_dict()}

    @router.post("/activate/{request_id}")
    async def activate_endpoint(request: Request, request_id: str):
        """Activate an approved transfer."""
        _gate(request, "L2_ROSTER_MAKER")
        from rosteriq.staff_sharing import activate_transfer
        req = activate_transfer(request_id)
        return {"status": "ok", "request": req.to_dict()}

    @router.post("/complete/{request_id}")
    async def complete_endpoint(request: Request, request_id: str):
        """Complete an active transfer."""
        _gate(request, "L2_ROSTER_MAKER")
        from rosteriq.staff_sharing import complete_transfer
        req = complete_transfer(request_id)
        return {"status": "ok", "request": req.to_dict()}

    @router.get("/requests")
    async def list_requests(request: Request,
                           venue_id: Optional[str] = None,
                           status: Optional[str] = None,
                           employee_id: Optional[str] = None):
        """List transfer requests with optional filters."""
        _gate(request, "L1_SUPERVISOR")
        from rosteriq.staff_sharing import list_transfer_requests, TransferStatus
        status_enum = TransferStatus(status) if status else None
        reqs = list_transfer_requests(venue_id, status_enum, employee_id)
        return {"requests": [r.to_dict() for r in reqs]}

    @router.get("/available/{venue_id}")
    async def get_available(request: Request, venue_id: str):
        """Get staff available for sharing from a venue."""
        _gate(request, "L1_SUPERVISOR")
        from rosteriq.staff_sharing import get_available_for_sharing
        avail = get_available_for_sharing(venue_id)
        return {"available_staff": [a.to_dict() for a in avail]}

    @router.get("/borrowable/{to_venue_id}")
    async def get_borrowable(request: Request, to_venue_id: str,
                            role: Optional[str] = None):
        """Get staff available to borrow into a venue."""
        _gate(request, "L1_SUPERVISOR")
        from rosteriq.staff_sharing import get_borrowable_staff
        staff = get_borrowable_staff(to_venue_id, role)
        return {"borrowable_staff": staff}

    @router.get("/stats/{venue_id}")
    async def get_stats(request: Request, venue_id: str,
                       period_start: str, period_end: str):
        """Get sharing statistics for a venue in a period."""
        _gate(request, "L1_SUPERVISOR")
        from rosteriq.staff_sharing import get_sharing_stats
        stats = get_sharing_stats(venue_id, date.fromisoformat(period_start),
                                 date.fromisoformat(period_end))
        return stats.to_dict()

    @router.put("/availability")
    async def set_availability(request: Request, body: dict):
        """Set staff member's cross-venue availability."""
        _gate(request, "L2_ROSTER_MAKER")
        from rosteriq.staff_sharing import set_cross_venue_availability
        avail = set_cross_venue_availability(
            employee_id=body["employee_id"],
            employee_name=body["employee_name"],
            home_venue_id=body["home_venue_id"],
            available_venues=body.get("available_venues", []),
            roles=body.get("roles", []),
            certs=body.get("certs", []),
            max_hours=body.get("max_hours_cross_venue", 10.0),
        )
        return {"status": "ok", "availability": avail.to_dict()}
