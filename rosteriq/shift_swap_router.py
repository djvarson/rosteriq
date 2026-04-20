"""FastAPI router for shift swap endpoints.

Provides REST API for shift swap management:
- POST /api/v1/swaps/{venue_id}/offer - Staff offer up a shift
- POST /api/v1/swaps/{venue_id}/{swap_id}/claim - Staff claim a shift
- POST /api/v1/swaps/{venue_id}/{swap_id}/approve - Managers approve
- POST /api/v1/swaps/{venue_id}/{swap_id}/reject - Managers reject
- POST /api/v1/swaps/{venue_id}/{swap_id}/cancel - Offerer cancels
- GET /api/v1/swaps/{venue_id} - List swaps
- GET /api/v1/swaps/{venue_id}/available - Available swaps to claim
- GET /api/v1/swaps/{venue_id}/pending - Pending manager review
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field

from rosteriq.shift_swap import get_swap_store, SwapStatus

logger = logging.getLogger("rosteriq.shift_swap_router")

# Auth gating - fall back to no-op in demo/sandbox when auth stack unavailable
try:
    from rosteriq.auth import require_access, AccessLevel
except Exception:
    require_access = None
    AccessLevel = None


async def _gate(request: Request, level_name: str) -> None:
    """Apply role gating if auth stack is present; no-op in demo."""
    if require_access is None or AccessLevel is None:
        return
    level = getattr(AccessLevel, level_name)
    await require_access(level)(request=request)


# ---------------------------------------------------------------------------
# Pydantic Models
# ---------------------------------------------------------------------------


class OfferRequest(BaseModel):
    """Request to offer up a shift."""
    shift_id: str = Field(..., description="Shift ID")
    shift_date: str = Field(..., description="ISO date YYYY-MM-DD")
    shift_start: str = Field(..., description="HH:MM")
    shift_end: str = Field(..., description="HH:MM")
    role: str = Field(..., description="Role (bartender, floor, kitchen, etc)")
    reason: str = Field(..., description="Why they can't work it")
    offered_by: Optional[str] = Field(None, description="Employee ID if not from auth")
    offered_by_name: Optional[str] = Field(None, description="Employee name if not from auth")


class ClaimRequest(BaseModel):
    """Request to claim a shift swap."""
    claimed_by: Optional[str] = Field(None, description="Employee ID if not from auth")
    claimed_by_name: Optional[str] = Field(None, description="Employee name if not from auth")


class ApproveRequest(BaseModel):
    """Request to approve a shift swap."""
    note: Optional[str] = Field(None, description="Optional approval note")


class RejectRequest(BaseModel):
    """Request to reject a shift swap."""
    note: Optional[str] = Field(None, description="Optional rejection reason")


class CancelRequest(BaseModel):
    """Request to cancel a shift swap."""
    pass


class ShiftSwapResponse(BaseModel):
    """Response containing a shift swap."""
    swap_id: str
    venue_id: str
    shift_id: str
    shift_date: str
    shift_start: str
    shift_end: str
    role: str
    offered_by: str
    offered_by_name: str
    reason: str
    status: str
    claimed_by: Optional[str] = None
    claimed_by_name: Optional[str] = None
    claimed_at: Optional[str] = None
    reviewed_by: Optional[str] = None
    reviewed_at: Optional[str] = None
    review_note: Optional[str] = None
    created_at: str
    updated_at: str

    @classmethod
    def from_swap(cls, swap: Any) -> ShiftSwapResponse:
        """Convert a ShiftSwap dataclass to response."""
        return cls(**swap.to_dict())


class ListResponse(BaseModel):
    """Response containing a list of swaps."""
    count: int = Field(..., description="Number of swaps")
    swaps: List[ShiftSwapResponse]


# ---------------------------------------------------------------------------
# Router
# ---------------------------------------------------------------------------

router = APIRouter(prefix="/api/v1/swaps", tags=["shift-swaps"])


@router.post("/{venue_id}/offer", response_model=ShiftSwapResponse)
async def offer_shift(
    venue_id: str,
    req: OfferRequest,
    request: Request,
) -> ShiftSwapResponse:
    """
    Offer up a shift that the user can't work.

    Staff (L1+) can offer their own shifts. The shift enters OFFERED status,
    visible to other staff for claiming.

    Args:
        venue_id: Venue ID
        req: Offer details
        request: HTTP request for auth context

    Returns:
        Created shift swap in OFFERED status

    Raises:
        400: Invalid input
        403: Not authorized
    """
    await _gate(request, "L1_STAFF")

    # Use auth context if available, else fallback to request body
    offered_by = req.offered_by or getattr(request.state, "user_id", "unknown")
    offered_by_name = req.offered_by_name or getattr(request.state, "user_name", "Staff")

    try:
        store = get_swap_store()
        swap = store.offer(
            venue_id=venue_id,
            shift_id=req.shift_id,
            shift_date=req.shift_date,
            shift_start=req.shift_start,
            shift_end=req.shift_end,
            role=req.role,
            offered_by=offered_by,
            offered_by_name=offered_by_name,
            reason=req.reason,
        )
        return ShiftSwapResponse.from_swap(swap)
    except Exception as e:
        logger.exception("Failed to offer shift")
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/{venue_id}/{swap_id}/claim", response_model=ShiftSwapResponse)
async def claim_shift(
    venue_id: str,
    swap_id: str,
    req: ClaimRequest,
    request: Request,
) -> ShiftSwapResponse:
    """
    Claim a shift swap that's been offered.

    Staff (L1+) can claim an offered shift. The swap enters CLAIMED status,
    pending manager approval.

    Args:
        venue_id: Venue ID
        swap_id: Swap ID to claim
        req: Claim details
        request: HTTP request for auth context

    Returns:
        Updated swap in CLAIMED status

    Raises:
        400: Invalid input or swap not in OFFERED status
        403: Not authorized
        404: Swap not found
    """
    await _gate(request, "L1_STAFF")

    claimed_by = req.claimed_by or getattr(request.state, "user_id", "unknown")
    claimed_by_name = req.claimed_by_name or getattr(request.state, "user_name", "Staff")

    try:
        store = get_swap_store()
        swap = store.get(swap_id)
        if not swap:
            raise HTTPException(status_code=404, detail=f"Swap {swap_id} not found")
        if swap.venue_id != venue_id:
            raise HTTPException(status_code=404, detail=f"Swap not in venue {venue_id}")

        updated = store.claim(swap_id, claimed_by, claimed_by_name)
        return ShiftSwapResponse.from_swap(updated)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.exception("Failed to claim shift")
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/{venue_id}/{swap_id}/approve", response_model=ShiftSwapResponse)
async def approve_swap(
    venue_id: str,
    swap_id: str,
    req: ApproveRequest,
    request: Request,
) -> ShiftSwapResponse:
    """
    Approve a claimed shift swap.

    Managers (L2+) approve claimed swaps. The swap enters APPROVED status.

    Args:
        venue_id: Venue ID
        swap_id: Swap ID to approve
        req: Approval details
        request: HTTP request for auth context

    Returns:
        Updated swap in APPROVED status

    Raises:
        400: Invalid input or swap not in CLAIMED status
        403: Not authorized
        404: Swap not found
    """
    await _gate(request, "L2_ROSTER_MAKER")

    reviewed_by = getattr(request.state, "user_id", "unknown")

    try:
        store = get_swap_store()
        swap = store.get(swap_id)
        if not swap:
            raise HTTPException(status_code=404, detail=f"Swap {swap_id} not found")
        if swap.venue_id != venue_id:
            raise HTTPException(status_code=404, detail=f"Swap not in venue {venue_id}")

        updated = store.approve(swap_id, reviewed_by, note=req.note)
        return ShiftSwapResponse.from_swap(updated)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.exception("Failed to approve swap")
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/{venue_id}/{swap_id}/reject", response_model=ShiftSwapResponse)
async def reject_swap(
    venue_id: str,
    swap_id: str,
    req: RejectRequest,
    request: Request,
) -> ShiftSwapResponse:
    """
    Reject a claimed shift swap.

    Managers (L2+) reject claimed swaps. The swap enters REJECTED status.

    Args:
        venue_id: Venue ID
        swap_id: Swap ID to reject
        req: Rejection details
        request: HTTP request for auth context

    Returns:
        Updated swap in REJECTED status

    Raises:
        400: Invalid input or swap not in CLAIMED status
        403: Not authorized
        404: Swap not found
    """
    await _gate(request, "L2_ROSTER_MAKER")

    reviewed_by = getattr(request.state, "user_id", "unknown")

    try:
        store = get_swap_store()
        swap = store.get(swap_id)
        if not swap:
            raise HTTPException(status_code=404, detail=f"Swap {swap_id} not found")
        if swap.venue_id != venue_id:
            raise HTTPException(status_code=404, detail=f"Swap not in venue {venue_id}")

        updated = store.reject(swap_id, reviewed_by, note=req.note)
        return ShiftSwapResponse.from_swap(updated)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.exception("Failed to reject swap")
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/{venue_id}/{swap_id}/cancel", response_model=ShiftSwapResponse)
async def cancel_swap(
    venue_id: str,
    swap_id: str,
    req: CancelRequest,
    request: Request,
) -> ShiftSwapResponse:
    """
    Cancel an offered or claimed shift swap.

    Only the offerer can cancel. Works if swap is in OFFERED or CLAIMED status.

    Args:
        venue_id: Venue ID
        swap_id: Swap ID to cancel
        req: Cancel details (empty)
        request: HTTP request for auth context

    Returns:
        Updated swap in CANCELLED status

    Raises:
        400: Invalid input or swap not in cancellable status
        403: Not authorized / not the offerer
        404: Swap not found
    """
    await _gate(request, "L1_STAFF")

    cancelled_by = getattr(request.state, "user_id", "unknown")

    try:
        store = get_swap_store()
        swap = store.get(swap_id)
        if not swap:
            raise HTTPException(status_code=404, detail=f"Swap {swap_id} not found")
        if swap.venue_id != venue_id:
            raise HTTPException(status_code=404, detail=f"Swap not in venue {venue_id}")

        updated = store.cancel(swap_id, cancelled_by)
        return ShiftSwapResponse.from_swap(updated)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.exception("Failed to cancel swap")
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/{venue_id}", response_model=ListResponse)
async def list_swaps(
    venue_id: str,
    status: Optional[str] = None,
    limit: int = 50,
    request: Request = None,
) -> ListResponse:
    """
    List shift swaps for a venue.

    Staff (L1+) see all swaps. Optionally filter by status.

    Args:
        venue_id: Venue ID
        status: Optional status filter (offered, claimed, approved, etc)
        limit: Max results (default 50)
        request: HTTP request for auth context

    Returns:
        List of shift swaps, newest first
    """
    await _gate(request, "L1_STAFF")

    try:
        store = get_swap_store()
        status_filter = None
        if status:
            try:
                status_filter = SwapStatus(status)
            except ValueError:
                raise HTTPException(
                    status_code=400,
                    detail=f"Invalid status {status}. Must be one of: "
                    f"{', '.join(s.value for s in SwapStatus)}",
                )
        swaps = store.list_for_venue(venue_id, status=status_filter, limit=limit)
        return ListResponse(
            count=len(swaps),
            swaps=[ShiftSwapResponse.from_swap(s) for s in swaps],
        )
    except Exception as e:
        logger.exception("Failed to list swaps")
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/{venue_id}/available", response_model=ListResponse)
async def list_available(
    venue_id: str,
    request: Request = None,
) -> ListResponse:
    """
    List available swaps that can be claimed.

    Returns swaps in OFFERED status only, newest first. Up to 50 results.

    Args:
        venue_id: Venue ID
        request: HTTP request for auth context

    Returns:
        List of available shift swaps
    """
    await _gate(request, "L1_STAFF")

    try:
        store = get_swap_store()
        swaps = store.list_available(venue_id)
        return ListResponse(
            count=len(swaps),
            swaps=[ShiftSwapResponse.from_swap(s) for s in swaps],
        )
    except Exception as e:
        logger.exception("Failed to list available swaps")
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/{venue_id}/pending", response_model=ListResponse)
async def list_pending_review(
    venue_id: str,
    request: Request = None,
) -> ListResponse:
    """
    List swaps pending manager review.

    Returns swaps in CLAIMED status only, newest first. Up to 50 results.
    Managers (L2+) use this to find swaps needing approval/rejection.

    Args:
        venue_id: Venue ID
        request: HTTP request for auth context

    Returns:
        List of swaps pending manager approval
    """
    await _gate(request, "L2_ROSTER_MAKER")

    try:
        store = get_swap_store()
        swaps = store.list_pending_review(venue_id)
        return ListResponse(
            count=len(swaps),
            swaps=[ShiftSwapResponse.from_swap(s) for s in swaps],
        )
    except Exception as e:
        logger.exception("Failed to list pending swaps")
        raise HTTPException(status_code=400, detail=str(e))
