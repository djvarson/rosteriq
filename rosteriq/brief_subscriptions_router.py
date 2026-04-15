"""API router for brief subscription management.

Endpoints:
- POST /subscriptions: create subscription
- GET /subscriptions: list for venue
- PATCH /subscriptions/{id}: update
- DELETE /subscriptions/{id}: delete
- POST /trigger/morning: manually fire morning brief
- POST /trigger/weekly: manually fire weekly digest
- POST /trigger/portfolio: manually fire portfolio recap

Requires L2 (Tier 2) access; owner-only trigger endpoints.
"""
from __future__ import annotations

import logging
from datetime import date
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query, Request
from pydantic import BaseModel, Field

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


logger = logging.getLogger("rosteriq.brief_subscriptions_router")


# ---------------------------------------------------------------------------
# Request/Response Models
# ---------------------------------------------------------------------------

class BriefSubscriptionCreate(BaseModel):
    """Create a brief subscription."""
    venue_id: str = Field(..., description="Venue ID")
    user_id: str = Field(..., description="User ID")
    user_role: str = Field(..., description="'owner' | 'manager' | 'supervisor'")
    email: Optional[str] = Field(None, description="Email address")
    phone: Optional[str] = Field(None, description="Phone number")
    brief_types: List[str] = Field(
        default=["morning", "weekly"],
        description="'morning' | 'weekly' | 'portfolio'",
    )
    delivery_channels: List[str] = Field(
        default=["email"],
        description="'email' | 'sms'",
    )
    local_tz: str = Field(
        default="Australia/Perth",
        description="Timezone for scheduling",
    )


class BriefSubscriptionUpdate(BaseModel):
    """Update a brief subscription."""
    enabled: Optional[bool] = None
    delivery_channels: Optional[List[str]] = None
    brief_types: Optional[List[str]] = None
    local_tz: Optional[str] = None


class BriefSubscriptionResponse(BaseModel):
    """Brief subscription response."""
    subscription_id: str
    venue_id: str
    user_id: str
    user_role: str
    email: Optional[str]
    phone: Optional[str]
    brief_types: List[str]
    delivery_channels: List[str]
    local_tz: str
    enabled: bool


class BriefTriggerRequest(BaseModel):
    """Manual trigger request."""
    venue_id: str = Field(..., description="Venue ID")
    target_date: Optional[str] = Field(None, description="YYYY-MM-DD; defaults to today")


class BriefTriggerResponse(BaseModel):
    """Manual trigger response."""
    venue_id: str
    status: str  # "ok" | "error"
    detail: str
    delivered: int
    failed: int


# ---------------------------------------------------------------------------
# Router
# ---------------------------------------------------------------------------

router = APIRouter(prefix="/api/v1/briefs", tags=["briefs"])


@router.post("/subscriptions", response_model=BriefSubscriptionResponse)
async def create_subscription(req: BriefSubscriptionCreate, request: Request) -> Dict[str, Any]:
    """Create a new brief subscription.

    Requires L2 or OWNER access.
    """
    await _gate(request, "L2_ROSTER_MAKER")
    from rosteriq import brief_subscriptions

    # Validate at least one channel
    if not req.delivery_channels:
        raise HTTPException(
            status_code=400,
            detail="At least one delivery channel is required",
        )

    # Validate at least email or phone
    if not req.email and not req.phone:
        raise HTTPException(
            status_code=400,
            detail="Email or phone is required",
        )

    store = brief_subscriptions.get_subscription_store()
    sub = store.create(
        venue_id=req.venue_id,
        user_id=req.user_id,
        user_role=req.user_role,
        email=req.email,
        phone=req.phone,
        brief_types=req.brief_types,
        delivery_channels=req.delivery_channels,
        local_tz=req.local_tz,
    )
    return sub.to_dict()


@router.get("/subscriptions", response_model=List[BriefSubscriptionResponse])
async def list_subscriptions(request: Request, venue_id: str = Query(...)) -> List[Dict[str, Any]]:
    """List all subscriptions for a venue.

    Requires L2+ access.
    """
    await _gate(request, "L2_ROSTER_MAKER")
    from rosteriq import brief_subscriptions

    store = brief_subscriptions.get_subscription_store()
    subs = store.list_for_venue(venue_id)
    return [s.to_dict() for s in subs]


@router.patch("/subscriptions/{subscription_id}", response_model=BriefSubscriptionResponse)
async def update_subscription(
    subscription_id: str,
    req: BriefSubscriptionUpdate,
    request: Request,
) -> Dict[str, Any]:
    """Update a subscription (enabled, channels, brief_types, tz).

    Requires L2+ access.
    """
    await _gate(request, "L2_ROSTER_MAKER")
    from rosteriq import brief_subscriptions

    store = brief_subscriptions.get_subscription_store()
    sub = store.update(subscription_id, **req.model_dump(exclude_unset=True))
    if not sub:
        raise HTTPException(status_code=404, detail="Subscription not found")
    return sub.to_dict()


@router.delete("/subscriptions/{subscription_id}")
async def delete_subscription(subscription_id: str, request: Request) -> Dict[str, Any]:
    """Delete a subscription.

    Requires L2+ access.
    """
    await _gate(request, "L2_ROSTER_MAKER")
    from rosteriq import brief_subscriptions

    store = brief_subscriptions.get_subscription_store()
    deleted = store.delete(subscription_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="Subscription not found")
    return {"status": "ok", "subscription_id": subscription_id}


@router.post("/trigger/morning", response_model=BriefTriggerResponse)
async def trigger_morning_brief(req: BriefTriggerRequest, request: Request) -> Dict[str, Any]:
    """Manually fire a morning brief for a venue.

    Requires OWNER access.
    """
    await _gate(request, "OWNER")
    from rosteriq import brief_dispatcher

    try:
        result = await brief_dispatcher.dispatch_morning_brief_with_delivery(
            req.venue_id,
            target_date=req.target_date,
        )
        return {
            "venue_id": req.venue_id,
            "status": "ok",
            "detail": "Morning brief dispatched",
            "delivered": len(result.get("delivered", [])),
            "failed": len(result.get("failed", [])),
        }
    except Exception as exc:
        logger.exception("trigger_morning_brief failed")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to dispatch morning brief: {exc}",
        )


@router.post("/trigger/weekly", response_model=BriefTriggerResponse)
async def trigger_weekly_digest(req: BriefTriggerRequest, request: Request) -> Dict[str, Any]:
    """Manually fire a weekly digest for a venue.

    Requires OWNER access.
    """
    await _gate(request, "OWNER")
    from rosteriq import brief_dispatcher

    try:
        result = await brief_dispatcher.dispatch_weekly_digest_with_delivery(
            req.venue_id,
            week_ending=req.target_date,
        )
        return {
            "venue_id": req.venue_id,
            "status": "ok",
            "detail": "Weekly digest dispatched",
            "delivered": len(result.get("delivered", [])),
            "failed": len(result.get("failed", [])),
        }
    except Exception as exc:
        logger.exception("trigger_weekly_digest failed")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to dispatch weekly digest: {exc}",
        )


@router.post("/trigger/portfolio", response_model=BriefTriggerResponse)
async def trigger_portfolio_recap(req: BriefTriggerRequest, request: Request) -> Dict[str, Any]:
    """Manually fire a portfolio recap.

    Requires OWNER access. Dispatches to all owner subscriptions.
    """
    await _gate(request, "OWNER")
    from rosteriq import brief_dispatcher

    try:
        result = await brief_dispatcher.dispatch_portfolio_recap_with_delivery(
            [req.venue_id],
            target_date=req.target_date,
        )
        return {
            "venue_id": req.venue_id,
            "status": "ok",
            "detail": "Portfolio recap dispatched",
            "delivered": len(result.get("delivered", [])),
            "failed": len(result.get("failed", [])),
        }
    except Exception as exc:
        logger.exception("trigger_portfolio_recap failed")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to dispatch portfolio recap: {exc}",
        )
