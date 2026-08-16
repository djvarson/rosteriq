"""
Approval workflow routes for RosterIQ.

REST API endpoints for managing roster approvals with rules engine,
tier-based workflows, and escalation.

Endpoints:
- POST /api/approvals/submit/{roster_id} — submit for approval
- GET /api/approvals/pending — my pending reviews
- POST /api/approvals/{id}/approve — approve
- POST /api/approvals/{id}/reject — reject with notes
- POST /api/approvals/{id}/escalate — manual escalate
- GET /api/approvals/history/{roster_id} — approval history
- GET /api/approvals/{id}/diff — diff between revisions
- GET /api/approvals/rules — list active rules
"""

import logging
from datetime import datetime
from typing import List, Optional
from decimal import Decimal

from fastapi import APIRouter, HTTPException, Depends, Query
from pydantic import BaseModel

from rosteriq.middleware.auth import get_current_user, UserContext
from rosteriq.middleware.tenant import enforce_venue_access
from rosteriq.database import get_db
from rosteriq.services.approval_workflow import (
    approval_workflow,
    ApprovalRequest,
    ApprovalStatus,
)
from rosteriq.services.events import audit

logger = logging.getLogger(__name__)
router = APIRouter(tags=["approvals"])


# ============================================================================
# Tenant helpers
# ============================================================================


def _user_can_access_venue(user: UserContext, venue_id: Optional[str]) -> bool:
    """Owner passes everything; manager/staff only their own venue_ids."""
    if user.is_owner:
        return True
    return bool(venue_id) and venue_id in (user.venue_ids or [])


def _enforce_venue_or_404(venue_id: Optional[str], not_found_detail: str) -> None:
    """
    Tenant gate for by-id routes. Denial is reported as 404 (same as a missing
    record) so approval/roster ids are not an oracle for other tenants' data.
    """
    try:
        enforce_venue_access(venue_id)
    except HTTPException as exc:
        if exc.status_code == 403:
            raise HTTPException(status_code=404, detail=not_found_detail)
        raise


def _load_approval_request_scoped(request_id: str) -> dict:
    """Load an approval request by id, 404 if missing or outside the caller's venues."""
    request_dict = get_db().get_approval_request(request_id)
    not_found = f"Request {request_id} not found"
    if not request_dict:
        raise HTTPException(status_code=404, detail=not_found)
    _enforce_venue_or_404(request_dict.get("venue_id"), not_found)
    return request_dict


# ============================================================================
# Request/Response Models
# ============================================================================


class SubmitApprovalRequest(BaseModel):
    """Request to submit a roster for approval."""
    pass


class ApprovalResponse(BaseModel):
    """Response with approval request details."""
    id: str
    roster_id: str
    venue_id: str
    submitted_by: str
    submitted_at: datetime
    status: str
    reviewed_by: Optional[str] = None
    reviewed_at: Optional[datetime] = None
    review_notes: Optional[str] = None
    revision_number: int
    escalated_at: Optional[datetime] = None
    escalated_to: Optional[str] = None
    tier: str
    auto_approved_by_rules: List[str]
    failed_rules: List[str]


class ReviewApprovalRequest(BaseModel):
    """Request to review (approve/reject) a roster."""
    approved: bool
    notes: Optional[str] = None


class RosterDiffResponse(BaseModel):
    """Response with roster revision diff."""
    roster_id: str
    revision_a: int
    revision_b: int
    added_shifts: List[dict]
    removed_shifts: List[dict]
    changed_shifts: List[dict]
    cost_delta: Decimal
    hours_delta: float


class ApprovalRuleResponse(BaseModel):
    """Details about an approval rule."""
    id: str
    name: str
    action: str
    priority: int


class PendingApprovalsResponse(BaseModel):
    """List of pending approvals for a user."""
    pending_count: int
    approvals: List[ApprovalResponse]


# ============================================================================
# Routes
# ============================================================================


@router.post("/api/approvals/submit/{roster_id}")
async def submit_for_approval(
    roster_id: str,
    user: UserContext = Depends(get_current_user),
) -> ApprovalResponse:
    """
    POST /api/approvals/submit/{roster_id}

    Submit a roster for approval.

    Applies tier-based rules:
    - Starter: Immediately active (no approval workflow)
    - Pro: Auto-approve if rules pass, else manager review
    - Enterprise: Multi-level with escalation for high-cost

    Returns:
        ApprovalResponse with status and next steps
    """
    # Tenant gate: the roster must belong to one of the caller's venues.
    roster = get_db().get_roster(roster_id)
    if not roster:
        raise HTTPException(status_code=404, detail=f"Roster {roster_id} not found")
    _enforce_venue_or_404(roster.venue_id, f"Roster {roster_id} not found")

    try:
        request = await approval_workflow.submit_for_approval(
            roster_id=roster_id,
            submitted_by=user.user_id,
        )
        audit("approval.submit", request.venue_id, "approval_request", request.id,
              roster_id=roster_id, status=request.status.value, tier=request.tier,
              failed_rules=list(request.failed_rules or []))

        return ApprovalResponse(
            id=request.id,
            roster_id=request.roster_id,
            venue_id=request.venue_id,
            submitted_by=request.submitted_by,
            submitted_at=request.submitted_at,
            status=request.status.value,
            reviewed_by=request.reviewed_by,
            reviewed_at=request.reviewed_at,
            review_notes=request.review_notes,
            revision_number=request.revision_number,
            escalated_at=request.escalated_at,
            escalated_to=request.escalated_to,
            tier=request.tier,
            auto_approved_by_rules=request.auto_approved_by_rules,
            failed_rules=request.failed_rules,
        )
    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"Error submitting roster for approval: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/api/approvals/pending")
async def get_pending_approvals(
    venue_id: Optional[str] = Query(None),
    user: UserContext = Depends(get_current_user),
) -> PendingApprovalsResponse:
    """
    GET /api/approvals/pending?venue_id=...

    Get approvals pending review by current user.

    Optional query params:
    - venue_id: Filter by specific venue

    Returns:
        List of pending ApprovalRequest objects
    """
    # Tenant gate: an explicit venue filter must be one of the caller's venues.
    if venue_id:
        enforce_venue_access(venue_id)

    try:
        approvals = approval_workflow.get_pending_approvals(
            user_id=user.user_id,
            venue_id=venue_id,
        )

        # Non-owners only ever see approvals for their own venues.
        if not user.is_owner:
            approvals = [a for a in approvals if _user_can_access_venue(user, a.venue_id)]

        return PendingApprovalsResponse(
            pending_count=len(approvals),
            approvals=[
                ApprovalResponse(
                    id=a.id,
                    roster_id=a.roster_id,
                    venue_id=a.venue_id,
                    submitted_by=a.submitted_by,
                    submitted_at=a.submitted_at,
                    status=a.status.value,
                    reviewed_by=a.reviewed_by,
                    reviewed_at=a.reviewed_at,
                    review_notes=a.review_notes,
                    revision_number=a.revision_number,
                    escalated_at=a.escalated_at,
                    escalated_to=a.escalated_to,
                    tier=a.tier,
                    auto_approved_by_rules=a.auto_approved_by_rules,
                    failed_rules=a.failed_rules,
                )
                for a in approvals
            ],
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching pending approvals: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/api/approvals/{request_id}/approve")
async def approve_roster(
    request_id: str,
    body: ReviewApprovalRequest,
    user: UserContext = Depends(get_current_user),
) -> ApprovalResponse:
    """
    POST /api/approvals/{request_id}/approve

    Approve a roster after review.

    Body:
        {
            "approved": true,
            "notes": "Looks good to go"
        }

    Returns:
        Updated ApprovalResponse
    """
    # Tenant gate BEFORE acting: 404 if missing or outside the caller's venues.
    _load_approval_request_scoped(request_id)

    try:
        request = await approval_workflow.review(
            request_id=request_id,
            reviewer_id=user.user_id,
            approved=True,
            notes=body.notes,
        )
        audit("approval.approve", request.venue_id, "approval_request", request.id,
              roster_id=request.roster_id, notes=body.notes, status=request.status.value,
              revision=request.revision_number)

        return ApprovalResponse(
            id=request.id,
            roster_id=request.roster_id,
            venue_id=request.venue_id,
            submitted_by=request.submitted_by,
            submitted_at=request.submitted_at,
            status=request.status.value,
            reviewed_by=request.reviewed_by,
            reviewed_at=request.reviewed_at,
            review_notes=request.review_notes,
            revision_number=request.revision_number,
            escalated_at=request.escalated_at,
            escalated_to=request.escalated_to,
            tier=request.tier,
            auto_approved_by_rules=request.auto_approved_by_rules,
            failed_rules=request.failed_rules,
        )
    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"Error approving roster: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/api/approvals/{request_id}/reject")
async def reject_roster(
    request_id: str,
    body: ReviewApprovalRequest,
    user: UserContext = Depends(get_current_user),
) -> ApprovalResponse:
    """
    POST /api/approvals/{request_id}/reject

    Reject a roster with notes for resubmission.

    Body:
        {
            "approved": false,
            "notes": "Overtime too high, please revise"
        }

    Returns:
        Updated ApprovalResponse
    """
    # Tenant gate BEFORE acting: 404 if missing or outside the caller's venues.
    _load_approval_request_scoped(request_id)

    try:
        if body.approved:
            raise ValueError("Cannot use approve endpoint for rejection")

        request = await approval_workflow.review(
            request_id=request_id,
            reviewer_id=user.user_id,
            approved=False,
            notes=body.notes,
        )
        audit("approval.reject", request.venue_id, "approval_request", request.id,
              roster_id=request.roster_id, notes=body.notes, status=request.status.value,
              revision=request.revision_number)

        return ApprovalResponse(
            id=request.id,
            roster_id=request.roster_id,
            venue_id=request.venue_id,
            submitted_by=request.submitted_by,
            submitted_at=request.submitted_at,
            status=request.status.value,
            reviewed_by=request.reviewed_by,
            reviewed_at=request.reviewed_at,
            review_notes=request.review_notes,
            revision_number=request.revision_number,
            escalated_at=request.escalated_at,
            escalated_to=request.escalated_to,
            tier=request.tier,
            auto_approved_by_rules=request.auto_approved_by_rules,
            failed_rules=request.failed_rules,
        )
    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error rejecting roster: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/api/approvals/{request_id}/escalate")
async def escalate_approval(
    request_id: str,
    user: UserContext = Depends(get_current_user),
) -> ApprovalResponse:
    """
    POST /api/approvals/{request_id}/escalate

    Manually escalate an approval to next level.

    Auto-escalates after 24 hours of inactivity.

    Returns:
        Updated ApprovalResponse
    """
    # Tenant gate BEFORE acting: 404 if missing or outside the caller's venues.
    _load_approval_request_scoped(request_id)

    try:
        request = await approval_workflow.escalate(request_id=request_id)
        audit("approval.escalate", request.venue_id, "approval_request", request.id,
              roster_id=request.roster_id, escalated_to=request.escalated_to)

        return ApprovalResponse(
            id=request.id,
            roster_id=request.roster_id,
            venue_id=request.venue_id,
            submitted_by=request.submitted_by,
            submitted_at=request.submitted_at,
            status=request.status.value,
            reviewed_by=request.reviewed_by,
            reviewed_at=request.reviewed_at,
            review_notes=request.review_notes,
            revision_number=request.revision_number,
            escalated_at=request.escalated_at,
            escalated_to=request.escalated_to,
            tier=request.tier,
            auto_approved_by_rules=request.auto_approved_by_rules,
            failed_rules=request.failed_rules,
        )
    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"Error escalating approval: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/api/approvals/history/{roster_id}")
async def get_approval_history(
    roster_id: str,
    user: UserContext = Depends(get_current_user),
) -> List[ApprovalResponse]:
    """
    GET /api/approvals/history/{roster_id}

    Get approval history for a roster (all approval actions).

    Returns:
        List of all ApprovalResponse objects for this roster
    """
    # Tenant gate: if the roster exists it must be in one of the caller's venues
    # (404 on denial so roster ids are not an oracle).
    roster = get_db().get_roster(roster_id)
    if roster is not None:
        _enforce_venue_or_404(roster.venue_id, f"Roster {roster_id} not found")

    try:
        approvals = approval_workflow.get_approval_history(roster_id=roster_id)

        # Belt and braces: never return another tenant's approval records even
        # if the roster row itself has gone.
        if not user.is_owner:
            approvals = [a for a in approvals if _user_can_access_venue(user, a.venue_id)]

        return [
            ApprovalResponse(
                id=a.id,
                roster_id=a.roster_id,
                venue_id=a.venue_id,
                submitted_by=a.submitted_by,
                submitted_at=a.submitted_at,
                status=a.status.value,
                reviewed_by=a.reviewed_by,
                reviewed_at=a.reviewed_at,
                review_notes=a.review_notes,
                revision_number=a.revision_number,
                escalated_at=a.escalated_at,
                escalated_to=a.escalated_to,
                tier=a.tier,
                auto_approved_by_rules=a.auto_approved_by_rules,
                failed_rules=a.failed_rules,
            )
            for a in approvals
        ]
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching approval history: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/api/approvals/{request_id}/diff")
async def get_revision_diff(
    request_id: str,
    revision_a: int = Query(...),
    revision_b: int = Query(...),
    user: UserContext = Depends(get_current_user),
) -> RosterDiffResponse:
    """
    GET /api/approvals/{request_id}/diff?revision_a=1&revision_b=2

    Get diff between two roster revisions.

    Query params:
    - revision_a: First revision number
    - revision_b: Second revision number

    Returns:
        RosterDiffResponse with added/removed/changed shifts
    """
    # Tenant gate: 404 if missing or outside the caller's venues.
    request_dict = _load_approval_request_scoped(request_id)

    try:
        roster_id = request_dict["roster_id"]

        diff = approval_workflow.diff_revisions(
            roster_id=roster_id,
            revision_a=revision_a,
            revision_b=revision_b,
        )

        return RosterDiffResponse(
            roster_id=diff.roster_id,
            revision_a=diff.revision_a,
            revision_b=diff.revision_b,
            added_shifts=diff.added_shifts,
            removed_shifts=diff.removed_shifts,
            changed_shifts=diff.changed_shifts,
            cost_delta=diff.cost_delta,
            hours_delta=diff.hours_delta,
        )
    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"Error fetching revision diff: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/api/approvals/rules")
async def list_approval_rules(
    user: UserContext = Depends(get_current_user),
) -> List[ApprovalRuleResponse]:
    """
    GET /api/approvals/rules

    List all active approval rules and their configurations.

    Returns:
        List of ApprovalRuleResponse objects
    """
    try:
        rules = []
        for rule in approval_workflow.rules.values():
            rules.append(
                ApprovalRuleResponse(
                    id=rule.id,
                    name=rule.name,
                    action=rule.action.value,
                    priority=rule.priority,
                )
            )

        # Sort by priority descending
        rules.sort(key=lambda r: r.priority, reverse=True)

        return rules
    except Exception as e:
        logger.error(f"Error listing approval rules: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")
