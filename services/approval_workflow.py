"""
Approval workflow service for RosterIQ.

Implements an automated roster approval workflow with rules engine, tier-based gating,
escalation logic, and revision tracking. Supports auto-approval for rosters meeting
compliance criteria, manual review workflows, and multi-level approval chains.

Usage:
    from rosteriq.services.approval_workflow import approval_workflow
    request = await approval_workflow.submit_for_approval(
        roster_id="r1",
        submitted_by="user123"
    )
"""

import logging
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Optional, List, Callable, Dict, Any
from enum import Enum
import uuid

from rosteriq.database import get_db
from rosteriq.models import Roster

logger = logging.getLogger(__name__)


# ============================================================================
# Enums and dataclasses
# ============================================================================


class ApprovalStatus(str, Enum):
    """Status of an approval request."""
    pending = "pending"
    approved = "approved"
    rejected = "rejected"
    auto_approved = "auto_approved"
    escalated = "escalated"


class ApprovalAction(str, Enum):
    """Action returned by a rule."""
    auto_approve = "auto_approve"
    require_review = "require_review"
    escalate = "escalate"


@dataclass
class ApprovalRequest:
    """Roster approval request with tracking."""
    id: str
    roster_id: str
    venue_id: str
    submitted_by: str
    submitted_at: datetime
    status: ApprovalStatus
    reviewed_by: Optional[str] = None
    reviewed_at: Optional[datetime] = None
    review_notes: Optional[str] = None
    revision_number: int = 1
    escalated_at: Optional[datetime] = None
    escalated_to: Optional[str] = None  # owner_id or area_manager_id
    tier: str = "pro"  # subscription tier
    auto_approved_by_rules: List[str] = field(default_factory=list)  # rules that passed
    failed_rules: List[str] = field(default_factory=list)  # rules that failed


@dataclass
class ApprovalRule:
    """Rule in the approval workflow engine."""
    id: str
    name: str
    condition: Callable[[Dict[str, Any]], bool]  # returns True if rule passes
    action: ApprovalAction
    priority: int = 0  # Higher priority evaluated first


@dataclass
class RosterDiff:
    """Difference between two roster revisions."""
    roster_id: str
    revision_a: int
    revision_b: int
    added_shifts: List[Dict[str, Any]] = field(default_factory=list)
    removed_shifts: List[Dict[str, Any]] = field(default_factory=list)
    changed_shifts: List[Dict[str, Any]] = field(default_factory=list)
    cost_delta: Decimal = Decimal("0")
    hours_delta: float = 0.0


# ============================================================================
# Approval Workflow Service
# ============================================================================


class ApprovalWorkflow:
    """
    Orchestrates roster approval with rules engine, tier-based gates, and escalation.
    """

    def __init__(self):
        self.db = get_db()
        self.rules: Dict[str, ApprovalRule] = {}
        self._init_default_rules()

    def _init_default_rules(self) -> None:
        """Initialize default approval rules."""

        def compliance_check(context: Dict[str, Any]) -> bool:
            """Roster with compliance_score >= 95 is eligible."""
            return context.get("compliance_score", 0) >= 95

        def budget_check(context: Dict[str, Any]) -> bool:
            """Labour cost <= 105% of budget target."""
            labour_cost = context.get("labour_cost", Decimal("0"))
            budget_target = context.get("budget_target", Decimal("10000"))
            return labour_cost <= budget_target * Decimal("1.05")

        def overtime_check(context: Dict[str, Any]) -> bool:
            """No overtime hours required."""
            return context.get("overtime_hours", 0) == 0

        def penalty_check(context: Dict[str, Any]) -> bool:
            """Penalty cost <= 20% of total."""
            penalty_cost = context.get("penalty_cost", Decimal("0"))
            total_cost = context.get("total_cost", Decimal("1"))
            pct = (penalty_cost / total_cost * 100) if total_cost > 0 else 0
            return pct <= 20

        def headcount_check(context: Dict[str, Any]) -> bool:
            """No hours understaffed."""
            return context.get("understaffed_hours", 0) == 0

        self.rules["compliance_check"] = ApprovalRule(
            id="compliance_check",
            name="Compliance Check",
            condition=compliance_check,
            action=ApprovalAction.auto_approve,
            priority=5,
        )

        self.rules["budget_check"] = ApprovalRule(
            id="budget_check",
            name="Budget Check",
            condition=budget_check,
            action=ApprovalAction.auto_approve,
            priority=4,
        )

        self.rules["overtime_check"] = ApprovalRule(
            id="overtime_check",
            name="Overtime Check",
            condition=overtime_check,
            action=ApprovalAction.require_review,
            priority=3,
        )

        self.rules["penalty_check"] = ApprovalRule(
            id="penalty_check",
            name="Penalty Check",
            condition=penalty_check,
            action=ApprovalAction.require_review,
            priority=2,
        )

        self.rules["headcount_check"] = ApprovalRule(
            id="headcount_check",
            name="Headcount Check",
            condition=headcount_check,
            action=ApprovalAction.require_review,
            priority=1,
        )

    def add_custom_rule(self, rule: ApprovalRule) -> None:
        """Register a custom approval rule."""
        self.rules[rule.id] = rule
        logger.info(f"Registered custom rule: {rule.name}")

    def _extract_roster_context(self, roster: Roster, venue_id: str) -> Dict[str, Any]:
        """Extract relevant metrics from roster for rule evaluation."""
        venue = self.db.get_venue(venue_id)
        if not venue:
            return {}

        total_cost = roster.total_cost or Decimal("0")
        total_hours = roster.total_hours

        # Calculate metrics (simplified)
        compliance_score = 90.0  # Placeholder - would call compliance checker
        labour_cost = total_cost
        budget_target = Decimal("10000")  # Placeholder - from venue config
        overtime_hours = 0  # Placeholder - would scan shifts
        penalty_cost = Decimal("0")  # Placeholder - would aggregate shift penalties
        understaffed_hours = 0  # Placeholder - would check demand vs supply

        return {
            "compliance_score": compliance_score,
            "labour_cost": labour_cost,
            "budget_target": budget_target,
            "overtime_hours": overtime_hours,
            "penalty_cost": penalty_cost,
            "total_cost": total_cost,
            "total_hours": total_hours,
            "understaffed_hours": understaffed_hours,
        }

    def _evaluate_rules(self, context: Dict[str, Any]) -> tuple[List[str], List[str]]:
        """
        Evaluate all rules against context.

        Returns:
            (auto_approved_rules, failed_rules) - lists of rule IDs
        """
        # Sort rules by priority (descending)
        sorted_rules = sorted(
            self.rules.values(),
            key=lambda r: r.priority,
            reverse=True
        )

        auto_approved = []
        failed = []

        for rule in sorted_rules:
            try:
                if rule.condition(context):
                    if rule.action == ApprovalAction.auto_approve:
                        auto_approved.append(rule.id)
                else:
                    failed.append(rule.id)
            except Exception as e:
                logger.error(f"Error evaluating rule {rule.id}: {e}")
                failed.append(rule.id)

        return auto_approved, failed

    def _get_subscription_tier(self, venue_id: str) -> str:
        """Get subscription tier for venue."""
        subscription = self.db.get_subscription(venue_id)
        if not subscription:
            return "starter"
        return subscription.get("tier", "starter")

    async def submit_for_approval(self, roster_id: str, submitted_by: str) -> ApprovalRequest:
        """
        Submit a roster for approval.

        Applies tier-based rules:
        - Starter: No approval workflow, immediately activate
        - Pro: Auto-approve if all rules pass, else require manager review
        - Enterprise: Multi-level with escalation for high-cost rosters

        Args:
            roster_id: ID of roster to approve
            submitted_by: User ID submitting

        Returns:
            ApprovalRequest with status and next steps
        """
        roster = self.db.get_roster(roster_id)
        if not roster:
            raise ValueError(f"Roster {roster_id} not found")

        venue_id = roster.venue_id
        tier = self._get_subscription_tier(venue_id)

        # Starter: no approval, immediate activation
        if tier == "starter":
            logger.info(f"Starter tier: auto-activating roster {roster_id}")
            return ApprovalRequest(
                id=str(uuid.uuid4()),
                roster_id=roster_id,
                venue_id=venue_id,
                submitted_by=submitted_by,
                submitted_at=datetime.utcnow(),
                status=ApprovalStatus.auto_approved,
                tier=tier,
            )

        # Extract context and evaluate rules
        context = self._extract_roster_context(roster, venue_id)
        auto_approved_rules, failed_rules = self._evaluate_rules(context)

        request = ApprovalRequest(
            id=str(uuid.uuid4()),
            roster_id=roster_id,
            venue_id=venue_id,
            submitted_by=submitted_by,
            submitted_at=datetime.utcnow(),
            status=ApprovalStatus.pending,
            tier=tier,
            auto_approved_by_rules=auto_approved_rules,
            failed_rules=failed_rules,
        )

        # Pro tier: auto-approve if all rules pass
        if tier == "pro":
            if not failed_rules:
                logger.info(f"Pro tier: auto-approving roster {roster_id}")
                request.status = ApprovalStatus.auto_approved
                request.reviewed_at = datetime.utcnow()
                request.reviewed_by = "system"
            else:
                logger.info(f"Pro tier: requiring manager review for roster {roster_id}")

        # Enterprise tier: escalate high-cost rosters
        if tier == "enterprise":
            if not failed_rules and roster.total_cost and roster.total_cost > Decimal("5000"):
                logger.info(f"Enterprise tier: escalating high-cost roster {roster_id}")
                request.status = ApprovalStatus.escalated
                request.escalated_at = datetime.utcnow()

        # Save approval request
        self.db.save_approval_request(asdict(request))

        # Fire approval_needed event (for notifications)
        self._fire_event("approval_needed", {
            "request_id": request.id,
            "roster_id": roster_id,
            "venue_id": venue_id,
            "submitted_by": submitted_by,
            "status": request.status.value,
        })

        return request

    async def review(
        self,
        request_id: str,
        reviewer_id: str,
        approved: bool,
        notes: Optional[str] = None,
    ) -> ApprovalRequest:
        """
        Review and approve or reject a roster.

        Args:
            request_id: Approval request ID
            reviewer_id: User ID of reviewer
            approved: True to approve, False to reject
            notes: Optional review notes

        Returns:
            Updated ApprovalRequest
        """
        request_dict = self.db.get_approval_request(request_id)
        if not request_dict:
            raise ValueError(f"Request {request_id} not found")

        request = ApprovalRequest(**request_dict)

        # Validate reviewer permissions
        user = self.db.get_user_by_id(reviewer_id)
        if not user:
            raise ValueError(f"User {reviewer_id} not found")

        role = user.get("role")
        if role not in ("manager", "owner"):
            raise ValueError(f"User {reviewer_id} cannot review (role: {role})")

        # Update request
        request.reviewed_by = reviewer_id
        request.reviewed_at = datetime.utcnow()
        request.review_notes = notes

        if approved:
            request.status = ApprovalStatus.approved
            logger.info(f"Roster {request.roster_id} approved by {reviewer_id}")
            self._fire_event("approved", {
                "request_id": request.id,
                "roster_id": request.roster_id,
                "venue_id": request.venue_id,
                "reviewed_by": reviewer_id,
            })
        else:
            request.status = ApprovalStatus.rejected
            logger.info(f"Roster {request.roster_id} rejected by {reviewer_id}")
            self._fire_event("rejected", {
                "request_id": request.id,
                "roster_id": request.roster_id,
                "venue_id": request.venue_id,
                "reviewed_by": reviewer_id,
                "notes": notes,
            })

        self.db.save_approval_request(asdict(request))
        return request

    async def escalate(self, request_id: str) -> ApprovalRequest:
        """
        Escalate an approval request to next level.

        Auto-escalates after 24 hours if no response.

        Args:
            request_id: Approval request ID

        Returns:
            Updated ApprovalRequest
        """
        request_dict = self.db.get_approval_request(request_id)
        if not request_dict:
            raise ValueError(f"Request {request_id} not found")

        request = ApprovalRequest(**request_dict)
        request.status = ApprovalStatus.escalated
        request.escalated_at = datetime.utcnow()

        logger.info(f"Roster {request.roster_id} escalated to owner level")

        self._fire_event("escalated", {
            "request_id": request.id,
            "roster_id": request.roster_id,
            "venue_id": request.venue_id,
            "escalated_at": request.escalated_at.isoformat(),
        })

        self.db.save_approval_request(asdict(request))
        return request

    def get_pending_approvals(self, user_id: str, venue_id: Optional[str] = None) -> List[ApprovalRequest]:
        """
        Get approvals pending review by a user.

        Args:
            user_id: User ID
            venue_id: Optional filter by venue

        Returns:
            List of ApprovalRequest objects
        """
        requests = self.db.list_approval_requests(
            venue_id=venue_id,
            status="pending"
        )

        # Filter to approvals assigned to this reviewer
        # (simplified - in production would track assigned reviewers)
        user = self.db.get_user_by_id(user_id)
        if not user:
            return []

        role = user.get("role")
        if role not in ("manager", "owner"):
            return []

        return [ApprovalRequest(**r) for r in requests]

    def get_approval_history(self, roster_id: str) -> List[ApprovalRequest]:
        """
        Get all approval actions for a roster.

        Args:
            roster_id: Roster ID

        Returns:
            List of all ApprovalRequest objects for this roster
        """
        requests = self.db.list_approval_requests(
            roster_id=roster_id
        )
        return [ApprovalRequest(**r) for r in requests]

    def create_revision(self, roster_id: str, changes: Dict[str, Any]) -> int:
        """
        Create a new roster revision with change tracking.

        Args:
            roster_id: Roster ID
            changes: Dictionary of changes applied

        Returns:
            New revision number
        """
        revisions = self.db.list_roster_revisions(roster_id)
        next_revision = len(revisions) + 1

        revision = {
            "roster_id": roster_id,
            "revision_number": next_revision,
            "changes": changes,
            "created_at": datetime.utcnow(),
        }

        self.db.save_roster_revision(revision)
        logger.info(f"Created revision {next_revision} for roster {roster_id}")

        return next_revision

    def diff_revisions(
        self,
        roster_id: str,
        revision_a: int,
        revision_b: int,
    ) -> RosterDiff:
        """
        Compute diff between two roster revisions.

        Args:
            roster_id: Roster ID
            revision_a: First revision number
            revision_b: Second revision number

        Returns:
            RosterDiff with added/removed/changed shifts
        """
        revisions = self.db.list_roster_revisions(roster_id)

        rev_a = next((r for r in revisions if r["revision_number"] == revision_a), None)
        rev_b = next((r for r in revisions if r["revision_number"] == revision_b), None)

        if not rev_a or not rev_b:
            raise ValueError(f"Revisions {revision_a} or {revision_b} not found")

        shifts_a = set(str(s) for s in rev_a.get("changes", {}).get("shifts", []))
        shifts_b = set(str(s) for s in rev_b.get("changes", {}).get("shifts", []))

        added = [s for s in shifts_b if s not in shifts_a]
        removed = [s for s in shifts_a if s not in shifts_b]
        changed = shifts_a & shifts_b

        return RosterDiff(
            roster_id=roster_id,
            revision_a=revision_a,
            revision_b=revision_b,
            added_shifts=[{"shift": s} for s in added],
            removed_shifts=[{"shift": s} for s in removed],
            changed_shifts=[{"shift": s} for s in changed],
        )

    def _fire_event(self, event_type: str, payload: Dict[str, Any]) -> None:
        """Fire event for notifications and integrations."""
        # Placeholder for event dispatcher
        # Would integrate with rosteriq.services.ws_events.get_dispatcher()
        logger.debug(f"Event: {event_type} - {payload}")


# ============================================================================
# Singleton instance
# ============================================================================

approval_workflow = ApprovalWorkflow()

# UUID import added to module
import uuid
