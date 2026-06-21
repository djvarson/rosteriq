"""
Automated Roster Publishing Workflow for RosterIQ.

Orchestrates the complete lifecycle of a roster from DRAFT through publication,
including state management, conflict detection, approval workflows, Tanda integration,
and staff notifications.

State machine:
    DRAFT → PENDING_REVIEW → APPROVED → PUBLISHING → PUBLISHED → ARCHIVED

    Also: REJECTED (from PENDING_REVIEW), FAILED (from PUBLISHING)

Usage:
    from rosteriq.services.roster_publisher import publisher
    result = await publisher.publish_roster(roster_id="r1", publisher_id="u1")
    print(result.success, result.tanda_push_result)
"""

import logging
from dataclasses import dataclass, field, asdict
from datetime import datetime, date
from decimal import Decimal
from enum import Enum
from typing import Optional, List, Dict, Any, Tuple
import uuid

from rosteriq.database import get_db
from rosteriq.models import Roster, Shift, VenueConfig, Employee
from rosteriq.services.conflict_detector import ConflictDetector, ConflictSeverity
from rosteriq.services.approval_workflow import approval_workflow, ApprovalStatus
from rosteriq.services.notification_hub import get_notification_hub, NotificationEventType
from rosteriq.services.tanda_roster_push import TandaRosterPush as TandaRosterPusher

logger = logging.getLogger(__name__)


# ============================================================================
# Enums and Dataclasses
# ============================================================================


class RosterPublishState(str, Enum):
    """Roster publishing state."""
    DRAFT = "draft"
    PENDING_REVIEW = "pending_review"
    APPROVED = "approved"
    PUBLISHING = "publishing"
    PUBLISHED = "published"
    ARCHIVED = "archived"
    REJECTED = "rejected"
    FAILED = "failed"


@dataclass
class PublishResult:
    """Result of a roster publishing operation."""
    success: bool
    roster_id: str
    state: str
    conflicts_found: int
    critical_conflicts: int
    tanda_push_result: Optional[Dict[str, Any]] = None
    notifications_sent: int = 0
    message: str = ""
    timestamp: str = ""
    approval_request_id: Optional[str] = None


@dataclass
class RosterState:
    """Current state and history of a roster."""
    roster_id: str
    current_state: str
    state_history: List[Tuple[str, str, str]] = field(default_factory=list)
    # state_history: list of (state, timestamp_iso, actor_id)
    created_at: str = ""
    last_modified_by: str = ""
    last_modified_at: str = ""


@dataclass
class PublicationEvent:
    """Event logged during roster publication."""
    roster_id: str
    venue_id: str
    state: str
    publisher_id: str
    timestamp: str
    details: Dict[str, Any] = field(default_factory=dict)


# ============================================================================
# RosterPublisher Service
# ============================================================================


class RosterPublisher:
    """
    Orchestrates the complete roster publishing lifecycle.

    Manages state transitions, conflict detection, approvals, Tanda integration,
    and notifications.
    """

    def __init__(self):
        """Initialize publisher with dependencies."""
        self.conflict_detector = ConflictDetector()
        self.notification_hub = get_notification_hub()
        # Built lazily per-venue — TandaRosterPush needs that venue's authed Tanda
        # adapter, so it can't be constructed at init (doing so crashed import and
        # silently dropped the ENTIRE publishing router).
        self.tanda_pusher = None

    @property
    def db(self):
        # Resolve the store dynamically rather than caching at init — `publisher`
        # is a module-level singleton, so a cached db would point at a stale store
        # after the test harness resets the global DB (and is simply more correct).
        return get_db()

    def _tanda_pusher_for(self, venue):
        """Build a TandaRosterPush for a venue's Tanda connection, or None if the
        venue has no usable Tanda install (publish then proceeds locally)."""
        try:
            from rosteriq.services.tanda_plugin import TandaPluginService
            state_code = venue.state.value if hasattr(venue.state, "value") else str(venue.state)
            adapter = TandaPluginService(self.db).build_adapter(venue.tanda_org_id, state_code)
            return TandaRosterPusher(adapter)
        except Exception as e:
            logger.warning(f"Tanda push unavailable for venue {getattr(venue, 'id', '?')}: {e}")
            return None

    async def publish_roster(
        self,
        roster_id: str,
        publisher_id: str,
        skip_approval: bool = False,
    ) -> PublishResult:
        """
        Orchestrate the full roster publishing workflow.

        Steps:
        1. Validate roster exists and is in valid state
        2. Run conflict detection
        3. If critical conflicts, block with details
        4. If skip_approval=False, submit to approval workflow
        5. On approval, transition to APPROVED state
        6. Push to Tanda (if configured)
        7. If successful, transition to PUBLISHED
        8. Notify all affected staff
        9. Log publication event

        Args:
            roster_id: ID of roster to publish
            publisher_id: User ID triggering the publication
            skip_approval: If True, bypass approval workflow (admin only)

        Returns:
            PublishResult with outcome details
        """
        start_time = datetime.utcnow()
        result = PublishResult(
            success=False,
            roster_id=roster_id,
            state=RosterPublishState.DRAFT.value,
            conflicts_found=0,
            critical_conflicts=0,
            timestamp=start_time.isoformat(),
        )

        try:
            # Step 1: Validate roster exists and state
            roster = self.db.get_roster(roster_id)
            if not roster:
                result.message = f"Roster {roster_id} not found"
                logger.error(result.message)
                return result

            current_state_str = self.db.get_roster_state(roster_id)
            # FAILED and a stale PUBLISHING are retryable — a transient Tanda outage
            # or a mid-publish crash must not permanently block re-publishing.
            if current_state_str not in (
                RosterPublishState.DRAFT.value,
                RosterPublishState.REJECTED.value,
                RosterPublishState.FAILED.value,
                RosterPublishState.PUBLISHING.value,
            ):
                result.message = (
                    f"Roster in {current_state_str} state; "
                    f"can only publish from DRAFT, REJECTED, FAILED or a stalled PUBLISHING"
                )
                logger.error(result.message)
                return result

            result.state = current_state_str

            # Step 2: Get venue and employees
            venue = self.db.get_venue(roster.venue_id)
            if not venue:
                result.message = f"Venue {roster.venue_id} not found"
                logger.error(result.message)
                return result

            employees = [e for e in self.db.list_employees() if getattr(e, "venue_id", None) == roster.venue_id]

            # Step 3: Detect conflicts
            conflicts = self.conflict_detector.detect_conflicts(
                roster=roster,
                venue=venue,
                employees=employees,
            )
            result.conflicts_found = len(conflicts)
            result.critical_conflicts = len(
                [c for c in conflicts if c.severity == ConflictSeverity.CRITICAL]
            )

            # Step 4: Block if critical conflicts
            if result.critical_conflicts > 0:
                critical_msgs = [
                    c.message for c in conflicts
                    if c.severity == ConflictSeverity.CRITICAL
                ]
                result.message = (
                    f"Cannot publish: {result.critical_conflicts} critical conflicts: "
                    f"{'; '.join(critical_msgs[:3])}"
                )
                logger.warning(result.message)

                # Transition to FAILED state
                self.transition_state(
                    roster_id,
                    RosterPublishState.FAILED.value,
                    f"Published by {publisher_id}: critical conflicts blocked",
                )
                result.state = RosterPublishState.FAILED.value
                return result

            # Step 5: Determine approval path
            if skip_approval:
                # Admin override - go straight to APPROVED
                logger.info(
                    f"Roster {roster_id} approved with admin override by {publisher_id}"
                )
                self.transition_state(
                    roster_id,
                    RosterPublishState.APPROVED.value,
                    f"Admin override by {publisher_id}",
                )
                result.state = RosterPublishState.APPROVED.value
                approval_request_id = None
            else:
                # Submit to approval workflow
                logger.info(
                    f"Submitting roster {roster_id} for approval by {publisher_id}"
                )
                approval_request = await approval_workflow.submit_for_approval(
                    roster_id=roster_id,
                    submitted_by=publisher_id,
                )
                result.approval_request_id = approval_request.id

                # Check if auto-approved
                if approval_request.status == ApprovalStatus.auto_approved:
                    logger.info(
                        f"Roster {roster_id} auto-approved: "
                        f"{len(approval_request.auto_approved_by_rules)} rules passed"
                    )
                    self.transition_state(
                        roster_id,
                        RosterPublishState.APPROVED.value,
                        f"Auto-approved by system ({len(approval_request.auto_approved_by_rules)} rules)",
                    )
                    result.state = RosterPublishState.APPROVED.value
                else:
                    # Pending review
                    logger.info(f"Roster {roster_id} submitted for manager review")
                    self.transition_state(
                        roster_id,
                        RosterPublishState.PENDING_REVIEW.value,
                        f"Submitted for approval by {publisher_id}",
                    )
                    result.state = RosterPublishState.PENDING_REVIEW.value
                    result.success = True
                    result.message = "Roster submitted for approval"
                    self._log_publication_event(roster_id, venue.id, RosterPublishState.PENDING_REVIEW.value, publisher_id)
                    return result

            # Step 6: Push to Tanda (if configured)
            tanda_result = None
            if venue.tanda_org_id:
                logger.info(f"Pushing roster {roster_id} to Tanda (org: {venue.tanda_org_id})")
                self.transition_state(
                    roster_id,
                    RosterPublishState.PUBLISHING.value,
                    f"Pushing to Tanda by {publisher_id}",
                )
                result.state = RosterPublishState.PUBLISHING.value

                try:
                    pusher = self._tanda_pusher_for(venue)
                    if pusher is None:
                        raise RuntimeError("Tanda is not connected for this venue")
                    tanda_result = await pusher.push_roster(
                        roster=roster,
                        venue_id=venue.tanda_org_id,
                        dry_run=False,
                    )
                    # Convert PushResult to dict
                    result.tanda_push_result = {
                        "success": tanda_result.success,
                        "message": tanda_result.message,
                        "shifts_pushed": tanda_result.shifts_pushed,
                        "shifts_failed": tanda_result.shifts_failed,
                    }

                    if not tanda_result.success:
                        error_msg = tanda_result.message or "Unknown error"
                        result.message = f"Tanda push failed: {error_msg}"
                        logger.error(result.message)
                        self.transition_state(
                            roster_id,
                            RosterPublishState.FAILED.value,
                            f"Tanda push failed: {error_msg}",
                        )
                        result.state = RosterPublishState.FAILED.value
                        return result

                except Exception as e:
                    result.message = f"Tanda push error: {str(e)}"
                    logger.error(result.message, exc_info=True)
                    self.transition_state(
                        roster_id,
                        RosterPublishState.FAILED.value,
                        f"Tanda push exception: {str(e)}",
                    )
                    result.state = RosterPublishState.FAILED.value
                    return result
            else:
                logger.info(f"No Tanda org_id for venue {venue.id}, skipping Tanda push")

            # Step 7: Transition to PUBLISHED
            logger.info(f"Transitioning roster {roster_id} to PUBLISHED")
            self.transition_state(
                roster_id,
                RosterPublishState.PUBLISHED.value,
                f"Published by {publisher_id}",
            )
            result.state = RosterPublishState.PUBLISHED.value

            # Step 8: Notify all affected staff
            notifications_sent = await self._notify_staff(
                roster=roster,
                venue=venue,
                conflicts_count=result.conflicts_found,
            )
            result.notifications_sent = notifications_sent

            # Step 9: Log publication event
            self._log_publication_event(
                roster_id,
                venue.id,
                RosterPublishState.PUBLISHED.value,
                publisher_id,
            )

            result.success = True
            result.message = (
                f"Roster published successfully. "
                f"{result.conflicts_found} conflicts (warning level). "
                f"{notifications_sent} staff notified."
            )
            logger.info(result.message)

            return result

        except Exception as e:
            result.message = f"Publication failed: {str(e)}"
            logger.error(result.message, exc_info=True)
            self.transition_state(
                roster_id,
                RosterPublishState.FAILED.value,
                f"Exception: {str(e)}",
            )
            result.state = RosterPublishState.FAILED.value
            return result

    async def on_approval_received(
        self,
        roster_id: str,
        approved: bool,
        reviewer_id: str,
    ) -> PublishResult:
        """
        Called when an approval decision is made.

        If approved, transitions to APPROVED and initiates Tanda push.
        If rejected, transitions back to DRAFT for revision.

        Args:
            roster_id: Roster ID
            approved: True if approved, False if rejected
            reviewer_id: User ID of reviewer

        Returns:
            PublishResult with outcome
        """
        result = PublishResult(
            success=False,
            roster_id=roster_id,
            state="",
            conflicts_found=0,
            critical_conflicts=0,
            timestamp=datetime.utcnow().isoformat(),
        )

        try:
            roster = self.db.get_roster(roster_id)
            if not roster:
                result.message = f"Roster {roster_id} not found"
                return result

            venue = self.db.get_venue(roster.venue_id)

            if approved:
                logger.info(f"Roster {roster_id} approved by {reviewer_id}")
                self.transition_state(
                    roster_id,
                    RosterPublishState.APPROVED.value,
                    f"Approved by {reviewer_id}",
                )
                result.state = RosterPublishState.APPROVED.value

                # Now proceed with Tanda push as in publish_roster
                if venue.tanda_org_id:
                    self.transition_state(
                        roster_id,
                        RosterPublishState.PUBLISHING.value,
                        f"Pushing to Tanda after approval",
                    )

                    try:
                        pusher = self._tanda_pusher_for(venue)
                        if pusher is None:
                            raise RuntimeError("Tanda is not connected for this venue")
                        tanda_result = await pusher.push_roster(
                            roster=roster,
                            venue_id=venue.tanda_org_id,
                            dry_run=False,
                        )
                        result.tanda_push_result = {
                            "success": tanda_result.success,
                            "message": tanda_result.message,
                            "shifts_pushed": tanda_result.shifts_pushed,
                            "shifts_failed": tanda_result.shifts_failed,
                        }

                        if not tanda_result.success:
                            error_msg = tanda_result.message or "Unknown error"
                            result.message = f"Tanda push failed: {error_msg}"
                            self.transition_state(
                                roster_id,
                                RosterPublishState.FAILED.value,
                                f"Tanda push failed: {error_msg}",
                            )
                            result.state = RosterPublishState.FAILED.value
                            return result
                    except Exception as e:
                        result.message = f"Tanda push error: {str(e)}"
                        self.transition_state(
                            roster_id,
                            RosterPublishState.FAILED.value,
                            f"Tanda push exception: {str(e)}",
                        )
                        result.state = RosterPublishState.FAILED.value
                        return result

                # Transition to PUBLISHED
                self.transition_state(
                    roster_id,
                    RosterPublishState.PUBLISHED.value,
                    f"Published after approval by {reviewer_id}",
                )
                result.state = RosterPublishState.PUBLISHED.value

                # Notify staff
                notifications_sent = await self._notify_staff(
                    roster=roster,
                    venue=venue,
                    conflicts_count=0,
                )
                result.notifications_sent = notifications_sent

                result.success = True
                result.message = f"Roster approved and published. {notifications_sent} staff notified."

            else:
                # Rejected - go back to DRAFT
                logger.info(f"Roster {roster_id} rejected by {reviewer_id}")
                self.transition_state(
                    roster_id,
                    RosterPublishState.REJECTED.value,
                    f"Rejected by {reviewer_id}",
                )
                result.state = RosterPublishState.REJECTED.value
                result.success = True
                result.message = "Roster rejected and returned to draft for revision"

            return result

        except Exception as e:
            result.message = f"Approval callback failed: {str(e)}"
            logger.error(result.message, exc_info=True)
            return result

    def get_roster_state(self, roster_id: str) -> Optional[RosterState]:
        """
        Get current state and history of a roster.

        Args:
            roster_id: Roster ID

        Returns:
            RosterState with current state and history, or None if not found
        """
        current_state = self.db.get_roster_state(roster_id)
        if not current_state:
            return None

        state_history = self.db.get_roster_state_history(roster_id) or []

        # The store writes each transition as {state, reason, actor_id, at} — read
        # those keys (not the non-existent "actor"/"timestamp", which returned None).
        return RosterState(
            roster_id=roster_id,
            current_state=current_state,
            state_history=[
                (h.get("state"), h.get("at"), h.get("actor_id"))
                for h in state_history
            ],
            last_modified_by=state_history[-1].get("actor_id") if state_history else "",
            last_modified_at=state_history[-1].get("at") if state_history else "",
        )

    def transition_state(
        self,
        roster_id: str,
        new_state: str,
        reason: str,
        actor_id: str = "system",
    ) -> bool:
        """
        Manually transition roster to a new state.

        Args:
            roster_id: Roster ID
            new_state: New state (must be valid RosterPublishState)
            reason: Reason for transition
            actor_id: User ID triggering transition (default "system")

        Returns:
            True if successful, False otherwise
        """
        try:
            # Validate state
            if new_state not in [s.value for s in RosterPublishState]:
                logger.error(f"Invalid state: {new_state}")
                return False

            # Record transition
            self.db.update_roster_state(
                roster_id=roster_id,
                new_state=new_state,
                reason=reason,
                actor_id=actor_id,
            )

            logger.info(
                f"Roster {roster_id} transitioned to {new_state} by {actor_id}: {reason}"
            )
            return True

        except Exception as e:
            logger.error(
                f"Error transitioning roster {roster_id} to {new_state}: {str(e)}",
                exc_info=True,
            )
            return False

    def recall_roster(self, roster_id: str, reason: str) -> bool:
        """
        Recall (un-publish) a published roster.

        Transitions PUBLISHED → DRAFT.

        Args:
            roster_id: Roster ID
            reason: Reason for recall

        Returns:
            True if successful, False otherwise
        """
        current_state = self.db.get_roster_state(roster_id)
        if current_state != RosterPublishState.PUBLISHED.value:
            logger.error(
                f"Cannot recall roster {roster_id}: "
                f"in {current_state} state (must be PUBLISHED)"
            )
            return False

        return self.transition_state(
            roster_id,
            RosterPublishState.DRAFT.value,
            f"Recalled: {reason}",
            actor_id="system",
        )

    def archive_roster(self, roster_id: str) -> bool:
        """
        Archive a published roster.

        Transitions PUBLISHED → ARCHIVED. Archived rosters are read-only
        and do not appear in active views.

        Args:
            roster_id: Roster ID

        Returns:
            True if successful, False otherwise
        """
        current_state = self.db.get_roster_state(roster_id)
        if current_state != RosterPublishState.PUBLISHED.value:
            logger.error(
                f"Cannot archive roster {roster_id}: "
                f"in {current_state} state (must be PUBLISHED)"
            )
            return False

        return self.transition_state(
            roster_id,
            RosterPublishState.ARCHIVED.value,
            "Archived",
            actor_id="system",
        )

    def get_publication_history(
        self,
        venue_id: str,
        limit: int = 20,
    ) -> List[PublicationEvent]:
        """
        Get publication history for a venue.

        Args:
            venue_id: Venue ID
            limit: Max events to return

        Returns:
            List of PublicationEvent objects, most recent first
        """
        events = self.db.get_publication_history(venue_id, limit=limit)
        return [
            PublicationEvent(
                roster_id=e.get("roster_id"),
                venue_id=e.get("venue_id"),
                state=e.get("state"),
                publisher_id=e.get("publisher_id"),
                timestamp=e.get("timestamp"),
                details=e.get("details", {}),
            )
            for e in events
        ]

    async def auto_publish_if_no_conflicts(self, roster_id: str) -> PublishResult:
        """
        Shortcut to auto-publish a roster if it has no conflicts.

        Useful for programmatically-generated rosters that pass all checks.

        Args:
            roster_id: Roster ID

        Returns:
            PublishResult with outcome
        """
        result = PublishResult(
            success=False,
            roster_id=roster_id,
            state=RosterPublishState.DRAFT.value,
            conflicts_found=0,
            critical_conflicts=0,
            timestamp=datetime.utcnow().isoformat(),
        )

        try:
            roster = self.db.get_roster(roster_id)
            if not roster:
                result.message = f"Roster {roster_id} not found"
                return result

            venue = self.db.get_venue(roster.venue_id)
            if not venue:
                result.message = f"Venue {roster.venue_id} not found"
                return result

            employees = [e for e in self.db.list_employees() if getattr(e, "venue_id", None) == roster.venue_id]

            # Check conflicts
            conflicts = self.conflict_detector.detect_conflicts(
                roster=roster,
                venue=venue,
                employees=employees,
            )
            result.conflicts_found = len(conflicts)
            result.critical_conflicts = len(
                [c for c in conflicts if c.severity == ConflictSeverity.CRITICAL]
            )

            if result.critical_conflicts > 0 or len(conflicts) > 0:
                result.message = (
                    f"Roster has {result.critical_conflicts} critical conflicts "
                    f"and {result.conflicts_found} total conflicts; cannot auto-publish"
                )
                return result

            # No conflicts - publish with admin override
            return await self.publish_roster(
                roster_id=roster_id,
                publisher_id="system",
                skip_approval=True,
            )

        except Exception as e:
            result.message = f"Auto-publish failed: {str(e)}"
            logger.error(result.message, exc_info=True)
            return result

    async def _notify_staff(
        self,
        roster: Roster,
        venue: VenueConfig,
        conflicts_count: int,
    ) -> int:
        """
        Notify all affected staff of roster publication.

        Args:
            roster: Roster object
            venue: Venue object
            conflicts_count: Number of conflicts (for context)

        Returns:
            Number of notifications sent
        """
        try:
            employees = set(s.employee_id for s in roster.shifts)
            notifications_sent = 0

            for employee_id in employees:
                try:
                    await self.notification_hub.dispatch(
                        event_type=NotificationEventType.ROSTER_PUBLISHED.value,
                        venue_id=venue.id,
                        payload={
                            "roster_id": roster.id,
                            "week_start": roster.week_start.isoformat(),
                            "week_end": roster.week_end.isoformat(),
                            "employee_id": employee_id,
                            "conflicts_warning": conflicts_count > 0,
                        },
                        employee_ids=[employee_id],
                    )
                    notifications_sent += 1
                except Exception as e:
                    logger.error(
                        f"Error notifying employee {employee_id}: {str(e)}",
                        exc_info=True,
                    )

            return notifications_sent

        except Exception as e:
            logger.error(f"Error notifying staff: {str(e)}", exc_info=True)
            return 0

    def _log_publication_event(
        self,
        roster_id: str,
        venue_id: str,
        state: str,
        publisher_id: str,
    ) -> None:
        """
        Log a publication event to audit trail.

        Args:
            roster_id: Roster ID
            venue_id: Venue ID
            state: State transitioned to
            publisher_id: User ID triggering the event
        """
        try:
            event = PublicationEvent(
                roster_id=roster_id,
                venue_id=venue_id,
                state=state,
                publisher_id=publisher_id,
                timestamp=datetime.utcnow().isoformat(),
                details={"event_type": "state_transition"},
            )
            self.db.save_publication_event(asdict(event))
        except Exception as e:
            logger.error(f"Error logging publication event: {str(e)}", exc_info=True)


# ============================================================================
# Singleton instance
# ============================================================================

publisher = RosterPublisher()
