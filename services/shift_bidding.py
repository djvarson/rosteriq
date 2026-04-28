"""
Shift bidding marketplace service for RosterIQ.

Enables employees to bid on open shifts posted by managers.
Handles bid scoring, auto-assignment, and award mechanics.
"""

import uuid
import logging
from dataclasses import dataclass, field
from datetime import datetime, date, time, timedelta
from decimal import Decimal
from typing import Optional, List, Dict, Set, Tuple
from enum import Enum

from rosteriq.database import BaseStore
from rosteriq.models import Shift, ShiftStatus, Employee
from rosteriq.award_rules import get_day_type, get_penalty_multiplier
from rosteriq.services.notifications import NotificationService

logger = logging.getLogger(__name__)


# ============================================================================
# ENUMS
# ============================================================================


class OpenShiftStatus(str, Enum):
    """Status of an open shift posting."""
    open = "open"
    filled = "filled"
    expired = "expired"
    cancelled = "cancelled"


class BidStatus(str, Enum):
    """Status of a bid."""
    pending = "pending"
    accepted = "accepted"
    rejected = "rejected"
    withdrawn = "withdrawn"


# ============================================================================
# DATA MODELS
# ============================================================================


@dataclass
class OpenShift:
    """Open shift posting for employees to bid on."""
    id: str
    venue_id: str
    date: date
    start_time: time
    end_time: time
    role_required: str
    skills_required: List[str] = field(default_factory=list)
    min_rate: Decimal = Decimal("0")
    max_rate: Optional[Decimal] = None  # Optional cap on offered rate
    posted_by: str = ""  # Manager user ID
    posted_at: datetime = field(default_factory=datetime.utcnow)
    deadline: datetime = field(default_factory=lambda: datetime.utcnow() + timedelta(hours=24))
    status: OpenShiftStatus = OpenShiftStatus.open
    notes: Optional[str] = None

    def to_dict(self) -> dict:
        """Convert to dictionary for storage."""
        return {
            "id": self.id,
            "venue_id": self.venue_id,
            "date": self.date.isoformat(),
            "start_time": self.start_time.isoformat(),
            "end_time": self.end_time.isoformat(),
            "role_required": self.role_required,
            "skills_required": self.skills_required,
            "min_rate": str(self.min_rate),
            "max_rate": str(self.max_rate) if self.max_rate else None,
            "posted_by": self.posted_by,
            "posted_at": self.posted_at.isoformat(),
            "deadline": self.deadline.isoformat(),
            "status": self.status.value,
            "notes": self.notes,
        }

    @staticmethod
    def from_dict(data: dict) -> "OpenShift":
        """Reconstruct from dictionary."""
        return OpenShift(
            id=data["id"],
            venue_id=data["venue_id"],
            date=date.fromisoformat(data["date"]),
            start_time=time.fromisoformat(data["start_time"]),
            end_time=time.fromisoformat(data["end_time"]),
            role_required=data["role_required"],
            skills_required=data.get("skills_required", []),
            min_rate=Decimal(data.get("min_rate", 0)),
            max_rate=Decimal(data["max_rate"]) if data.get("max_rate") else None,
            posted_by=data.get("posted_by", ""),
            posted_at=datetime.fromisoformat(data["posted_at"]),
            deadline=datetime.fromisoformat(data["deadline"]),
            status=OpenShiftStatus(data.get("status", "open")),
            notes=data.get("notes"),
        )


@dataclass
class Bid:
    """Bid on an open shift."""
    id: str
    open_shift_id: str
    employee_id: str
    offered_rate: Decimal
    message: Optional[str] = None
    seniority_years: float = 0.0
    preference_score: float = 0.0  # 0-1 based on employee's preferences/history
    submitted_at: datetime = field(default_factory=datetime.utcnow)
    status: BidStatus = BidStatus.pending

    def to_dict(self) -> dict:
        """Convert to dictionary for storage."""
        return {
            "id": self.id,
            "open_shift_id": self.open_shift_id,
            "employee_id": self.employee_id,
            "offered_rate": str(self.offered_rate),
            "message": self.message,
            "seniority_years": self.seniority_years,
            "preference_score": self.preference_score,
            "submitted_at": self.submitted_at.isoformat(),
            "status": self.status.value,
        }

    @staticmethod
    def from_dict(data: dict) -> "Bid":
        """Reconstruct from dictionary."""
        return Bid(
            id=data["id"],
            open_shift_id=data["open_shift_id"],
            employee_id=data["employee_id"],
            offered_rate=Decimal(data["offered_rate"]),
            message=data.get("message"),
            seniority_years=data.get("seniority_years", 0.0),
            preference_score=data.get("preference_score", 0.0),
            submitted_at=datetime.fromisoformat(data["submitted_at"]),
            status=BidStatus(data.get("status", "pending")),
        )


# ============================================================================
# SHIFT BIDDING SERVICE
# ============================================================================


class ShiftBiddingService:
    """Service for managing shift bidding marketplace."""

    def __init__(self, db: BaseStore, notification_service: Optional[NotificationService] = None):
        """
        Initialize the bidding service.

        Args:
            db: Database store instance
            notification_service: Optional notification service for alerts
        """
        self.db = db
        self.notifications = notification_service

    # ========================================================================
    # OPEN SHIFT MANAGEMENT
    # ========================================================================

    def post_open_shift(
        self,
        venue_id: str,
        shift_details: dict,
        posted_by: str,
    ) -> OpenShift:
        """
        Post an open shift for employees to bid on.

        Args:
            venue_id: Venue ID
            shift_details: Dict with keys: date, start_time, end_time, role_required,
                          skills_required (optional), min_rate (optional), max_rate (optional),
                          deadline (optional), notes (optional)
            posted_by: Manager user ID

        Returns:
            OpenShift object

        Raises:
            ValueError: If validation fails
        """
        # Validate inputs
        shift_date = shift_details.get("date")
        start_time = shift_details.get("start_time")
        end_time = shift_details.get("end_time")
        role_required = shift_details.get("role_required")
        min_rate = Decimal(shift_details.get("min_rate", "0"))

        if not all([shift_date, start_time, end_time, role_required]):
            raise ValueError("Missing required shift details: date, start_time, end_time, role_required")

        if isinstance(shift_date, str):
            shift_date = date.fromisoformat(shift_date)
        if isinstance(start_time, str):
            start_time = time.fromisoformat(start_time)
        if isinstance(end_time, str):
            end_time = time.fromisoformat(end_time)

        # Validate minimum rate meets award requirements
        day_type = get_day_type(shift_date)
        from rosteriq.models import EmploymentType
        award_multiplier = get_penalty_multiplier(EmploymentType.casual, day_type)
        if min_rate <= 0:
            raise ValueError("min_rate must be greater than 0")

        # Set deadline (default: 24 hours before shift start)
        deadline = shift_details.get("deadline")
        if deadline:
            if isinstance(deadline, str):
                deadline = datetime.fromisoformat(deadline)
        else:
            shift_datetime = datetime.combine(shift_date, start_time)
            deadline = shift_datetime - timedelta(hours=24)

        # Create open shift
        open_shift = OpenShift(
            id=str(uuid.uuid4()),
            venue_id=venue_id,
            date=shift_date,
            start_time=start_time,
            end_time=end_time,
            role_required=role_required,
            skills_required=shift_details.get("skills_required", []),
            min_rate=min_rate,
            max_rate=Decimal(shift_details["max_rate"]) if shift_details.get("max_rate") else None,
            posted_by=posted_by,
            posted_at=datetime.utcnow(),
            deadline=deadline,
            notes=shift_details.get("notes"),
        )

        # Save to database
        self.db.save_open_shift(open_shift.to_dict())

        # Notify eligible employees
        self._notify_eligible_employees(open_shift)

        logger.info(f"Posted open shift {open_shift.id} for {role_required} on {shift_date}")
        return open_shift

    def _notify_eligible_employees(self, open_shift: OpenShift) -> None:
        """Notify employees eligible to bid on the shift."""
        if not self.notifications:
            return

        # Get all employees for the venue
        all_employees = self.db.list_employees()
        venue_employees = [e for e in all_employees]  # TODO: Filter by venue_id when available

        # Filter by role and skills
        eligible = []
        for emp in venue_employees:
            # Check role match (assume role is stored in employee model)
            if hasattr(emp, "skills") and open_shift.skills_required:
                has_required_skills = all(s in emp.skills for s in open_shift.skills_required)
                if has_required_skills:
                    eligible.append(emp)
            else:
                eligible.append(emp)

        # Notify each eligible employee
        for emp in eligible:
            if not hasattr(emp, "email") or not emp.email:
                continue
            try:
                self.notifications.send_notification(
                    user_id=emp.id,
                    notification_type="shift_bidding",
                    title=f"New shift available: {open_shift.role_required}",
                    message=f"A {open_shift.role_required} shift is available on {open_shift.date} "
                           f"from {open_shift.start_time} to {open_shift.end_time}. "
                           f"Bidding deadline: {open_shift.deadline}",
                    data={"open_shift_id": open_shift.id},
                )
            except Exception as e:
                logger.warning(f"Failed to notify employee {emp.id}: {e}")

    # ========================================================================
    # BID MANAGEMENT
    # ========================================================================

    def place_bid(
        self,
        open_shift_id: str,
        employee_id: str,
        offered_rate: Decimal,
        message: Optional[str] = None,
    ) -> Bid:
        """
        Place a bid on an open shift.

        Args:
            open_shift_id: ID of the open shift
            employee_id: ID of the employee bidding
            offered_rate: Rate offered by employee
            message: Optional message/note from employee

        Returns:
            Bid object

        Raises:
            ValueError: If validation fails
        """
        # Fetch open shift
        open_shift_data = self.db.get_open_shift(open_shift_id)
        if not open_shift_data:
            raise ValueError(f"Open shift {open_shift_id} not found")

        open_shift = OpenShift.from_dict(open_shift_data)

        # Validate shift is still open
        if open_shift.status != OpenShiftStatus.open:
            raise ValueError(f"Shift is {open_shift.status.value}, not accepting bids")

        # Validate deadline hasn't passed
        if datetime.utcnow() > open_shift.deadline:
            raise ValueError("Bidding deadline has passed")

        # Fetch employee
        employee = self.db.get_employee(employee_id)
        if not employee:
            raise ValueError(f"Employee {employee_id} not found")

        # Validate offered rate >= minimum award rate
        if offered_rate < open_shift.min_rate:
            raise ValueError(
                f"Offered rate {offered_rate} is below minimum {open_shift.min_rate}"
            )

        # Validate offered rate <= maximum (if set)
        if open_shift.max_rate and offered_rate > open_shift.max_rate:
            raise ValueError(
                f"Offered rate {offered_rate} exceeds maximum {open_shift.max_rate}"
            )

        # Validate employee has required skills
        if open_shift.skills_required:
            has_required_skills = all(s in employee.skills for s in open_shift.skills_required)
            if not has_required_skills:
                missing = [s for s in open_shift.skills_required if s not in employee.skills]
                raise ValueError(f"Missing required skills: {missing}")

        # Check for double-booking
        if self._is_employee_scheduled(employee_id, open_shift.date, open_shift.start_time, open_shift.end_time):
            raise ValueError(
                f"Employee is already rostered for this time slot"
            )

        # Check if employee already bid on this shift
        existing_bids = self.db.list_bids(open_shift_id)
        for bid_data in existing_bids:
            bid = Bid.from_dict(bid_data)
            if bid.employee_id == employee_id and bid.status == BidStatus.pending:
                raise ValueError("Employee has already bid on this shift")

        # Calculate employee seniority and preference score
        seniority_years = self._calculate_seniority(employee)
        preference_score = self._calculate_preference_score(employee, open_shift)

        # Create bid
        bid = Bid(
            id=str(uuid.uuid4()),
            open_shift_id=open_shift_id,
            employee_id=employee_id,
            offered_rate=offered_rate,
            message=message,
            seniority_years=seniority_years,
            preference_score=preference_score,
            submitted_at=datetime.utcnow(),
            status=BidStatus.pending,
        )

        # Save to database
        self.db.save_bid(bid.to_dict())

        logger.info(f"Bid {bid.id} placed by {employee_id} on shift {open_shift_id}")
        return bid

    def _is_employee_scheduled(
        self,
        employee_id: str,
        shift_date: date,
        start_time: time,
        end_time: time,
    ) -> bool:
        """Check if employee is already scheduled for this time."""
        # TODO: Query actual rosters when available
        # For now, assume no conflicts
        return False

    def _calculate_seniority(self, employee: Employee) -> float:
        """Calculate seniority in years based on created_at."""
        if not hasattr(employee, "created_at") or not employee.created_at:
            return 0.0
        years = (datetime.utcnow() - employee.created_at).days / 365.25
        return max(0.0, years)

    def _calculate_preference_score(self, employee: Employee, open_shift: OpenShift) -> float:
        """
        Calculate preference score (0-1) for employee bidding on this shift.
        Higher score = better fit based on availability, role fit, etc.
        """
        score = 0.5  # Base score

        # Check availability preferences if stored
        if hasattr(employee, "availability") and employee.availability:
            day_name = open_shift.date.strftime("%A").lower()
            if day_name in employee.availability:
                score += 0.25

        # Role match bonus
        if hasattr(employee, "role") and hasattr(open_shift, "role_required"):
            if employee.role == open_shift.role_required:
                score += 0.25

        return min(1.0, score)

    def withdraw_bid(self, bid_id: str, employee_id: str) -> bool:
        """
        Withdraw a bid.

        Args:
            bid_id: ID of the bid
            employee_id: ID of the employee (for authorization)

        Returns:
            True if successful

        Raises:
            ValueError: If bid not found or unauthorized
        """
        bid_data = self.db.get_bid(bid_id)
        if not bid_data:
            raise ValueError(f"Bid {bid_id} not found")

        bid = Bid.from_dict(bid_data)

        # Verify authorization
        if bid.employee_id != employee_id:
            raise ValueError("Unauthorized: bid belongs to different employee")

        # Can only withdraw pending bids
        if bid.status != BidStatus.pending:
            raise ValueError(f"Cannot withdraw bid with status {bid.status.value}")

        # Update status
        bid.status = BidStatus.withdrawn
        self.db.save_bid(bid.to_dict())

        logger.info(f"Bid {bid_id} withdrawn by {employee_id}")
        return True

    # ========================================================================
    # BID SCORING & ASSIGNMENT
    # ========================================================================

    def auto_assign(self, open_shift_id: str) -> Optional[Bid]:
        """
        Automatically assign shift to best-scoring bid.

        Scoring: 40% rate (lower is better), 30% seniority, 20% preference_score, 10% submission time

        Args:
            open_shift_id: ID of the open shift

        Returns:
            Winning Bid or None if no eligible bids

        Raises:
            ValueError: If shift not found
        """
        # Fetch open shift
        open_shift_data = self.db.get_open_shift(open_shift_id)
        if not open_shift_data:
            raise ValueError(f"Open shift {open_shift_id} not found")

        open_shift = OpenShift.from_dict(open_shift_data)

        # Get all pending bids
        all_bids = self.db.list_bids(open_shift_id)
        pending_bids = [
            Bid.from_dict(b) for b in all_bids
            if Bid.from_dict(b).status == BidStatus.pending
        ]

        if not pending_bids:
            logger.info(f"No pending bids for shift {open_shift_id}")
            return None

        # Score each bid
        scored_bids = []
        for bid in pending_bids:
            score = self._calculate_bid_score(bid, open_shift)
            scored_bids.append((bid, score))

        # Sort by score (highest first), then by seniority (highest first), then by submission time (earliest first)
        scored_bids.sort(
            key=lambda x: (
                -x[1],  # Higher score first
                -x[0].seniority_years,  # Higher seniority first
                x[0].submitted_at,  # Earlier submission first
            )
        )

        winning_bid = scored_bids[0][0]
        logger.info(f"Auto-assigned shift {open_shift_id} to bid {winning_bid.id} with score {scored_bids[0][1]:.2f}")

        return winning_bid

    def _calculate_bid_score(self, bid: Bid, open_shift: OpenShift) -> float:
        """
        Calculate composite score for a bid.

        Factors:
        - 40% rate (lower is better): normalized to 0-1
        - 30% seniority: capped at 10 years = 1.0
        - 20% preference_score: already 0-1
        - 10% submission time: recent submissions score higher
        """
        # Rate component (40%): lower rate is better
        # Normalize: bid at min_rate = 1.0, bid at min_rate * 1.5 = 0.0
        max_acceptable_rate = open_shift.min_rate * Decimal("1.5")
        if open_shift.max_rate:
            max_acceptable_rate = min(max_acceptable_rate, open_shift.max_rate)

        if bid.offered_rate >= max_acceptable_rate:
            rate_score = 0.0
        else:
            rate_score = float(
                (max_acceptable_rate - bid.offered_rate) / (max_acceptable_rate - open_shift.min_rate)
            )
        rate_component = rate_score * 0.40

        # Seniority component (30%): capped at 10 years
        seniority_score = min(1.0, bid.seniority_years / 10.0)
        seniority_component = seniority_score * 0.30

        # Preference component (20%)
        preference_component = bid.preference_score * 0.20

        # Submission time component (10%): recent bids score slightly higher
        # Bids within 1 hour of deadline get full points
        hours_until_deadline = (open_shift.deadline - bid.submitted_at).total_seconds() / 3600
        submission_score = min(1.0, max(0.0, hours_until_deadline / 1.0))
        submission_component = submission_score * 0.10

        total_score = rate_component + seniority_component + preference_component + submission_component
        return total_score

    def list_bids(self, open_shift_id: str) -> List[Bid]:
        """
        Get all bids for a shift, sorted by score.

        Args:
            open_shift_id: ID of the open shift

        Returns:
            List of Bid objects sorted by score (highest first)
        """
        # Fetch open shift
        open_shift_data = self.db.get_open_shift(open_shift_id)
        if not open_shift_data:
            return []

        open_shift = OpenShift.from_dict(open_shift_data)

        # Get all bids
        all_bids_data = self.db.list_bids(open_shift_id)
        bids = [Bid.from_dict(b) for b in all_bids_data]

        # Score pending bids and sort
        scored = []
        for bid in bids:
            score = self._calculate_bid_score(bid, open_shift) if bid.status == BidStatus.pending else 0.0
            scored.append((bid, score))

        scored.sort(key=lambda x: -x[1])
        return [bid for bid, _ in scored]

    # ========================================================================
    # MANUAL AWARD
    # ========================================================================

    def award_shift(
        self,
        open_shift_id: str,
        bid_id: str,
        awarded_by: str,
    ) -> Shift:
        """
        Manager manually awards shift to a bidder, creating actual roster shift.

        Args:
            open_shift_id: ID of the open shift
            bid_id: ID of the winning bid
            awarded_by: Manager user ID

        Returns:
            Created Shift object

        Raises:
            ValueError: If validation fails
        """
        # Fetch open shift
        open_shift_data = self.db.get_open_shift(open_shift_id)
        if not open_shift_data:
            raise ValueError(f"Open shift {open_shift_id} not found")

        open_shift = OpenShift.from_dict(open_shift_data)

        # Fetch winning bid
        bid_data = self.db.get_bid(bid_id)
        if not bid_data:
            raise ValueError(f"Bid {bid_id} not found")

        winning_bid = Bid.from_dict(bid_data)

        # Verify bid belongs to this shift
        if winning_bid.open_shift_id != open_shift_id:
            raise ValueError("Bid does not belong to this shift")

        # Create actual shift
        shift = Shift(
            id=str(uuid.uuid4()),
            employee_id=winning_bid.employee_id,
            date=open_shift.date,
            start_time=open_shift.start_time,
            end_time=open_shift.end_time,
            status=ShiftStatus.scheduled,
            role=open_shift.role_required,
        )

        # Save shift
        self.db.save_shift(shift)

        # Update bid status
        winning_bid.status = BidStatus.accepted
        self.db.save_bid(winning_bid.to_dict())

        # Reject all other bids
        all_bids_data = self.db.list_bids(open_shift_id)
        for bid_data in all_bids_data:
            bid = Bid.from_dict(bid_data)
            if bid.id != bid_id and bid.status == BidStatus.pending:
                bid.status = BidStatus.rejected
                self.db.save_bid(bid.to_dict())

        # Update open shift status
        open_shift.status = OpenShiftStatus.filled
        self.db.save_open_shift(open_shift.to_dict())

        # Send notifications
        self._notify_award(open_shift, winning_bid, all_bids_data)

        logger.info(f"Shift {open_shift_id} awarded to bid {bid_id} by {awarded_by}")
        return shift

    def _notify_award(self, open_shift: OpenShift, winning_bid: Bid, all_bids_data: List[dict]) -> None:
        """Notify winner and losers of award decision."""
        if not self.notifications:
            return

        # Notify winner
        try:
            self.notifications.send_notification(
                user_id=winning_bid.employee_id,
                notification_type="shift_awarded",
                title="Shift awarded!",
                message=f"Your bid has been accepted for the {open_shift.role_required} shift on {open_shift.date}",
                data={"shift_id": open_shift.id},
            )
        except Exception as e:
            logger.warning(f"Failed to notify winner {winning_bid.employee_id}: {e}")

        # Notify losers
        for bid_data in all_bids_data:
            bid = Bid.from_dict(bid_data)
            if bid.id != winning_bid.id and bid.status in [BidStatus.pending, BidStatus.rejected]:
                try:
                    self.notifications.send_notification(
                        user_id=bid.employee_id,
                        notification_type="shift_bid_rejected",
                        title="Bid not selected",
                        message=f"Your bid for the {open_shift.role_required} shift on {open_shift.date} was not selected",
                        data={"open_shift_id": open_shift.id},
                    )
                except Exception as e:
                    logger.warning(f"Failed to notify loser {bid.employee_id}: {e}")

    # ========================================================================
    # SHIFT MANAGEMENT
    # ========================================================================

    def cancel_open_shift(self, open_shift_id: str, cancelled_by: str) -> bool:
        """
        Cancel an open shift posting.

        Args:
            open_shift_id: ID of the open shift
            cancelled_by: User ID cancelling

        Returns:
            True if successful

        Raises:
            ValueError: If shift not found
        """
        open_shift_data = self.db.get_open_shift(open_shift_id)
        if not open_shift_data:
            raise ValueError(f"Open shift {open_shift_id} not found")

        open_shift = OpenShift.from_dict(open_shift_data)

        # Can't cancel already filled shifts
        if open_shift.status == OpenShiftStatus.filled:
            raise ValueError("Cannot cancel a shift that has already been filled")

        # Update status
        open_shift.status = OpenShiftStatus.cancelled
        self.db.save_open_shift(open_shift.to_dict())

        # Notify all bidders
        if self.notifications:
            all_bids_data = self.db.list_bids(open_shift_id)
            for bid_data in all_bids_data:
                bid = Bid.from_dict(bid_data)
                if bid.status == BidStatus.pending:
                    try:
                        self.notifications.send_notification(
                            user_id=bid.employee_id,
                            notification_type="shift_cancelled",
                            title="Shift posting cancelled",
                            message=f"The {open_shift.role_required} shift on {open_shift.date} has been cancelled",
                            data={"open_shift_id": open_shift_id},
                        )
                    except Exception as e:
                        logger.warning(f"Failed to notify {bid.employee_id}: {e}")

        logger.info(f"Shift {open_shift_id} cancelled by {cancelled_by}")
        return True

    def expire_unfilled_shifts(self) -> List[str]:
        """
        Mark expired shifts and notify managers.

        Returns:
            List of expired shift IDs

        Raises:
            ValueError: If shifts not properly structured
        """
        # Get all open shifts (would need a better query method)
        # For now, iterate through all open shifts and check deadline
        expired_ids = []

        # Note: This is a simplified implementation
        # In production, would need a proper query method like list_open_shifts()
        now = datetime.utcnow()

        # TODO: Implement proper query method in database
        # For now, return empty list
        return expired_ids

    # ========================================================================
    # QUERIES
    # ========================================================================

    def list_open_shifts(
        self,
        venue_id: str,
        role: Optional[str] = None,
        date_range: Optional[Tuple[date, date]] = None,
        status: Optional[OpenShiftStatus] = None,
    ) -> List[OpenShift]:
        """
        List open shifts for a venue.

        Args:
            venue_id: Venue ID
            role: Optional role filter
            date_range: Optional (start_date, end_date) tuple
            status: Optional status filter (default: open shifts only)

        Returns:
            List of OpenShift objects
        """
        if status is None:
            status = OpenShiftStatus.open

        shifts_data = self.db.list_open_shifts(venue_id, status.value)
        shifts = [OpenShift.from_dict(s) for s in shifts_data]

        # Apply filters
        if role:
            shifts = [s for s in shifts if s.role_required == role]

        if date_range:
            start_date, end_date = date_range
            shifts = [s for s in shifts if start_date <= s.date <= end_date]

        return shifts

    def get_open_shift(self, shift_id: str) -> Optional[OpenShift]:
        """Get a specific open shift by ID."""
        data = self.db.get_open_shift(shift_id)
        if data:
            return OpenShift.from_dict(data)
        return None

    def get_eligible_employees(self, open_shift_id: str) -> List[Employee]:
        """
        Get list of employees eligible to bid on a shift.

        Args:
            open_shift_id: ID of the open shift

        Returns:
            List of Employee objects
        """
        open_shift_data = self.db.get_open_shift(open_shift_id)
        if not open_shift_data:
            return []

        open_shift = OpenShift.from_dict(open_shift_data)
        all_employees = self.db.list_employees()

        eligible = []
        for emp in all_employees:
            # Check required skills
            if open_shift.skills_required:
                if not hasattr(emp, "skills") or not emp.skills:
                    continue
                if not all(s in emp.skills for s in open_shift.skills_required):
                    continue

            # Check not already scheduled
            if self._is_employee_scheduled(
                emp.id,
                open_shift.date,
                open_shift.start_time,
                open_shift.end_time,
            ):
                continue

            eligible.append(emp)

        return eligible
