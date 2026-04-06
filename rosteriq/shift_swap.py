"""
Shift Swap and Staff Notification System for RosterIQ.

Handles staff requesting shift swaps, manager approvals, and notifications across
multiple channels (email, SMS, push, in-app).
"""

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Optional, List, Dict, Tuple, Set
from abc import ABC, abstractmethod
import uuid
from fastapi import APIRouter, HTTPException, Depends, Query
from pydantic import BaseModel


# ============================================================================
# Enums
# ============================================================================

class SwapStatus(str, Enum):
    """Status of a swap request."""
    PENDING = "PENDING"
    APPROVED = "APPROVED"
    REJECTED = "REJECTED"
    EXPIRED = "EXPIRED"
    CANCELLED = "CANCELLED"


class NotificationType(str, Enum):
    """Types of notifications that can be sent."""
    ROSTER_PUBLISHED = "ROSTER_PUBLISHED"
    SHIFT_ASSIGNED = "SHIFT_ASSIGNED"
    SHIFT_CHANGED = "SHIFT_CHANGED"
    SWAP_REQUESTED = "SWAP_REQUESTED"
    SWAP_APPROVED = "SWAP_APPROVED"
    SWAP_REJECTED = "SWAP_REJECTED"
    SHIFT_REMINDER = "SHIFT_REMINDER"
    AVAILABILITY_REQUEST = "AVAILABILITY_REQUEST"


class NotificationChannel(str, Enum):
    """Notification delivery channels."""
    EMAIL = "EMAIL"
    SMS = "SMS"
    PUSH = "PUSH"
    IN_APP = "IN_APP"


# ============================================================================
# Data Models
# ============================================================================

@dataclass
class SwapRequest:
    """Represents a shift swap request between employees."""
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    requester_id: str = ""
    requester_name: str = ""
    original_shift_id: str = ""
    original_date: str = ""  # ISO format: YYYY-MM-DD
    original_start: str = ""  # HH:MM
    original_end: str = ""    # HH:MM
    target_shift_id: Optional[str] = None  # None for open swap requests
    target_employee_id: Optional[str] = None  # None for open swaps
    target_employee_name: Optional[str] = None
    reason: str = ""
    status: SwapStatus = SwapStatus.PENDING
    created_at: datetime = field(default_factory=datetime.utcnow)
    resolved_at: Optional[datetime] = None
    resolved_by: Optional[str] = None
    auto_approved: bool = False

    def to_dict(self) -> Dict:
        """Convert to dictionary for API responses."""
        return {
            "id": self.id,
            "requester_id": self.requester_id,
            "requester_name": self.requester_name,
            "original_shift_id": self.original_shift_id,
            "original_date": self.original_date,
            "original_start": self.original_start,
            "original_end": self.original_end,
            "target_shift_id": self.target_shift_id,
            "target_employee_id": self.target_employee_id,
            "target_employee_name": self.target_employee_name,
            "reason": self.reason,
            "status": self.status.value,
            "created_at": self.created_at.isoformat(),
            "resolved_at": self.resolved_at.isoformat() if self.resolved_at else None,
            "resolved_by": self.resolved_by,
            "auto_approved": self.auto_approved,
        }


@dataclass
class Notification:
    """Represents a notification sent to an employee."""
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    employee_id: str = ""
    type: NotificationType = NotificationType.SHIFT_ASSIGNED
    title: str = ""
    message: str = ""
    metadata: Dict = field(default_factory=dict)
    created_at: datetime = field(default_factory=datetime.utcnow)
    read_at: Optional[datetime] = None
    sent_via: NotificationChannel = NotificationChannel.IN_APP
    is_read: bool = False

    def to_dict(self) -> Dict:
        """Convert to dictionary for API responses."""
        return {
            "id": self.id,
            "employee_id": self.employee_id,
            "type": self.type.value,
            "title": self.title,
            "message": self.message,
            "metadata": self.metadata,
            "created_at": self.created_at.isoformat(),
            "read_at": self.read_at.isoformat() if self.read_at else None,
            "sent_via": self.sent_via.value,
            "is_read": self.is_read,
        }


@dataclass
class SwapRule:
    """Configuration rules for shift swaps."""
    allow_open_swaps: bool = True
    require_manager_approval: bool = True
    auto_approve_same_role: bool = True
    auto_approve_same_cost: bool = False
    max_swaps_per_week: int = 3
    min_notice_hours: int = 24
    blackout_dates: List[str] = field(default_factory=list)  # ISO format dates


@dataclass
class Shift:
    """Represents a work shift."""
    id: str = ""
    employee_id: Optional[str] = None
    date: str = ""  # YYYY-MM-DD
    start_time: str = ""  # HH:MM
    end_time: str = ""  # HH:MM
    role: str = ""
    cost: float = 0.0
    required: bool = True


@dataclass
class Employee:
    """Represents an employee."""
    id: str = ""
    name: str = ""
    roles: Set[str] = field(default_factory=set)
    hourly_rate: float = 0.0
    unavailable_dates: List[str] = field(default_factory=list)
    active: bool = True


# ============================================================================
# Pydantic Models for API
# ============================================================================

class SwapRequestCreate(BaseModel):
    """Request payload to create a swap request."""
    original_shift_id: str
    target_shift_id: Optional[str] = None
    target_employee_id: Optional[str] = None
    reason: str = ""


class SwapApprovalRequest(BaseModel):
    """Request payload for swap approval."""
    approver_id: str


class SwapRejectionRequest(BaseModel):
    """Request payload for swap rejection."""
    approver_id: str
    reason: str = ""


class ShiftClaimRequest(BaseModel):
    """Request payload to claim an open shift."""
    employee_id: str


# ============================================================================
# SwapManager Class
# ============================================================================

class SwapManager:
    """
    Manages shift swap requests, eligibility checks, and auto-approval logic.

    Handles:
    - Creating swap requests
    - Evaluating swap eligibility
    - Approving/rejecting swaps
    - Auto-processing swaps based on rules
    - Managing open shift pickups
    """

    def __init__(self, rules: SwapRule):
        """
        Initialize SwapManager with swap rules.

        Args:
            rules: SwapRule configuration object
        """
        self.rules = rules
        self.swaps: Dict[str, SwapRequest] = {}
        self.employees: Dict[str, Employee] = {}
        self.shifts: Dict[str, Shift] = {}

    def register_employee(self, employee: Employee) -> None:
        """Register an employee in the swap system."""
        self.employees[employee.id] = employee

    def register_shift(self, shift: Shift) -> None:
        """Register a shift in the swap system."""
        self.shifts[shift.id] = shift

    def create_swap_request(
        self,
        requester: Employee,
        original_shift: Shift,
        target_shift: Optional[Shift] = None,
        target_employee: Optional[Employee] = None,
        reason: str = "",
    ) -> SwapRequest:
        """
        Create a new shift swap request.

        Args:
            requester: Employee requesting the swap
            original_shift: The shift to be swapped out
            target_shift: Target shift (None for open swap)
            target_employee: Employee who would take original shift (None for open)
            reason: Reason for the swap request

        Returns:
            SwapRequest: The created swap request

        Raises:
            ValueError: If swap violates business rules
        """
        # Validate requester has the original shift
        if original_shift.employee_id != requester.id:
            raise ValueError("Requester must own the original shift")

        # Check minimum notice
        shift_datetime = datetime.fromisoformat(
            f"{original_shift.date}T{original_shift.start_time}"
        )
        if (shift_datetime - datetime.utcnow()).total_seconds() < (
            self.rules.min_notice_hours * 3600
        ):
            raise ValueError(
                f"Swap request requires at least {self.rules.min_notice_hours} hours notice"
            )

        # Check blackout dates
        if original_shift.date in self.rules.blackout_dates:
            raise ValueError("Shift date is in blackout period")

        request = SwapRequest(
            requester_id=requester.id,
            requester_name=requester.name,
            original_shift_id=original_shift.id,
            original_date=original_shift.date,
            original_start=original_shift.start_time,
            original_end=original_shift.end_time,
            target_shift_id=target_shift.id if target_shift else None,
            target_employee_id=target_employee.id if target_employee else None,
            target_employee_name=target_employee.name if target_employee else None,
            reason=reason,
        )

        self.swaps[request.id] = request
        return request

    def get_eligible_swaps(
        self, shift: Shift, available_employees: List[Employee]
    ) -> List[Employee]:
        """
        Find employees eligible to take a specific shift.

        Checks:
        - Required role/skills match
        - Not already assigned to shift
        - Available on that date
        - Not exceeding weekly swap limits

        Args:
            shift: The shift to fill
            available_employees: Pool of employees to check

        Returns:
            List[Employee]: Employees who can take the shift
        """
        eligible = []

        for emp in available_employees:
            # Check role match
            if shift.role not in emp.roles:
                continue

            # Check not already assigned
            if shift.employee_id == emp.id:
                continue

            # Check availability
            if shift.date in emp.unavailable_dates:
                continue

            # Check weekly swap limit
            swaps_this_week = self._count_swaps_this_week(emp.id)
            if swaps_this_week >= self.rules.max_swaps_per_week:
                continue

            # Check active status
            if not emp.active:
                continue

            eligible.append(emp)

        return eligible

    def evaluate_swap(self, request: SwapRequest) -> Dict:
        """
        Evaluate a swap request and return recommendation.

        Checks:
        - Role and skill compatibility
        - Cost impact
        - Fairness (workload balance)
        - Constraint violations

        Args:
            request: SwapRequest to evaluate

        Returns:
            Dict with keys: eligible (bool), recommendation (str), score (float),
                           reasons (List[str])
        """
        reasons = []
        score = 0.0
        max_score = 100.0

        original_shift = self.shifts.get(request.original_shift_id)
        target_shift = self.shifts.get(request.target_shift_id) if request.target_shift_id else None

        if not original_shift:
            return {
                "eligible": False,
                "recommendation": "REJECT",
                "score": 0.0,
                "reasons": ["Original shift not found"],
            }

        requester = self.employees.get(request.requester_id)
        target_employee = (
            self.employees.get(request.target_employee_id)
            if request.target_employee_id
            else None
        )

        # If not a direct swap, mark as open swap
        if not target_shift and not target_employee:
            return {
                "eligible": True,
                "recommendation": "OPEN_SWAP",
                "score": 50.0,
                "reasons": ["Open swap request - awaiting eligible volunteers"],
            }

        if target_shift and target_employee:
            # Check role compatibility
            if original_shift.role not in target_employee.roles:
                reasons.append("Target employee lacks required role")
            else:
                score += 30.0
                reasons.append("Role requirements met")

            # Check cost impact
            cost_diff = abs(original_shift.cost - target_shift.cost)
            if cost_diff < 5:
                score += 25.0
                reasons.append("Similar cost impact")
            else:
                reasons.append(f"Cost difference: ${cost_diff:.2f}")

            # Check availability
            if target_shift.date in target_employee.unavailable_dates:
                reasons.append("Target employee unavailable on date")
            else:
                score += 20.0
                reasons.append("Target employee available")

            # Auto-approval checks
            if self.rules.auto_approve_same_role and original_shift.role == target_shift.role:
                score += 15.0
                reasons.append("Auto-approval: same role")

            if self.rules.auto_approve_same_cost and original_shift.cost == target_shift.cost:
                score += 10.0
                reasons.append("Auto-approval: same cost")

        eligible = score >= 50.0
        recommendation = "APPROVE" if score >= 75.0 else ("CONDITIONAL" if eligible else "REJECT")

        return {
            "eligible": eligible,
            "recommendation": recommendation,
            "score": min(score, max_score),
            "reasons": reasons,
        }

    def approve_swap(self, request_id: str, approver_id: str) -> SwapRequest:
        """
        Approve a swap request and update shifts.

        Args:
            request_id: ID of swap request to approve
            approver_id: ID of manager approving

        Returns:
            SwapRequest: Updated swap request

        Raises:
            ValueError: If swap is invalid
        """
        request = self.swaps.get(request_id)
        if not request:
            raise ValueError(f"Swap request {request_id} not found")

        if request.status != SwapStatus.PENDING:
            raise ValueError(f"Cannot approve swap with status {request.status}")

        original_shift = self.shifts.get(request.original_shift_id)
        target_shift = self.shifts.get(request.target_shift_id)

        if not original_shift or not target_shift:
            raise ValueError("Required shifts not found")

        # Execute the swap
        original_shift.employee_id = request.target_employee_id
        target_shift.employee_id = request.requester_id

        request.status = SwapStatus.APPROVED
        request.resolved_at = datetime.utcnow()
        request.resolved_by = approver_id

        return request

    def reject_swap(self, request_id: str, approver_id: str, reason: str = "") -> SwapRequest:
        """
        Reject a swap request.

        Args:
            request_id: ID of swap request to reject
            approver_id: ID of manager rejecting
            reason: Reason for rejection

        Returns:
            SwapRequest: Updated swap request

        Raises:
            ValueError: If swap is invalid
        """
        request = self.swaps.get(request_id)
        if not request:
            raise ValueError(f"Swap request {request_id} not found")

        if request.status != SwapStatus.PENDING:
            raise ValueError(f"Cannot reject swap with status {request.status}")

        request.status = SwapStatus.REJECTED
        request.resolved_at = datetime.utcnow()
        request.resolved_by = approver_id
        request.reason = reason if reason else request.reason

        return request

    def cancel_swap(self, request_id: str) -> SwapRequest:
        """
        Cancel a swap request (only by requester).

        Args:
            request_id: ID of swap request to cancel

        Returns:
            SwapRequest: Updated swap request

        Raises:
            ValueError: If swap is invalid
        """
        request = self.swaps.get(request_id)
        if not request:
            raise ValueError(f"Swap request {request_id} not found")

        if request.status not in [SwapStatus.PENDING, SwapStatus.APPROVED]:
            raise ValueError(f"Cannot cancel swap with status {request.status}")

        request.status = SwapStatus.CANCELLED
        request.resolved_at = datetime.utcnow()

        return request

    def auto_process_swaps(self, pending_requests: List[SwapRequest]) -> List[SwapRequest]:
        """
        Auto-approve or auto-reject swaps based on configured rules.

        Args:
            pending_requests: List of pending swap requests

        Returns:
            List[SwapRequest]: Processed requests
        """
        processed = []

        for request in pending_requests:
            if request.status != SwapStatus.PENDING:
                continue

            evaluation = self.evaluate_swap(request)

            if evaluation["recommendation"] == "APPROVE":
                try:
                    self.approve_swap(request.id, "AUTO_SYSTEM")
                    request.auto_approved = True
                    processed.append(request)
                except ValueError:
                    continue

            elif evaluation["recommendation"] == "REJECT":
                try:
                    reason = "; ".join(evaluation["reasons"])
                    self.reject_swap(request.id, "AUTO_SYSTEM", reason)
                    processed.append(request)
                except ValueError:
                    continue

        return processed

    def get_open_shifts(self, roster: List[Shift]) -> List[Shift]:
        """
        Get shifts available for pickup (unfilled or open swaps).

        Args:
            roster: List of shifts in roster

        Returns:
            List[Shift]: Open shifts available for claiming
        """
        return [shift for shift in roster if shift.employee_id is None and shift.required]

    def claim_open_shift(self, employee: Employee, shift: Shift) -> SwapRequest:
        """
        Employee claims an unfilled shift.

        Args:
            employee: Employee claiming shift
            shift: Shift to claim

        Returns:
            SwapRequest: Created claim request

        Raises:
            ValueError: If claim is invalid
        """
        if shift.employee_id is not None:
            raise ValueError("Shift is already assigned")

        if shift.role not in employee.roles:
            raise ValueError("Employee lacks required role")

        if shift.date in employee.unavailable_dates:
            raise ValueError("Employee unavailable on that date")

        # Create a pseudo-swap request for tracking
        request = SwapRequest(
            requester_id=employee.id,
            requester_name=employee.name,
            original_shift_id=shift.id,
            original_date=shift.date,
            original_start=shift.start_time,
            original_end=shift.end_time,
            target_shift_id=shift.id,
            target_employee_id=employee.id,
            target_employee_name=employee.name,
            reason="Open shift claim",
            status=SwapStatus.APPROVED,
            auto_approved=True,
        )

        self.swaps[request.id] = request
        shift.employee_id = employee.id

        return request

    def expire_stale_requests(self, max_age_hours: int = 48) -> int:
        """
        Expire pending requests older than max_age_hours.

        Args:
            max_age_hours: Maximum age in hours before expiry

        Returns:
            int: Number of requests expired
        """
        cutoff_time = datetime.utcnow() - timedelta(hours=max_age_hours)
        expired_count = 0

        for request in self.swaps.values():
            if (
                request.status == SwapStatus.PENDING
                and request.created_at < cutoff_time
            ):
                request.status = SwapStatus.EXPIRED
                expired_count += 1

        return expired_count

    def _count_swaps_this_week(self, employee_id: str) -> int:
        """Count approved swaps for employee in current week."""
        week_start = datetime.utcnow() - timedelta(days=datetime.utcnow().weekday())
        count = 0

        for request in self.swaps.values():
            if (
                request.requester_id == employee_id
                and request.status == SwapStatus.APPROVED
                and request.created_at >= week_start
            ):
                count += 1

        return count


# ============================================================================
# NotificationManager Class
# ============================================================================

class NotificationManager:
    """
    Manages notifications sent to employees across multiple channels.

    Supports: email, SMS, push notifications, and in-app notifications.
    Tracks delivery status and read status.
    """

    def __init__(self, channels: Optional[List[NotificationChannel]] = None):
        """
        Initialize NotificationManager.

        Args:
            channels: List of notification channels to use (default: IN_APP only)
        """
        self.channels = channels or [NotificationChannel.IN_APP]
        self.notifications: Dict[str, Notification] = {}

    def notify_roster_published(
        self, employees: List[Employee], roster_period: str
    ) -> List[Notification]:
        """
        Notify all staff of new roster publication.

        Args:
            employees: List of employees to notify
            roster_period: Description of roster period (e.g., "Week of 14 April")

        Returns:
            List[Notification]: Created notifications
        """
        notifications = []
        title, message = self._generate_message(
            NotificationType.ROSTER_PUBLISHED, period=roster_period
        )

        for emp in employees:
            notif = self._create_notification(
                emp.id,
                NotificationType.ROSTER_PUBLISHED,
                title,
                message,
                {"period": roster_period},
            )
            notifications.append(notif)

        return notifications

    def notify_shift_assigned(self, employee: Employee, shift: Shift) -> Notification:
        """
        Notify employee of shift assignment.

        Args:
            employee: Employee assigned
            shift: Assigned shift

        Returns:
            Notification: Created notification
        """
        title, message = self._generate_message(
            NotificationType.SHIFT_ASSIGNED,
            date=shift.date,
            time=f"{shift.start_time}-{shift.end_time}",
            role=shift.role,
        )

        return self._create_notification(
            employee.id,
            NotificationType.SHIFT_ASSIGNED,
            title,
            message,
            {
                "shift_id": shift.id,
                "date": shift.date,
                "time": f"{shift.start_time}-{shift.end_time}",
                "role": shift.role,
            },
        )

    def notify_shift_changed(
        self, employee: Employee, old_shift: Shift, new_shift: Shift
    ) -> Notification:
        """
        Notify employee of shift modification.

        Args:
            employee: Employee affected
            old_shift: Original shift details
            new_shift: New shift details

        Returns:
            Notification: Created notification
        """
        title, message = self._generate_message(
            NotificationType.SHIFT_CHANGED,
            old_time=f"{old_shift.start_time}-{old_shift.end_time}",
            new_time=f"{new_shift.start_time}-{new_shift.end_time}",
            date=new_shift.date,
        )

        return self._create_notification(
            employee.id,
            NotificationType.SHIFT_CHANGED,
            title,
            message,
            {
                "old_shift_id": old_shift.id,
                "new_shift_id": new_shift.id,
                "old_time": f"{old_shift.start_time}-{old_shift.end_time}",
                "new_time": f"{new_shift.start_time}-{new_shift.end_time}",
            },
        )

    def notify_swap_request(self, manager: Employee, request: SwapRequest) -> Notification:
        """
        Notify manager of pending swap request.

        Args:
            manager: Manager to notify
            request: Swap request to review

        Returns:
            Notification: Created notification
        """
        title, message = self._generate_message(
            NotificationType.SWAP_REQUESTED,
            requester=request.requester_name,
            date=request.original_date,
            time=f"{request.original_start}-{request.original_end}",
        )

        return self._create_notification(
            manager.id,
            NotificationType.SWAP_REQUESTED,
            title,
            message,
            {
                "swap_id": request.id,
                "requester_id": request.requester_id,
                "requester_name": request.requester_name,
                "date": request.original_date,
            },
        )

    def notify_swap_result(self, employee: Employee, request: SwapRequest) -> Notification:
        """
        Notify employee of swap request result (approved/rejected).

        Args:
            employee: Employee to notify
            request: Resolved swap request

        Returns:
            Notification: Created notification
        """
        is_approved = request.status == SwapStatus.APPROVED
        notif_type = (
            NotificationType.SWAP_APPROVED
            if is_approved
            else NotificationType.SWAP_REJECTED
        )

        title, message = self._generate_message(
            notif_type,
            status="approved" if is_approved else "rejected",
            date=request.original_date,
        )

        return self._create_notification(
            employee.id,
            notif_type,
            title,
            message,
            {
                "swap_id": request.id,
                "status": request.status.value,
                "reason": request.reason,
            },
        )

    def notify_shift_reminder(
        self, employee: Employee, shift: Shift, hours_before: int = 24
    ) -> Notification:
        """
        Send reminder notification before scheduled shift.

        Args:
            employee: Employee to remind
            shift: Shift coming up
            hours_before: Hours before shift to send reminder

        Returns:
            Notification: Created notification
        """
        title, message = self._generate_message(
            NotificationType.SHIFT_REMINDER,
            time=f"{shift.start_time}-{shift.end_time}",
            date=shift.date,
            hours=hours_before,
        )

        return self._create_notification(
            employee.id,
            NotificationType.SHIFT_REMINDER,
            title,
            message,
            {
                "shift_id": shift.id,
                "time": f"{shift.start_time}-{shift.end_time}",
                "hours_before": hours_before,
            },
        )

    def request_availability(
        self, employees: List[Employee], date_range: Tuple[str, str]
    ) -> List[Notification]:
        """
        Request availability from employees for a date range.

        Args:
            employees: Employees to request availability from
            date_range: Tuple of (start_date, end_date) in YYYY-MM-DD format

        Returns:
            List[Notification]: Created notifications
        """
        notifications = []
        start_date, end_date = date_range

        title, message = self._generate_message(
            NotificationType.AVAILABILITY_REQUEST,
            start_date=start_date,
            end_date=end_date,
        )

        for emp in employees:
            notif = self._create_notification(
                emp.id,
                NotificationType.AVAILABILITY_REQUEST,
                title,
                message,
                {
                    "start_date": start_date,
                    "end_date": end_date,
                },
            )
            notifications.append(notif)

        return notifications

    def get_unread(self, employee_id: str) -> List[Notification]:
        """
        Get all unread notifications for an employee.

        Args:
            employee_id: Employee ID

        Returns:
            List[Notification]: Unread notifications
        """
        return [
            n
            for n in self.notifications.values()
            if n.employee_id == employee_id and not n.is_read
        ]

    def mark_read(self, notification_id: str) -> Notification:
        """
        Mark notification as read.

        Args:
            notification_id: ID of notification to mark read

        Returns:
            Notification: Updated notification

        Raises:
            ValueError: If notification not found
        """
        notif = self.notifications.get(notification_id)
        if not notif:
            raise ValueError(f"Notification {notification_id} not found")

        notif.is_read = True
        notif.read_at = datetime.utcnow()

        return notif

    def send_batch(self, notifications: List[Notification]) -> Dict:
        """
        Batch send notifications with delivery tracking.

        Args:
            notifications: List of notifications to send

        Returns:
            Dict with keys: total, sent, failed, delivery_channels
        """
        result = {
            "total": len(notifications),
            "sent": 0,
            "failed": 0,
            "delivery_channels": {ch.value: 0 for ch in self.channels},
        }

        for notif in notifications:
            # Simulate sending through configured channels
            for channel in self.channels:
                notif.sent_via = channel
                result["delivery_channels"][channel.value] += 1
                result["sent"] += 1

        return result

    def _create_notification(
        self,
        employee_id: str,
        notif_type: NotificationType,
        title: str,
        message: str,
        metadata: Dict,
    ) -> Notification:
        """Create and store a notification."""
        notif = Notification(
            employee_id=employee_id,
            type=notif_type,
            title=title,
            message=message,
            metadata=metadata,
            sent_via=self.channels[0] if self.channels else NotificationChannel.IN_APP,
        )
        self.notifications[notif.id] = notif
        return notif

    def _format_shift_time(self, shift: Shift) -> str:
        """
        Format shift time in human-readable format.

        Returns string like: "Mon 14 Apr, 10am-6pm"

        Args:
            shift: Shift to format

        Returns:
            str: Formatted shift time
        """
        date_obj = datetime.fromisoformat(shift.date)
        day_name = date_obj.strftime("%a")
        date_str = date_obj.strftime("%d %b")

        start_hour = int(shift.start_time.split(":")[0])
        end_hour = int(shift.end_time.split(":")[0])

        start_ampm = "am" if start_hour < 12 else "pm"
        end_ampm = "am" if end_hour < 12 else "pm"

        start_display = start_hour if start_hour <= 12 else start_hour - 12
        end_display = end_hour if end_hour <= 12 else end_hour - 12

        return f"{day_name} {date_str}, {start_display}{start_ampm}-{end_display}{end_ampm}"

    def _generate_message(
        self, notif_type: NotificationType, **context
    ) -> Tuple[str, str]:
        """
        Generate notification title and message based on type.

        Args:
            notif_type: Type of notification
            **context: Context variables for message generation

        Returns:
            Tuple[str, str]: (title, message)
        """
        templates = {
            NotificationType.ROSTER_PUBLISHED: (
                "New Roster Published",
                f"Your roster for {context.get('period', 'next period')} is now available.",
            ),
            NotificationType.SHIFT_ASSIGNED: (
                "Shift Assigned",
                f"You've been assigned to work {context.get('time', '')} on {context.get('date', '')} as {context.get('role', 'Staff')}.",
            ),
            NotificationType.SHIFT_CHANGED: (
                "Shift Changed",
                f"Your shift on {context.get('date', '')} has been changed from {context.get('old_time', '')} to {context.get('new_time', '')}.",
            ),
            NotificationType.SWAP_REQUESTED: (
                "Swap Request Pending",
                f"{context.get('requester', 'An employee')} requested to swap their shift on {context.get('date', '')} ({context.get('time', '')}). Review needed.",
            ),
            NotificationType.SWAP_APPROVED: (
                "Swap Approved",
                f"Your swap request for {context.get('date', '')} has been {context.get('status', 'approved')}.",
            ),
            NotificationType.SWAP_REJECTED: (
                "Swap Rejected",
                f"Your swap request for {context.get('date', '')} has been {context.get('status', 'rejected')}.",
            ),
            NotificationType.SHIFT_REMINDER: (
                "Shift Reminder",
                f"You have a shift coming up on {context.get('date', '')} from {context.get('time', '')} ({context.get('hours', '24')} hours away).",
            ),
            NotificationType.AVAILABILITY_REQUEST: (
                "Availability Request",
                f"Please provide your availability for {context.get('start_date', '')} to {context.get('end_date', '')}.",
            ),
        }

        return templates.get(
            notif_type, ("Notification", "You have a new notification.")
        )


# ============================================================================
# FastAPI Router
# ============================================================================

def create_swap_router(
    swap_manager: SwapManager, notification_manager: NotificationManager
) -> APIRouter:
    """
    Create FastAPI router for shift swap and notification endpoints.

    Args:
        swap_manager: SwapManager instance
        notification_manager: NotificationManager instance

    Returns:
        APIRouter: Configured router
    """
    router = APIRouter(prefix="/api", tags=["swaps"])

    @router.post("/swaps")
    def create_swap(request: SwapRequestCreate, current_user_id: str):
        """Create a new shift swap request."""
        try:
            original_shift = swap_manager.shifts.get(request.original_shift_id)
            target_shift = (
                swap_manager.shifts.get(request.target_shift_id)
                if request.target_shift_id
                else None
            )
            requester = swap_manager.employees.get(current_user_id)

            if not original_shift or not requester:
                raise HTTPException(status_code=404, detail="Shift or employee not found")

            target_employee = (
                swap_manager.employees.get(request.target_employee_id)
                if request.target_employee_id
                else None
            )

            swap = swap_manager.create_swap_request(
                requester=requester,
                original_shift=original_shift,
                target_shift=target_shift,
                target_employee=target_employee,
                reason=request.reason,
            )

            return swap.to_dict()
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))

    @router.get("/swaps")
    def list_swaps(
        status: Optional[str] = Query(None),
        employee_id: Optional[str] = Query(None),
    ):
        """List swap requests with optional filtering."""
        results = list(swap_manager.swaps.values())

        if status:
            results = [
                s for s in results if s.status.value == status
            ]

        if employee_id:
            results = [
                s
                for s in results
                if s.requester_id == employee_id or s.target_employee_id == employee_id
            ]

        return [s.to_dict() for s in results]

    @router.post("/swaps/{swap_id}/approve")
    def approve_swap(swap_id: str, request: SwapApprovalRequest):
        """Approve a shift swap request."""
        try:
            swap = swap_manager.approve_swap(swap_id, request.approver_id)
            requester = swap_manager.employees.get(swap.requester_id)

            if requester:
                notification_manager.notify_swap_result(requester, swap)

            return swap.to_dict()
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))

    @router.post("/swaps/{swap_id}/reject")
    def reject_swap(swap_id: str, request: SwapRejectionRequest):
        """Reject a shift swap request."""
        try:
            swap = swap_manager.reject_swap(
                swap_id, request.approver_id, request.reason
            )
            requester = swap_manager.employees.get(swap.requester_id)

            if requester:
                notification_manager.notify_swap_result(requester, swap)

            return swap.to_dict()
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))

    @router.get("/shifts/open")
    def list_open_shifts():
        """Get list of open shifts available for pickup."""
        shifts = list(swap_manager.shifts.values())
        open_shifts = swap_manager.get_open_shifts(shifts)
        return [
            {
                "id": s.id,
                "date": s.date,
                "time": f"{s.start_time}-{s.end_time}",
                "role": s.role,
                "cost": s.cost,
            }
            for s in open_shifts
        ]

    @router.post("/shifts/{shift_id}/claim")
    def claim_shift(shift_id: str, request: ShiftClaimRequest):
        """Employee claims an open shift."""
        try:
            shift = swap_manager.shifts.get(shift_id)
            employee = swap_manager.employees.get(request.employee_id)

            if not shift or not employee:
                raise HTTPException(status_code=404, detail="Shift or employee not found")

            claim = swap_manager.claim_open_shift(employee, shift)
            return claim.to_dict()
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))

    @router.get("/notifications")
    def get_notifications(current_user_id: str):
        """Get notifications for current user."""
        notifications = notification_manager.get_unread(current_user_id)
        return [n.to_dict() for n in notifications]

    @router.post("/notifications/{notification_id}/read")
    def mark_notification_read(notification_id: str):
        """Mark a notification as read."""
        try:
            notif = notification_manager.mark_read(notification_id)
            return notif.to_dict()
        except ValueError as e:
            raise HTTPException(status_code=404, detail=str(e))

    return router
