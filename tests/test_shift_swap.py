"""
Comprehensive tests for shift swap and notification system.

Tests cover:
- Swap creation and validation
- Eligibility checking
- Auto-approval logic
- Rejection and cancellation
- Notification formatting and delivery
- Open shift management
- Request expiry
- API router endpoints
"""

import pytest
from datetime import datetime, timedelta
from unittest.mock import Mock, MagicMock, patch

from rosteriq.shift_swap import (
    SwapManager,
    NotificationManager,
    SwapRequest,
    Notification,
    SwapStatus,
    NotificationType,
    NotificationChannel,
    SwapRule,
    Shift,
    Employee,
    create_swap_router,
)


# ============================================================================
# Fixtures
# ============================================================================

@pytest.fixture
def swap_rules():
    """Create default swap rules."""
    return SwapRule(
        allow_open_swaps=True,
        require_manager_approval=True,
        auto_approve_same_role=True,
        auto_approve_same_cost=False,
        max_swaps_per_week=3,
        min_notice_hours=24,
        blackout_dates=[],
    )


@pytest.fixture
def swap_manager(swap_rules):
    """Create SwapManager instance."""
    return SwapManager(swap_rules)


@pytest.fixture
def notification_manager():
    """Create NotificationManager instance."""
    return NotificationManager(
        channels=[NotificationChannel.IN_APP, NotificationChannel.EMAIL]
    )


@pytest.fixture
def employee_alice():
    """Create test employee Alice."""
    return Employee(
        id="emp_001",
        name="Alice",
        roles={"bartender", "server"},
        hourly_rate=25.0,
        unavailable_dates=[],
        active=True,
    )


@pytest.fixture
def employee_bob():
    """Create test employee Bob."""
    return Employee(
        id="emp_002",
        name="Bob",
        roles={"server", "kitchen"},
        hourly_rate=22.0,
        unavailable_dates=[],
        active=True,
    )


@pytest.fixture
def employee_charlie():
    """Create test employee Charlie."""
    return Employee(
        id="emp_003",
        name="Charlie",
        roles={"bartender"},
        hourly_rate=25.0,
        unavailable_dates=["2026-04-20"],
        active=True,
    )


@pytest.fixture
def shift_alice_monday():
    """Create test shift for Alice on Monday."""
    tomorrow = (datetime.utcnow() + timedelta(days=3)).strftime("%Y-%m-%d")
    return Shift(
        id="shift_001",
        employee_id="emp_001",
        date=tomorrow,
        start_time="10:00",
        end_time="18:00",
        role="server",
        cost=80.0,
        required=True,
    )


@pytest.fixture
def shift_bob_monday():
    """Create test shift for Bob on Monday."""
    tomorrow = (datetime.utcnow() + timedelta(days=3)).strftime("%Y-%m-%d")
    return Shift(
        id="shift_002",
        employee_id="emp_002",
        date=tomorrow,
        start_time="14:00",
        end_time="22:00",
        role="kitchen",
        cost=88.0,
        required=True,
    )


@pytest.fixture
def shift_unassigned_monday():
    """Create unassigned shift for Monday."""
    tomorrow = (datetime.utcnow() + timedelta(days=3)).strftime("%Y-%m-%d")
    return Shift(
        id="shift_003",
        employee_id=None,
        date=tomorrow,
        start_time="06:00",
        end_time="14:00",
        role="kitchen",
        cost=80.0,
        required=True,
    )


# ============================================================================
# SwapRequest Tests
# ============================================================================

class TestSwapRequest:
    """Tests for SwapRequest data model."""

    def test_swap_request_creation(self):
        """Test creating a swap request."""
        req = SwapRequest(
            requester_id="emp_001",
            requester_name="Alice",
            original_shift_id="shift_001",
            original_date="2026-04-20",
            original_start="10:00",
            original_end="18:00",
            reason="Family event",
        )

        assert req.requester_id == "emp_001"
        assert req.requester_name == "Alice"
        assert req.status == SwapStatus.PENDING
        assert req.reason == "Family event"
        assert req.auto_approved is False

    def test_swap_request_to_dict(self):
        """Test converting swap request to dict."""
        req = SwapRequest(
            requester_id="emp_001",
            requester_name="Alice",
            original_shift_id="shift_001",
            original_date="2026-04-20",
            original_start="10:00",
            original_end="18:00",
            status=SwapStatus.APPROVED,
        )

        result = req.to_dict()
        assert result["requester_id"] == "emp_001"
        assert result["status"] == "APPROVED"
        assert "created_at" in result
        assert "id" in result

    def test_swap_request_with_target_employee(self):
        """Test swap request with specific target employee."""
        req = SwapRequest(
            requester_id="emp_001",
            requester_name="Alice",
            original_shift_id="shift_001",
            original_date="2026-04-20",
            original_start="10:00",
            original_end="18:00",
            target_shift_id="shift_002",
            target_employee_id="emp_002",
            target_employee_name="Bob",
        )

        assert req.target_employee_id == "emp_002"
        assert req.target_employee_name == "Bob"


# ============================================================================
# Notification Tests
# ============================================================================

class TestNotification:
    """Tests for Notification data model."""

    def test_notification_creation(self):
        """Test creating a notification."""
        notif = Notification(
            employee_id="emp_001",
            type=NotificationType.SHIFT_ASSIGNED,
            title="Shift Assigned",
            message="You're assigned to Mon 14 Apr, 10am-6pm",
        )

        assert notif.employee_id == "emp_001"
        assert notif.type == NotificationType.SHIFT_ASSIGNED
        assert not notif.is_read

    def test_notification_to_dict(self):
        """Test converting notification to dict."""
        notif = Notification(
            employee_id="emp_001",
            type=NotificationType.SHIFT_ASSIGNED,
            title="Shift Assigned",
            message="You're assigned",
            sent_via=NotificationChannel.EMAIL,
        )

        result = notif.to_dict()
        assert result["employee_id"] == "emp_001"
        assert result["type"] == "SHIFT_ASSIGNED"
        assert result["sent_via"] == "EMAIL"
        assert "created_at" in result

    def test_notification_mark_read(self):
        """Test marking notification as read."""
        notif = Notification(
            employee_id="emp_001",
            type=NotificationType.SHIFT_ASSIGNED,
            title="Test",
            message="Test",
        )

        assert not notif.is_read
        notif.is_read = True
        notif.read_at = datetime.utcnow()

        assert notif.is_read
        assert notif.read_at is not None


# ============================================================================
# SwapManager Tests
# ============================================================================

class TestSwapManagerCreation:
    """Tests for swap request creation."""

    def test_create_swap_request_valid(self, swap_manager, employee_alice, shift_alice_monday):
        """Test creating a valid swap request."""
        request = swap_manager.create_swap_request(
            requester=employee_alice,
            original_shift=shift_alice_monday,
            reason="Family event",
        )

        assert request.requester_id == "emp_001"
        assert request.original_shift_id == "shift_001"
        assert request.reason == "Family event"
        assert request.status == SwapStatus.PENDING

    def test_create_swap_requires_ownership(self, swap_manager, employee_bob, shift_alice_monday):
        """Test that requester must own the shift."""
        with pytest.raises(ValueError, match="must own the original shift"):
            swap_manager.create_swap_request(
                requester=employee_bob,
                original_shift=shift_alice_monday,
            )

    def test_create_swap_with_insufficient_notice(self, swap_manager, employee_alice):
        """Test that swap requires minimum notice."""
        soon = (datetime.utcnow() + timedelta(hours=12)).strftime("%Y-%m-%d")
        shift = Shift(
            id="shift_soon",
            employee_id="emp_001",
            date=soon,
            start_time="10:00",
            end_time="18:00",
            role="server",
            cost=80.0,
        )

        with pytest.raises(ValueError, match="24 hours notice"):
            swap_manager.create_swap_request(
                requester=employee_alice,
                original_shift=shift,
            )

    def test_create_swap_on_blackout_date(self, swap_manager, employee_alice, shift_alice_monday):
        """Test that swaps cannot be created on blackout dates."""
        swap_manager.rules.blackout_dates = [shift_alice_monday.date]

        with pytest.raises(ValueError, match="blackout period"):
            swap_manager.create_swap_request(
                requester=employee_alice,
                original_shift=shift_alice_monday,
            )


class TestSwapManagerEligibility:
    """Tests for checking swap eligibility."""

    def test_get_eligible_swaps_with_matching_role(
        self, swap_manager, employee_bob, shift_alice_monday
    ):
        """Test finding employees with matching role."""
        swap_manager.register_employee(employee_bob)
        emp_charlie = Employee(
            id="emp_003",
            name="Charlie",
            roles={"bartender"},
            hourly_rate=25.0,
        )
        swap_manager.register_employee(emp_charlie)

        eligible = swap_manager.get_eligible_swaps(shift_alice_monday, [employee_bob, emp_charlie])

        # Bob has "server" role, shift needs "server", so Bob is eligible
        assert employee_bob in eligible
        # Charlie only has "bartender", so not eligible
        assert emp_charlie not in eligible

    def test_get_eligible_swaps_excludes_assigned(
        self, swap_manager, employee_alice, shift_alice_monday
    ):
        """Test that assigned employee is excluded."""
        swap_manager.register_employee(employee_alice)

        eligible = swap_manager.get_eligible_swaps(shift_alice_monday, [employee_alice])

        # Alice is already assigned to this shift
        assert employee_alice not in eligible

    def test_get_eligible_swaps_respects_unavailability(
        self, swap_manager, employee_charlie, shift_alice_monday
    ):
        """Test that unavailable dates are respected."""
        # Make Charlie unavailable on the shift date
        employee_charlie.unavailable_dates = [shift_alice_monday.date]
        swap_manager.register_employee(employee_charlie)
        # Also give Charlie the server role
        employee_charlie.roles.add("server")

        eligible = swap_manager.get_eligible_swaps(shift_alice_monday, [employee_charlie])

        # Charlie is unavailable on that date
        assert employee_charlie not in eligible

    def test_get_eligible_swaps_respects_weekly_limit(
        self, swap_manager, employee_bob, shift_alice_monday
    ):
        """Test that weekly swap limits are enforced."""
        swap_manager.register_employee(employee_bob)
        swap_manager.rules.max_swaps_per_week = 2

        # Create 2 approved swaps for Bob this week
        for i in range(2):
            req = SwapRequest(
                requester_id="emp_002",
                status=SwapStatus.APPROVED,
                created_at=datetime.utcnow(),
            )
            swap_manager.swaps[f"swap_{i}"] = req

        eligible = swap_manager.get_eligible_swaps(shift_alice_monday, [employee_bob])

        # Bob has hit the limit
        assert employee_bob not in eligible


class TestSwapManagerEvaluation:
    """Tests for swap evaluation logic."""

    def test_evaluate_swap_open_swap(self, swap_manager, employee_alice, shift_alice_monday):
        """Test evaluation of open swap (no target)."""
        request = SwapRequest(
            requester_id="emp_001",
            original_shift_id="shift_001",
            target_shift_id=None,
            target_employee_id=None,
        )

        evaluation = swap_manager.evaluate_swap(request)

        assert evaluation["eligible"] is True
        assert evaluation["recommendation"] == "OPEN_SWAP"

    def test_evaluate_swap_missing_shift(self, swap_manager):
        """Test evaluation with missing shift."""
        request = SwapRequest(
            requester_id="emp_001",
            original_shift_id="nonexistent",
        )

        evaluation = swap_manager.evaluate_swap(request)

        assert evaluation["eligible"] is False
        assert evaluation["recommendation"] == "REJECT"

    def test_evaluate_swap_role_mismatch(
        self, swap_manager, employee_alice, employee_bob, shift_alice_monday, shift_bob_monday
    ):
        """Test evaluation with role mismatch."""
        swap_manager.register_shift(shift_alice_monday)
        swap_manager.register_shift(shift_bob_monday)
        swap_manager.register_employee(employee_bob)

        request = SwapRequest(
            requester_id="emp_001",
            original_shift_id="shift_001",
            target_shift_id="shift_002",
            target_employee_id="emp_002",
        )

        evaluation = swap_manager.evaluate_swap(request)

        assert evaluation["eligible"] is False
        assert "lacks required role" in evaluation["reasons"][0]

    def test_evaluate_swap_auto_approve_conditions(
        self, swap_manager, employee_alice, employee_bob, shift_alice_monday
    ):
        """Test auto-approval conditions."""
        bob_same_role = Employee(
            id="emp_002",
            name="Bob",
            roles={"server"},
            hourly_rate=25.0,
        )
        shift_bob_same_role = Shift(
            id="shift_002",
            employee_id="emp_002",
            date=shift_alice_monday.date,
            start_time="14:00",
            end_time="22:00",
            role="server",
            cost=80.0,
        )

        swap_manager.register_shift(shift_alice_monday)
        swap_manager.register_shift(shift_bob_same_role)
        swap_manager.register_employee(bob_same_role)

        request = SwapRequest(
            requester_id="emp_001",
            original_shift_id="shift_001",
            target_shift_id="shift_002",
            target_employee_id="emp_002",
        )

        evaluation = swap_manager.evaluate_swap(request)

        assert evaluation["score"] >= 75.0
        assert evaluation["recommendation"] == "APPROVE"


class TestSwapManagerApproval:
    """Tests for swap approval and rejection."""

    def test_approve_swap_updates_assignments(
        self, swap_manager, employee_alice, employee_bob, shift_alice_monday, shift_bob_monday
    ):
        """Test that approval updates shift assignments."""
        swap_manager.register_shift(shift_alice_monday)
        swap_manager.register_shift(shift_bob_monday)

        request = swap_manager.create_swap_request(
            requester=employee_alice,
            original_shift=shift_alice_monday,
            target_shift=shift_bob_monday,
            target_employee=employee_bob,
        )

        swap_manager.approve_swap(request.id, "manager_001")

        assert shift_alice_monday.employee_id == "emp_002"
        assert shift_bob_monday.employee_id == "emp_001"
        assert request.status == SwapStatus.APPROVED
        assert request.resolved_by == "manager_001"

    def test_reject_swap(self, swap_manager, employee_alice, shift_alice_monday):
        """Test rejecting a swap."""
        request = swap_manager.create_swap_request(
            requester=employee_alice,
            original_shift=shift_alice_monday,
            reason="Need swap",
        )

        swap_manager.reject_swap(request.id, "manager_001", "Insufficient notice")

        assert request.status == SwapStatus.REJECTED
        assert request.resolved_by == "manager_001"

    def test_cannot_approve_non_pending(self, swap_manager, employee_alice, shift_alice_monday):
        """Test that non-pending swaps cannot be approved."""
        request = swap_manager.create_swap_request(
            requester=employee_alice,
            original_shift=shift_alice_monday,
        )

        request.status = SwapStatus.REJECTED
        with pytest.raises(ValueError, match="Cannot approve"):
            swap_manager.approve_swap(request.id, "manager_001")

    def test_cancel_swap(self, swap_manager, employee_alice, shift_alice_monday):
        """Test cancelling a swap."""
        request = swap_manager.create_swap_request(
            requester=employee_alice,
            original_shift=shift_alice_monday,
        )

        swap_manager.cancel_swap(request.id)

        assert request.status == SwapStatus.CANCELLED


class TestSwapManagerAutoApproval:
    """Tests for automatic swap processing."""

    def test_auto_process_swaps_approves_eligible(
        self, swap_manager, employee_alice, employee_bob, shift_alice_monday, shift_bob_monday
    ):
        """Test that auto-processing approves eligible swaps."""
        swap_manager.register_shift(shift_alice_monday)
        swap_manager.register_shift(shift_bob_monday)
        swap_manager.register_employee(employee_bob)

        # Create a swap with matching conditions
        bob_server = Employee(
            id="emp_002",
            name="Bob",
            roles={"server"},
        )
        shift_bob_server = Shift(
            id="shift_002",
            employee_id="emp_002",
            date=shift_alice_monday.date,
            start_time="14:00",
            end_time="22:00",
            role="server",
            cost=80.0,
        )

        swap_manager.register_shift(shift_bob_server)
        swap_manager.register_employee(bob_server)

        request = swap_manager.create_swap_request(
            requester=employee_alice,
            original_shift=shift_alice_monday,
            target_shift=shift_bob_server,
            target_employee=bob_server,
        )

        processed = swap_manager.auto_process_swaps([request])

        assert len(processed) > 0
        assert processed[0].status == SwapStatus.APPROVED
        assert processed[0].auto_approved is True

    def test_auto_process_only_pending(self, swap_manager, employee_alice, shift_alice_monday):
        """Test that only PENDING swaps are auto-processed."""
        request = swap_manager.create_swap_request(
            requester=employee_alice,
            original_shift=shift_alice_monday,
        )

        request.status = SwapStatus.APPROVED
        processed = swap_manager.auto_process_swaps([request])

        assert len(processed) == 0


class TestSwapManagerOpenShifts:
    """Tests for open shift management."""

    def test_get_open_shifts(self, swap_manager, shift_alice_monday, shift_unassigned_monday):
        """Test getting open shifts."""
        shift_alice_monday.employee_id = "emp_001"  # Ensure it's assigned
        shifts = [shift_alice_monday, shift_unassigned_monday]

        open_shifts = swap_manager.get_open_shifts(shifts)

        assert len(open_shifts) == 1
        assert open_shifts[0].id == "shift_003"

    def test_claim_open_shift(self, swap_manager, employee_alice, shift_unassigned_monday):
        """Test employee claiming an open shift."""
        claim = swap_manager.claim_open_shift(employee_alice, shift_unassigned_monday)

        assert claim.requester_id == "emp_001"
        assert claim.status == SwapStatus.APPROVED
        assert claim.auto_approved is True
        assert shift_unassigned_monday.employee_id == "emp_001"

    def test_cannot_claim_assigned_shift(self, swap_manager, employee_alice, shift_alice_monday):
        """Test that assigned shifts cannot be claimed."""
        with pytest.raises(ValueError, match="already assigned"):
            swap_manager.claim_open_shift(employee_alice, shift_alice_monday)

    def test_cannot_claim_without_role(self, swap_manager, shift_unassigned_monday):
        """Test that employee without role cannot claim."""
        emp_no_kitchen = Employee(
            id="emp_004",
            name="Diana",
            roles={"bartender"},  # No kitchen role
        )

        with pytest.raises(ValueError, match="lacks required role"):
            swap_manager.claim_open_shift(emp_no_kitchen, shift_unassigned_monday)

    def test_cannot_claim_when_unavailable(self, swap_manager, employee_alice, shift_unassigned_monday):
        """Test that unavailable employees cannot claim."""
        employee_alice.unavailable_dates = [shift_unassigned_monday.date]

        with pytest.raises(ValueError, match="unavailable"):
            swap_manager.claim_open_shift(employee_alice, shift_unassigned_monday)


class TestSwapManagerExpiry:
    """Tests for request expiry logic."""

    def test_expire_stale_requests(self, swap_manager, employee_alice, shift_alice_monday):
        """Test that old pending requests are expired."""
        request = swap_manager.create_swap_request(
            requester=employee_alice,
            original_shift=shift_alice_monday,
        )

        # Manually set created_at to 3 days ago
        request.created_at = datetime.utcnow() - timedelta(days=3)

        expired_count = swap_manager.expire_stale_requests(max_age_hours=48)

        assert expired_count == 1
        assert request.status == SwapStatus.EXPIRED

    def test_expire_only_pending(self, swap_manager, employee_alice, shift_alice_monday):
        """Test that only PENDING requests are expired."""
        request = swap_manager.create_swap_request(
            requester=employee_alice,
            original_shift=shift_alice_monday,
        )

        request.created_at = datetime.utcnow() - timedelta(days=3)
        request.status = SwapStatus.APPROVED

        expired_count = swap_manager.expire_stale_requests(max_age_hours=48)

        assert expired_count == 0
        assert request.status == SwapStatus.APPROVED


# ============================================================================
# NotificationManager Tests
# ============================================================================

class TestNotificationManagerBasic:
    """Tests for basic notification operations."""

    def test_get_unread_notifications(self, notification_manager):
        """Test retrieving unread notifications."""
        notif1 = Notification(
            employee_id="emp_001",
            type=NotificationType.SHIFT_ASSIGNED,
            title="Test 1",
            message="Message 1",
            is_read=False,
        )
        notif2 = Notification(
            employee_id="emp_001",
            type=NotificationType.SHIFT_CHANGED,
            title="Test 2",
            message="Message 2",
            is_read=True,
        )

        notification_manager.notifications[notif1.id] = notif1
        notification_manager.notifications[notif2.id] = notif2

        unread = notification_manager.get_unread("emp_001")

        assert len(unread) == 1
        assert unread[0].id == notif1.id

    def test_mark_notification_read(self, notification_manager):
        """Test marking notification as read."""
        notif = Notification(
            employee_id="emp_001",
            type=NotificationType.SHIFT_ASSIGNED,
            title="Test",
            message="Test",
        )
        notification_manager.notifications[notif.id] = notif

        marked = notification_manager.mark_read(notif.id)

        assert marked.is_read is True
        assert marked.read_at is not None

    def test_mark_read_nonexistent(self, notification_manager):
        """Test marking nonexistent notification as read."""
        with pytest.raises(ValueError):
            notification_manager.mark_read("nonexistent")


class TestNotificationManagerGeneration:
    """Tests for notification message generation."""

    def test_format_shift_time(self, notification_manager, shift_alice_monday):
        """Test human-readable shift time formatting."""
        formatted = notification_manager._format_shift_time(shift_alice_monday)

        # Should be like "Mon 14 Apr, 10am-6pm"
        assert "am" in formatted or "pm" in formatted
        assert ":" not in formatted  # No seconds
        assert "-" in formatted

    def test_generate_message_roster_published(self, notification_manager):
        """Test generating roster published message."""
        title, message = notification_manager._generate_message(
            NotificationType.ROSTER_PUBLISHED,
            period="Week of 14 April"
        )

        assert "Roster" in title
        assert "14 April" in message

    def test_generate_message_shift_assigned(self, notification_manager):
        """Test generating shift assigned message."""
        title, message = notification_manager._generate_message(
            NotificationType.SHIFT_ASSIGNED,
            date="2026-04-14",
            time="10:00-18:00",
            role="server"
        )

        assert "Shift" in title
        assert "server" in message

    def test_generate_message_swap_requested(self, notification_manager):
        """Test generating swap request message."""
        title, message = notification_manager._generate_message(
            NotificationType.SWAP_REQUESTED,
            requester="Alice",
            date="2026-04-14",
            time="10:00-18:00"
        )

        assert "Alice" in message

    def test_generate_message_shift_reminder(self, notification_manager):
        """Test generating shift reminder message."""
        title, message = notification_manager._generate_message(
            NotificationType.SHIFT_REMINDER,
            date="2026-04-14",
            time="10:00-18:00",
            hours=24
        )

        assert "Reminder" in title
        assert "24" in message


class TestNotificationManagerChannels:
    """Tests for notification delivery channels."""

    def test_notify_with_multiple_channels(self):
        """Test sending notifications through multiple channels."""
        nm = NotificationManager(
            channels=[NotificationChannel.EMAIL, NotificationChannel.SMS, NotificationChannel.PUSH]
        )

        notif = Notification(
            employee_id="emp_001",
            type=NotificationType.SHIFT_ASSIGNED,
            title="Test",
            message="Test",
        )

        result = nm.send_batch([notif])

        assert result["total"] == 1
        assert result["sent"] == 3  # Sent through 3 channels
        assert result["delivery_channels"]["EMAIL"] == 1
        assert result["delivery_channels"]["SMS"] == 1
        assert result["delivery_channels"]["PUSH"] == 1


class TestNotificationManagerMethods:
    """Tests for specific notification methods."""

    def test_notify_shift_assigned(self, notification_manager, employee_alice, shift_alice_monday):
        """Test shift assignment notification."""
        notif = notification_manager.notify_shift_assigned(employee_alice, shift_alice_monday)

        assert notif.employee_id == "emp_001"
        assert notif.type == NotificationType.SHIFT_ASSIGNED
        assert "shift_001" in notif.metadata

    def test_notify_shift_changed(self, notification_manager, employee_alice, shift_alice_monday, shift_bob_monday):
        """Test shift change notification."""
        notif = notification_manager.notify_shift_changed(
            employee_alice, shift_alice_monday, shift_bob_monday
        )

        assert notif.employee_id == "emp_001"
        assert notif.type == NotificationType.SHIFT_CHANGED

    def test_notify_swap_request(self, notification_manager, employee_alice):
        """Test swap request notification."""
        request = SwapRequest(
            id="swap_001",
            requester_id="emp_001",
            requester_name="Alice",
            original_date="2026-04-14",
        )

        manager = Employee(id="mgr_001", name="Manager")
        notif = notification_manager.notify_swap_request(manager, request)

        assert notif.employee_id == "mgr_001"
        assert notif.type == NotificationType.SWAP_REQUESTED

    def test_notify_swap_result_approved(self, notification_manager, employee_alice):
        """Test swap approval notification."""
        request = SwapRequest(
            id="swap_001",
            requester_id="emp_001",
            status=SwapStatus.APPROVED,
            original_date="2026-04-14",
        )

        notif = notification_manager.notify_swap_result(employee_alice, request)

        assert notif.type == NotificationType.SWAP_APPROVED

    def test_notify_swap_result_rejected(self, notification_manager, employee_alice):
        """Test swap rejection notification."""
        request = SwapRequest(
            id="swap_001",
            requester_id="emp_001",
            status=SwapStatus.REJECTED,
            original_date="2026-04-14",
        )

        notif = notification_manager.notify_swap_result(employee_alice, request)

        assert notif.type == NotificationType.SWAP_REJECTED

    def test_notify_shift_reminder(self, notification_manager, employee_alice, shift_alice_monday):
        """Test shift reminder notification."""
        notif = notification_manager.notify_shift_reminder(
            employee_alice, shift_alice_monday, hours_before=24
        )

        assert notif.type == NotificationType.SHIFT_REMINDER
        assert notif.metadata["hours_before"] == 24

    def test_request_availability(self, notification_manager, employee_alice, employee_bob):
        """Test requesting employee availability."""
        notifications = notification_manager.request_availability(
            [employee_alice, employee_bob],
            ("2026-04-14", "2026-04-20")
        )

        assert len(notifications) == 2
        assert all(n.type == NotificationType.AVAILABILITY_REQUEST for n in notifications)


# ============================================================================
# API Router Tests
# ============================================================================

class TestAPIRouter:
    """Tests for FastAPI router endpoints."""

    def test_create_router(self, swap_manager, notification_manager):
        """Test creating the API router."""
        router = create_swap_router(swap_manager, notification_manager)

        assert router is not None
        assert len(router.routes) > 0

    def test_router_has_swap_endpoints(self, swap_manager, notification_manager):
        """Test that router has swap-related endpoints."""
        router = create_swap_router(swap_manager, notification_manager)

        route_paths = [route.path for route in router.routes]

        assert any("/swaps" in path for path in route_paths)
        assert any("/shifts/open" in path for path in route_paths)
        assert any("/notifications" in path for path in route_paths)


# ============================================================================
# Integration Tests
# ============================================================================

class TestIntegration:
    """Integration tests combining multiple components."""

    def test_end_to_end_swap_workflow(
        self, swap_manager, notification_manager, employee_alice, employee_bob,
        shift_alice_monday, shift_bob_monday
    ):
        """Test complete swap workflow: request -> approve -> notify."""
        swap_manager.register_shift(shift_alice_monday)
        swap_manager.register_shift(shift_bob_monday)
        swap_manager.register_employee(employee_bob)

        # Step 1: Request swap
        request = swap_manager.create_swap_request(
            requester=employee_alice,
            original_shift=shift_alice_monday,
            target_shift=shift_bob_monday,
            target_employee=employee_bob,
            reason="Personal emergency",
        )

        assert request.status == SwapStatus.PENDING

        # Step 2: Notify manager
        manager = Employee(id="mgr_001", name="Manager")
        manager_notif = notification_manager.notify_swap_request(manager, request)

        assert manager_notif.type == NotificationType.SWAP_REQUESTED

        # Step 3: Approve swap
        swap_manager.approve_swap(request.id, "mgr_001")
        assert request.status == SwapStatus.APPROVED

        # Step 4: Notify requester
        requester_notif = notification_manager.notify_swap_result(employee_alice, request)

        assert requester_notif.type == NotificationType.SWAP_APPROVED
        assert shift_alice_monday.employee_id == "emp_002"
        assert shift_bob_monday.employee_id == "emp_001"

    def test_end_to_end_open_shift_claim(
        self, swap_manager, notification_manager, employee_alice, shift_unassigned_monday
    ):
        """Test complete open shift claim workflow."""
        initial_state = shift_unassigned_monday.employee_id

        # Claim shift
        claim = swap_manager.claim_open_shift(employee_alice, shift_unassigned_monday)

        assert shift_unassigned_monday.employee_id == "emp_001"
        assert claim.status == SwapStatus.APPROVED

        # Notify employee
        notif = notification_manager.notify_shift_assigned(
            employee_alice, shift_unassigned_monday
        )

        assert notif.type == NotificationType.SHIFT_ASSIGNED

    def test_multiple_swaps_weekly_limit(
        self, swap_manager, employee_alice, shift_alice_monday
    ):
        """Test that weekly swap limits are enforced across requests."""
        swap_manager.rules.max_swaps_per_week = 2

        # Create 2 approved swaps
        for i in range(2):
            req = SwapRequest(
                requester_id="emp_001",
                original_shift_id="shift_001",
                status=SwapStatus.APPROVED,
                created_at=datetime.utcnow(),
            )
            swap_manager.swaps[f"swap_{i}"] = req

        emp_available = Employee(
            id="emp_004",
            name="Diana",
            roles={"server"},
        )
        swap_manager.register_employee(emp_available)

        eligible = swap_manager.get_eligible_swaps(shift_alice_monday, [emp_available])

        assert emp_available in eligible  # Diana hasn't hit limit

        # Add one more approved swap for Diana
        req3 = SwapRequest(
            requester_id="emp_004",
            status=SwapStatus.APPROVED,
            created_at=datetime.utcnow(),
        )
        swap_manager.swaps["swap_2"] = req3

        eligible = swap_manager.get_eligible_swaps(shift_alice_monday, [emp_available])

        assert emp_available not in eligible  # Diana now hits limit


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
