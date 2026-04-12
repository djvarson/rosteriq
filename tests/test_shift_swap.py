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

Runs with: python tests/test_shift_swap.py
"""

import sys
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import Mock, MagicMock, patch

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from rosteriq.shift_swap import (  # noqa: E402
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
# Helper factories (were pytest fixtures)
# ============================================================================

def _swap_rules():
    return SwapRule(
        allow_open_swaps=True,
        require_manager_approval=True,
        auto_approve_same_role=True,
        auto_approve_same_cost=False,
        max_swaps_per_week=3,
        min_notice_hours=24,
        blackout_dates=[],
    )

def _swap_manager(rules=None):
    return SwapManager(rules or _swap_rules())

def _notification_manager():
    return NotificationManager(
        channels=[NotificationChannel.IN_APP, NotificationChannel.EMAIL]
    )

def _employee_alice():
    return Employee(
        id="emp_001", name="Alice",
        roles={"bartender", "server"}, hourly_rate=25.0,
        unavailable_dates=[], active=True,
    )

def _employee_bob():
    return Employee(
        id="emp_002", name="Bob",
        roles={"server", "kitchen"}, hourly_rate=22.0,
        unavailable_dates=[], active=True,
    )

def _employee_charlie():
    return Employee(
        id="emp_003", name="Charlie",
        roles={"bartender"}, hourly_rate=25.0,
        unavailable_dates=["2026-04-20"], active=True,
    )

def _future_date():
    return (datetime.utcnow() + timedelta(days=3)).strftime("%Y-%m-%d")

def _shift_alice_monday():
    return Shift(
        id="shift_001", employee_id="emp_001",
        date=_future_date(), start_time="10:00", end_time="18:00",
        role="server", cost=80.0, required=True,
    )

def _shift_bob_monday():
    return Shift(
        id="shift_002", employee_id="emp_002",
        date=_future_date(), start_time="14:00", end_time="22:00",
        role="kitchen", cost=88.0, required=True,
    )

def _shift_unassigned_monday():
    return Shift(
        id="shift_003", employee_id=None,
        date=_future_date(), start_time="06:00", end_time="14:00",
        role="kitchen", cost=80.0, required=True,
    )


# ============================================================================
# SwapRequest Tests
# ============================================================================

def test_swap_request_creation():
    req = SwapRequest(
        requester_id="emp_001", requester_name="Alice",
        original_shift_id="shift_001", original_date="2026-04-20",
        original_start="10:00", original_end="18:00",
        reason="Family event",
    )
    assert req.requester_id == "emp_001"
    assert req.requester_name == "Alice"
    assert req.status == SwapStatus.PENDING
    assert req.reason == "Family event"
    assert req.auto_approved is False

def test_swap_request_to_dict():
    req = SwapRequest(
        requester_id="emp_001", requester_name="Alice",
        original_shift_id="shift_001", original_date="2026-04-20",
        original_start="10:00", original_end="18:00",
        status=SwapStatus.APPROVED,
    )
    result = req.to_dict()
    assert result["requester_id"] == "emp_001"
    assert result["status"] == "APPROVED"
    assert "created_at" in result
    assert "id" in result

def test_swap_request_with_target_employee():
    req = SwapRequest(
        requester_id="emp_001", requester_name="Alice",
        original_shift_id="shift_001", original_date="2026-04-20",
        original_start="10:00", original_end="18:00",
        target_shift_id="shift_002",
        target_employee_id="emp_002", target_employee_name="Bob",
    )
    assert req.target_employee_id == "emp_002"
    assert req.target_employee_name == "Bob"


# ============================================================================
# Notification Tests
# ============================================================================

def test_notification_creation():
    notif = Notification(
        employee_id="emp_001", type=NotificationType.SHIFT_ASSIGNED,
        title="Shift Assigned", message="You're assigned to Mon 14 Apr, 10am-6pm",
    )
    assert notif.employee_id == "emp_001"
    assert notif.type == NotificationType.SHIFT_ASSIGNED
    assert not notif.is_read

def test_notification_to_dict():
    notif = Notification(
        employee_id="emp_001", type=NotificationType.SHIFT_ASSIGNED,
        title="Shift Assigned", message="You're assigned",
        sent_via=NotificationChannel.EMAIL,
    )
    result = notif.to_dict()
    assert result["employee_id"] == "emp_001"
    assert result["type"] == "SHIFT_ASSIGNED"
    assert result["sent_via"] == "EMAIL"
    assert "created_at" in result

def test_notification_mark_read():
    notif = Notification(
        employee_id="emp_001", type=NotificationType.SHIFT_ASSIGNED,
        title="Test", message="Test",
    )
    assert not notif.is_read
    notif.is_read = True
    notif.read_at = datetime.utcnow()
    assert notif.is_read
    assert notif.read_at is not None


# ============================================================================
# SwapManager Creation Tests
# ============================================================================

def test_create_swap_request_valid():
    sm = _swap_manager()
    alice = _employee_alice()
    shift = _shift_alice_monday()
    request = sm.create_swap_request(requester=alice, original_shift=shift, reason="Family event")
    assert request.requester_id == "emp_001"
    assert request.original_shift_id == "shift_001"
    assert request.reason == "Family event"
    assert request.status == SwapStatus.PENDING

def test_create_swap_requires_ownership():
    sm = _swap_manager()
    bob = _employee_bob()
    shift = _shift_alice_monday()
    try:
        sm.create_swap_request(requester=bob, original_shift=shift)
        raise AssertionError("Should have raised ValueError")
    except ValueError as e:
        assert "must own the original shift" in str(e)

def test_create_swap_with_insufficient_notice():
    sm = _swap_manager()
    alice = _employee_alice()
    soon = (datetime.utcnow() + timedelta(hours=12)).strftime("%Y-%m-%d")
    shift = Shift(
        id="shift_soon", employee_id="emp_001",
        date=soon, start_time="10:00", end_time="18:00",
        role="server", cost=80.0,
    )
    try:
        sm.create_swap_request(requester=alice, original_shift=shift)
        raise AssertionError("Should have raised ValueError")
    except ValueError as e:
        assert "24 hours notice" in str(e)

def test_create_swap_on_blackout_date():
    sm = _swap_manager()
    alice = _employee_alice()
    shift = _shift_alice_monday()
    sm.rules.blackout_dates = [shift.date]
    try:
        sm.create_swap_request(requester=alice, original_shift=shift)
        raise AssertionError("Should have raised ValueError")
    except ValueError as e:
        assert "blackout period" in str(e)


# ============================================================================
# SwapManager Eligibility Tests
# ============================================================================

def test_get_eligible_swaps_with_matching_role():
    sm = _swap_manager()
    bob = _employee_bob()
    shift = _shift_alice_monday()
    sm.register_employee(bob)
    charlie = Employee(id="emp_003", name="Charlie", roles={"bartender"}, hourly_rate=25.0)
    sm.register_employee(charlie)
    eligible = sm.get_eligible_swaps(shift, [bob, charlie])
    assert bob in eligible
    assert charlie not in eligible

def test_get_eligible_swaps_excludes_assigned():
    sm = _swap_manager()
    alice = _employee_alice()
    shift = _shift_alice_monday()
    sm.register_employee(alice)
    eligible = sm.get_eligible_swaps(shift, [alice])
    assert alice not in eligible

def test_get_eligible_swaps_respects_unavailability():
    sm = _swap_manager()
    charlie = _employee_charlie()
    shift = _shift_alice_monday()
    charlie.unavailable_dates = [shift.date]
    charlie.roles.add("server")
    sm.register_employee(charlie)
    eligible = sm.get_eligible_swaps(shift, [charlie])
    assert charlie not in eligible

def test_get_eligible_swaps_respects_weekly_limit():
    sm = _swap_manager()
    bob = _employee_bob()
    shift = _shift_alice_monday()
    sm.register_employee(bob)
    sm.rules.max_swaps_per_week = 2
    for i in range(2):
        req = SwapRequest(requester_id="emp_002", status=SwapStatus.APPROVED, created_at=datetime.utcnow())
        sm.swaps[f"swap_{i}"] = req
    eligible = sm.get_eligible_swaps(shift, [bob])
    assert bob not in eligible


# ============================================================================
# SwapManager Evaluation Tests
# ============================================================================

def test_evaluate_swap_open_swap():
    sm = _swap_manager()
    request = SwapRequest(
        requester_id="emp_001", original_shift_id="shift_001",
        target_shift_id=None, target_employee_id=None,
    )
    evaluation = sm.evaluate_swap(request)
    assert evaluation["eligible"] is True
    assert evaluation["recommendation"] == "OPEN_SWAP"

def test_evaluate_swap_missing_shift():
    sm = _swap_manager()
    request = SwapRequest(requester_id="emp_001", original_shift_id="nonexistent")
    evaluation = sm.evaluate_swap(request)
    assert evaluation["eligible"] is False
    assert evaluation["recommendation"] == "REJECT"

def test_evaluate_swap_role_mismatch():
    sm = _swap_manager()
    bob = _employee_bob()
    shift_a = _shift_alice_monday()
    shift_b = _shift_bob_monday()
    sm.register_shift(shift_a)
    sm.register_shift(shift_b)
    sm.register_employee(bob)
    request = SwapRequest(
        requester_id="emp_001", original_shift_id="shift_001",
        target_shift_id="shift_002", target_employee_id="emp_002",
    )
    evaluation = sm.evaluate_swap(request)
    assert evaluation["eligible"] is False
    assert "lacks required role" in evaluation["reasons"][0]

def test_evaluate_swap_auto_approve_conditions():
    sm = _swap_manager()
    shift_a = _shift_alice_monday()
    bob_same_role = Employee(id="emp_002", name="Bob", roles={"server"}, hourly_rate=25.0)
    shift_bob_same = Shift(
        id="shift_002", employee_id="emp_002",
        date=shift_a.date, start_time="14:00", end_time="22:00",
        role="server", cost=80.0,
    )
    sm.register_shift(shift_a)
    sm.register_shift(shift_bob_same)
    sm.register_employee(bob_same_role)
    request = SwapRequest(
        requester_id="emp_001", original_shift_id="shift_001",
        target_shift_id="shift_002", target_employee_id="emp_002",
    )
    evaluation = sm.evaluate_swap(request)
    assert evaluation["score"] >= 75.0
    assert evaluation["recommendation"] == "APPROVE"


# ============================================================================
# SwapManager Approval Tests
# ============================================================================

def test_approve_swap_updates_assignments():
    sm = _swap_manager()
    alice = _employee_alice()
    bob = _employee_bob()
    shift_a = _shift_alice_monday()
    shift_b = _shift_bob_monday()
    sm.register_shift(shift_a)
    sm.register_shift(shift_b)
    request = sm.create_swap_request(
        requester=alice, original_shift=shift_a,
        target_shift=shift_b, target_employee=bob,
    )
    sm.approve_swap(request.id, "manager_001")
    assert shift_a.employee_id == "emp_002"
    assert shift_b.employee_id == "emp_001"
    assert request.status == SwapStatus.APPROVED
    assert request.resolved_by == "manager_001"

def test_reject_swap():
    sm = _swap_manager()
    alice = _employee_alice()
    shift = _shift_alice_monday()
    request = sm.create_swap_request(requester=alice, original_shift=shift, reason="Need swap")
    sm.reject_swap(request.id, "manager_001", "Insufficient notice")
    assert request.status == SwapStatus.REJECTED
    assert request.resolved_by == "manager_001"

def test_cannot_approve_non_pending():
    sm = _swap_manager()
    alice = _employee_alice()
    shift = _shift_alice_monday()
    request = sm.create_swap_request(requester=alice, original_shift=shift)
    request.status = SwapStatus.REJECTED
    try:
        sm.approve_swap(request.id, "manager_001")
        raise AssertionError("Should have raised ValueError")
    except ValueError as e:
        assert "Cannot approve" in str(e)

def test_cancel_swap():
    sm = _swap_manager()
    alice = _employee_alice()
    shift = _shift_alice_monday()
    request = sm.create_swap_request(requester=alice, original_shift=shift)
    sm.cancel_swap(request.id)
    assert request.status == SwapStatus.CANCELLED


# ============================================================================
# SwapManager Auto-Approval Tests
# ============================================================================

def test_auto_process_swaps_approves_eligible():
    sm = _swap_manager()
    alice = _employee_alice()
    shift_a = _shift_alice_monday()
    sm.register_shift(shift_a)
    bob_server = Employee(id="emp_002", name="Bob", roles={"server"})
    shift_bob_server = Shift(
        id="shift_002", employee_id="emp_002",
        date=shift_a.date, start_time="14:00", end_time="22:00",
        role="server", cost=80.0,
    )
    sm.register_shift(shift_bob_server)
    sm.register_employee(bob_server)
    request = sm.create_swap_request(
        requester=alice, original_shift=shift_a,
        target_shift=shift_bob_server, target_employee=bob_server,
    )
    processed = sm.auto_process_swaps([request])
    assert len(processed) > 0
    assert processed[0].status == SwapStatus.APPROVED
    assert processed[0].auto_approved is True

def test_auto_process_only_pending():
    sm = _swap_manager()
    alice = _employee_alice()
    shift = _shift_alice_monday()
    request = sm.create_swap_request(requester=alice, original_shift=shift)
    request.status = SwapStatus.APPROVED
    processed = sm.auto_process_swaps([request])
    assert len(processed) == 0


# ============================================================================
# Open Shifts Tests
# ============================================================================

def test_get_open_shifts():
    sm = _swap_manager()
    shift_a = _shift_alice_monday()
    shift_u = _shift_unassigned_monday()
    shift_a.employee_id = "emp_001"
    open_shifts = sm.get_open_shifts([shift_a, shift_u])
    assert len(open_shifts) == 1
    assert open_shifts[0].id == "shift_003"

def test_claim_open_shift():
    sm = _swap_manager()
    alice = _employee_alice()
    shift = _shift_unassigned_monday()
    claim = sm.claim_open_shift(alice, shift)
    assert claim.requester_id == "emp_001"
    assert claim.status == SwapStatus.APPROVED
    assert claim.auto_approved is True
    assert shift.employee_id == "emp_001"

def test_cannot_claim_assigned_shift():
    sm = _swap_manager()
    alice = _employee_alice()
    shift = _shift_alice_monday()
    try:
        sm.claim_open_shift(alice, shift)
        raise AssertionError("Should have raised ValueError")
    except ValueError as e:
        assert "already assigned" in str(e)

def test_cannot_claim_without_role():
    sm = _swap_manager()
    shift = _shift_unassigned_monday()
    diana = Employee(id="emp_004", name="Diana", roles={"bartender"})
    try:
        sm.claim_open_shift(diana, shift)
        raise AssertionError("Should have raised ValueError")
    except ValueError as e:
        assert "lacks required role" in str(e)

def test_cannot_claim_when_unavailable():
    sm = _swap_manager()
    alice = _employee_alice()
    shift = _shift_unassigned_monday()
    alice.unavailable_dates = [shift.date]
    try:
        sm.claim_open_shift(alice, shift)
        raise AssertionError("Should have raised ValueError")
    except ValueError as e:
        assert "unavailable" in str(e)


# ============================================================================
# Expiry Tests
# ============================================================================

def test_expire_stale_requests():
    sm = _swap_manager()
    alice = _employee_alice()
    shift = _shift_alice_monday()
    request = sm.create_swap_request(requester=alice, original_shift=shift)
    request.created_at = datetime.utcnow() - timedelta(days=3)
    expired_count = sm.expire_stale_requests(max_age_hours=48)
    assert expired_count == 1
    assert request.status == SwapStatus.EXPIRED

def test_expire_only_pending():
    sm = _swap_manager()
    alice = _employee_alice()
    shift = _shift_alice_monday()
    request = sm.create_swap_request(requester=alice, original_shift=shift)
    request.created_at = datetime.utcnow() - timedelta(days=3)
    request.status = SwapStatus.APPROVED
    expired_count = sm.expire_stale_requests(max_age_hours=48)
    assert expired_count == 0
    assert request.status == SwapStatus.APPROVED


# ============================================================================
# NotificationManager Tests
# ============================================================================

def test_get_unread_notifications():
    nm = _notification_manager()
    notif1 = Notification(employee_id="emp_001", type=NotificationType.SHIFT_ASSIGNED, title="Test 1", message="Message 1", is_read=False)
    notif2 = Notification(employee_id="emp_001", type=NotificationType.SHIFT_CHANGED, title="Test 2", message="Message 2", is_read=True)
    nm.notifications[notif1.id] = notif1
    nm.notifications[notif2.id] = notif2
    unread = nm.get_unread("emp_001")
    assert len(unread) == 1
    assert unread[0].id == notif1.id

def test_mark_notification_read():
    nm = _notification_manager()
    notif = Notification(employee_id="emp_001", type=NotificationType.SHIFT_ASSIGNED, title="Test", message="Test")
    nm.notifications[notif.id] = notif
    marked = nm.mark_read(notif.id)
    assert marked.is_read is True
    assert marked.read_at is not None

def test_mark_read_nonexistent():
    nm = _notification_manager()
    try:
        nm.mark_read("nonexistent")
        raise AssertionError("Should have raised ValueError")
    except ValueError:
        pass

def test_format_shift_time():
    nm = _notification_manager()
    shift = _shift_alice_monday()
    formatted = nm._format_shift_time(shift)
    assert "am" in formatted or "pm" in formatted
    assert "-" in formatted

def test_generate_message_roster_published():
    nm = _notification_manager()
    title, message = nm._generate_message(NotificationType.ROSTER_PUBLISHED, period="Week of 14 April")
    assert "Roster" in title
    assert "14 April" in message

def test_generate_message_shift_assigned():
    nm = _notification_manager()
    title, message = nm._generate_message(NotificationType.SHIFT_ASSIGNED, date="2026-04-14", time="10:00-18:00", role="server")
    assert "Shift" in title
    assert "server" in message

def test_generate_message_swap_requested():
    nm = _notification_manager()
    title, message = nm._generate_message(NotificationType.SWAP_REQUESTED, requester="Alice", date="2026-04-14", time="10:00-18:00")
    assert "Alice" in message

def test_generate_message_shift_reminder():
    nm = _notification_manager()
    title, message = nm._generate_message(NotificationType.SHIFT_REMINDER, date="2026-04-14", time="10:00-18:00", hours=24)
    assert "Reminder" in title
    assert "24" in message

def test_notify_with_multiple_channels():
    nm = NotificationManager(channels=[NotificationChannel.EMAIL, NotificationChannel.SMS, NotificationChannel.PUSH])
    notif = Notification(employee_id="emp_001", type=NotificationType.SHIFT_ASSIGNED, title="Test", message="Test")
    result = nm.send_batch([notif])
    assert result["total"] == 1
    assert result["sent"] == 3
    assert result["delivery_channels"]["EMAIL"] == 1
    assert result["delivery_channels"]["SMS"] == 1
    assert result["delivery_channels"]["PUSH"] == 1

def test_notify_shift_assigned():
    nm = _notification_manager()
    alice = _employee_alice()
    shift = _shift_alice_monday()
    notif = nm.notify_shift_assigned(alice, shift)
    assert notif.employee_id == "emp_001"
    assert notif.type == NotificationType.SHIFT_ASSIGNED
    assert "shift_001" in notif.metadata

def test_notify_shift_changed():
    nm = _notification_manager()
    alice = _employee_alice()
    notif = nm.notify_shift_changed(alice, _shift_alice_monday(), _shift_bob_monday())
    assert notif.employee_id == "emp_001"
    assert notif.type == NotificationType.SHIFT_CHANGED

def test_notify_swap_request():
    nm = _notification_manager()
    request = SwapRequest(id="swap_001", requester_id="emp_001", requester_name="Alice", original_date="2026-04-14")
    manager = Employee(id="mgr_001", name="Manager")
    notif = nm.notify_swap_request(manager, request)
    assert notif.employee_id == "mgr_001"
    assert notif.type == NotificationType.SWAP_REQUESTED

def test_notify_swap_result_approved():
    nm = _notification_manager()
    alice = _employee_alice()
    request = SwapRequest(id="swap_001", requester_id="emp_001", status=SwapStatus.APPROVED, original_date="2026-04-14")
    notif = nm.notify_swap_result(alice, request)
    assert notif.type == NotificationType.SWAP_APPROVED

def test_notify_swap_result_rejected():
    nm = _notification_manager()
    alice = _employee_alice()
    request = SwapRequest(id="swap_001", requester_id="emp_001", status=SwapStatus.REJECTED, original_date="2026-04-14")
    notif = nm.notify_swap_result(alice, request)
    assert notif.type == NotificationType.SWAP_REJECTED

def test_notify_shift_reminder():
    nm = _notification_manager()
    alice = _employee_alice()
    shift = _shift_alice_monday()
    notif = nm.notify_shift_reminder(alice, shift, hours_before=24)
    assert notif.type == NotificationType.SHIFT_REMINDER
    assert notif.metadata["hours_before"] == 24

def test_request_availability():
    nm = _notification_manager()
    alice = _employee_alice()
    bob = _employee_bob()
    notifications = nm.request_availability([alice, bob], ("2026-04-14", "2026-04-20"))
    assert len(notifications) == 2
    assert all(n.type == NotificationType.AVAILABILITY_REQUEST for n in notifications)


# ============================================================================
# API Router Tests
# ============================================================================

def test_create_router():
    sm = _swap_manager()
    nm = _notification_manager()
    router = create_swap_router(sm, nm)
    assert router is not None
    assert len(router.routes) > 0

def test_router_has_swap_endpoints():
    sm = _swap_manager()
    nm = _notification_manager()
    router = create_swap_router(sm, nm)
    route_paths = [route.path for route in router.routes]
    assert any("/swaps" in path for path in route_paths)
    assert any("/shifts/open" in path for path in route_paths)
    assert any("/notifications" in path for path in route_paths)


# ============================================================================
# Integration Tests
# ============================================================================

def test_end_to_end_swap_workflow():
    sm = _swap_manager()
    nm = _notification_manager()
    alice = _employee_alice()
    bob = _employee_bob()
    shift_a = _shift_alice_monday()
    shift_b = _shift_bob_monday()
    sm.register_shift(shift_a)
    sm.register_shift(shift_b)
    sm.register_employee(bob)

    request = sm.create_swap_request(
        requester=alice, original_shift=shift_a,
        target_shift=shift_b, target_employee=bob,
        reason="Personal emergency",
    )
    assert request.status == SwapStatus.PENDING

    manager = Employee(id="mgr_001", name="Manager")
    manager_notif = nm.notify_swap_request(manager, request)
    assert manager_notif.type == NotificationType.SWAP_REQUESTED

    sm.approve_swap(request.id, "mgr_001")
    assert request.status == SwapStatus.APPROVED

    requester_notif = nm.notify_swap_result(alice, request)
    assert requester_notif.type == NotificationType.SWAP_APPROVED
    assert shift_a.employee_id == "emp_002"
    assert shift_b.employee_id == "emp_001"

def test_end_to_end_open_shift_claim():
    sm = _swap_manager()
    nm = _notification_manager()
    alice = _employee_alice()
    shift = _shift_unassigned_monday()

    claim = sm.claim_open_shift(alice, shift)
    assert shift.employee_id == "emp_001"
    assert claim.status == SwapStatus.APPROVED

    notif = nm.notify_shift_assigned(alice, shift)
    assert notif.type == NotificationType.SHIFT_ASSIGNED

def test_multiple_swaps_weekly_limit():
    sm = _swap_manager()
    shift = _shift_alice_monday()
    sm.rules.max_swaps_per_week = 2

    for i in range(2):
        req = SwapRequest(requester_id="emp_001", original_shift_id="shift_001", status=SwapStatus.APPROVED, created_at=datetime.utcnow())
        sm.swaps[f"swap_{i}"] = req

    diana = Employee(id="emp_004", name="Diana", roles={"server"})
    sm.register_employee(diana)
    eligible = sm.get_eligible_swaps(shift, [diana])
    assert diana in eligible

    req3 = SwapRequest(requester_id="emp_004", status=SwapStatus.APPROVED, created_at=datetime.utcnow())
    sm.swaps["swap_2"] = req3
    eligible = sm.get_eligible_swaps(shift, [diana])
    assert diana not in eligible


# ============================================================================
# Runner
# ============================================================================

if __name__ == "__main__":
    tests = [v for k, v in list(globals().items()) if k.startswith("test_") and callable(v)]
    passed = failed = 0
    for t in tests:
        try:
            t()
            passed += 1
            print(f"  PASS {t.__name__}")
        except AssertionError as e:
            failed += 1
            print(f"  FAIL {t.__name__}: {e}")
        except Exception as e:
            failed += 1
            print(f"  ERROR {t.__name__}: {type(e).__name__}: {e}")
    print(f"\n{passed}/{passed + failed} tests passed")
    sys.exit(0 if failed == 0 else 1)
