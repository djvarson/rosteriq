"""Test suite for leave_management.py module.

Tests the LeaveStore with 30+ comprehensive test cases covering:
- Accrual calculations for different employment types and leave types
- Request lifecycle (submit → approve, submit → reject, submit → cancel)
- Balance tracking and updates
- Conflict detection with shifts
- Leave calendar generation
- Persistence roundtrip
- Edge cases: casual workers, cancelling approved leave
"""

import sys
import os
import unittest
import tempfile
from datetime import datetime, timezone, date, timedelta

# Add parent to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from rosteriq.leave_management import (
    get_leave_store,
    _reset_for_tests,
    LeaveRequest,
    LeaveBalance,
    LeaveType,
    LeaveStatus,
    calculate_accrual,
)
from rosteriq import persistence as _p


class TestLeaveAccrual(unittest.TestCase):
    """Test accrual calculation logic."""

    def test_annual_leave_full_time(self):
        """Test annual leave accrual for full-time employee."""
        # 4 weeks per year = 152 hours for full-time (38 hrs/week)
        # After 1 year
        hours = calculate_accrual("full_time", 38.0, 12.0, LeaveType.ANNUAL)
        self.assertAlmostEqual(hours, 152.0, places=1)

    def test_annual_leave_part_time(self):
        """Test annual leave accrual for part-time employee."""
        # 20 hrs/week for 1 year = 20 * 4 = 80 hours
        hours = calculate_accrual("part_time", 20.0, 12.0, LeaveType.ANNUAL)
        self.assertAlmostEqual(hours, 80.0, places=1)

    def test_annual_leave_half_year(self):
        """Test annual leave accrual for 6 months."""
        # Half year = 76 hours
        hours = calculate_accrual("full_time", 38.0, 6.0, LeaveType.ANNUAL)
        self.assertAlmostEqual(hours, 76.0, places=1)

    def test_personal_leave_full_time(self):
        """Test personal/carer's leave for full-time (10 days = 80 hours)."""
        hours = calculate_accrual("full_time", 38.0, 12.0, LeaveType.PERSONAL_CARER)
        self.assertAlmostEqual(hours, 80.0, places=1)

    def test_personal_leave_part_time(self):
        """Test personal/carer's leave for part-time."""
        hours = calculate_accrual("part_time", 20.0, 12.0, LeaveType.PERSONAL_CARER)
        self.assertGreater(hours, 0)

    def test_casual_no_paid_leave(self):
        """Test that casual workers don't accrue paid leave."""
        annual = calculate_accrual("casual", 30.0, 12.0, LeaveType.ANNUAL)
        personal = calculate_accrual("casual", 30.0, 12.0, LeaveType.PERSONAL_CARER)
        long_service = calculate_accrual("casual", 30.0, 12.0, LeaveType.LONG_SERVICE)
        self.assertEqual(annual, 0.0)
        self.assertEqual(personal, 0.0)
        self.assertEqual(long_service, 0.0)

    def test_compassionate_leave_not_accruing(self):
        """Test that compassionate leave doesn't accrue (per-event)."""
        hours = calculate_accrual("full_time", 38.0, 12.0, LeaveType.COMPASSIONATE)
        self.assertEqual(hours, 0.0)

    def test_community_service_not_accruing(self):
        """Test that community service leave doesn't accrue."""
        hours = calculate_accrual("full_time", 38.0, 12.0, LeaveType.COMMUNITY_SERVICE)
        self.assertEqual(hours, 0.0)

    def test_long_service_before_10_years(self):
        """Test that long service leave doesn't accrue before 10 years."""
        hours = calculate_accrual("full_time", 38.0, 60.0, LeaveType.LONG_SERVICE)  # 5 years
        self.assertEqual(hours, 0.0)

    def test_long_service_after_10_years(self):
        """Test long service leave accrual after 10 years."""
        # QLD: 8.667 weeks (≈346.67 hours) after 10 years
        hours = calculate_accrual("full_time", 38.0, 120.0, LeaveType.LONG_SERVICE)  # 10 years
        self.assertGreater(hours, 0)
        # At 10 years, accrual should be around 346.67 * 1 year = 346.67
        self.assertLess(hours, 400)

    def test_unpaid_leave_unlimited(self):
        """Test that unpaid leave is unlimited."""
        hours = calculate_accrual("full_time", 38.0, 12.0, LeaveType.UNPAID)
        self.assertEqual(hours, float("inf"))


class TestLeaveStore(unittest.TestCase):
    """Test suite for leave store functionality."""

    @classmethod
    def setUpClass(cls):
        """Set up a temp DB file for the entire test class."""
        cls.temp_db = tempfile.NamedTemporaryFile(delete=False, suffix=".db")
        cls.temp_db.close()
        cls.db_path = cls.temp_db.name
        os.environ["ROSTERIQ_DB_PATH"] = cls.db_path

    @classmethod
    def tearDownClass(cls):
        """Clean up temp DB file."""
        if os.path.exists(cls.db_path):
            os.unlink(cls.db_path)
        if "ROSTERIQ_DB_PATH" in os.environ:
            del os.environ["ROSTERIQ_DB_PATH"]

    def setUp(self):
        """Reset store and persistence before each test."""
        _reset_for_tests()
        _p.reset_for_tests()
        _p.reset_rehydrate_for_tests()
        # Clear the DB file between tests
        if os.path.exists(self.db_path):
            os.unlink(self.db_path)
        _p.init_db()

    def tearDown(self):
        """Clean up after each test."""
        _p.reset_for_tests()

    def test_submit_leave_request(self):
        """Test submitting a valid leave request."""
        store = get_leave_store()
        # Ensure balance exists first
        store._get_or_create_balance("emp_001", "Alice", "venue_001", LeaveType.ANNUAL)
        balance = store._balances["emp_001:venue_001:annual"]
        balance.accrued_hours = 152.0  # Full-time annual leave

        request = store.submit_leave_request(
            employee_id="emp_001",
            employee_name="Alice",
            venue_id="venue_001",
            leave_type=LeaveType.ANNUAL,
            start_date="2026-05-01",
            end_date="2026-05-05",
            hours_requested=40.0,
            reason="Planned vacation",
        )

        self.assertEqual(request.employee_id, "emp_001")
        self.assertEqual(request.employee_name, "Alice")
        self.assertEqual(request.venue_id, "venue_001")
        self.assertEqual(request.leave_type, LeaveType.ANNUAL)
        self.assertEqual(request.start_date, "2026-05-01")
        self.assertEqual(request.end_date, "2026-05-05")
        self.assertEqual(request.hours_requested, 40.0)
        self.assertEqual(request.status, LeaveStatus.PENDING)
        self.assertIsNotNone(request.request_id)
        self.assertIsNotNone(request.created_at)

    def test_submit_leave_insufficient_balance(self):
        """Test rejecting leave request with insufficient balance."""
        store = get_leave_store()
        # No balance or zero balance

        with self.assertRaises(ValueError) as context:
            store.submit_leave_request(
                employee_id="emp_001",
                employee_name="Alice",
                venue_id="venue_001",
                leave_type=LeaveType.ANNUAL,
                start_date="2026-05-01",
                end_date="2026-05-10",
                hours_requested=200.0,
                reason="Extended vacation",
            )

        self.assertIn("Insufficient", str(context.exception))

    def test_approve_leave(self):
        """Test approving a pending leave request."""
        store = get_leave_store()
        # Setup balance
        store._get_or_create_balance("emp_001", "Alice", "venue_001", LeaveType.ANNUAL)
        balance = store._balances["emp_001:venue_001:annual"]
        balance.accrued_hours = 152.0

        request = store.submit_leave_request(
            employee_id="emp_001",
            employee_name="Alice",
            venue_id="venue_001",
            leave_type=LeaveType.ANNUAL,
            start_date="2026-05-01",
            end_date="2026-05-05",
            hours_requested=40.0,
            reason="Vacation",
        )

        # Verify pending hours set
        balance = store._balances["emp_001:venue_001:annual"]
        self.assertEqual(balance.pending_hours, 40.0)

        # Approve
        approved = store.approve_leave(request.request_id, "mgr_001")
        self.assertEqual(approved.status, LeaveStatus.APPROVED)
        self.assertEqual(approved.decided_by, "mgr_001")
        self.assertIsNotNone(approved.decided_at)

        # Verify balance updated
        self.assertEqual(balance.pending_hours, 0.0)
        self.assertEqual(balance.used_hours, 40.0)

    def test_reject_leave(self):
        """Test rejecting a pending leave request."""
        store = get_leave_store()
        # Setup balance
        store._get_or_create_balance("emp_001", "Alice", "venue_001", LeaveType.ANNUAL)
        balance = store._balances["emp_001:venue_001:annual"]
        balance.accrued_hours = 152.0

        request = store.submit_leave_request(
            employee_id="emp_001",
            employee_name="Alice",
            venue_id="venue_001",
            leave_type=LeaveType.ANNUAL,
            start_date="2026-05-01",
            end_date="2026-05-05",
            hours_requested=40.0,
            reason="Vacation",
        )

        # Verify pending
        self.assertEqual(balance.pending_hours, 40.0)

        # Reject
        rejected = store.reject_leave(request.request_id, "mgr_001", "Understaffed period")
        self.assertEqual(rejected.status, LeaveStatus.REJECTED)
        self.assertEqual(rejected.notes, "Understaffed period")

        # Verify pending released
        self.assertEqual(balance.pending_hours, 0.0)
        self.assertEqual(balance.used_hours, 0.0)

    def test_cancel_approved_leave(self):
        """Test cancelling an approved leave request restores balance."""
        store = get_leave_store()
        # Setup balance
        store._get_or_create_balance("emp_001", "Alice", "venue_001", LeaveType.ANNUAL)
        balance = store._balances["emp_001:venue_001:annual"]
        balance.accrued_hours = 152.0

        request = store.submit_leave_request(
            employee_id="emp_001",
            employee_name="Alice",
            venue_id="venue_001",
            leave_type=LeaveType.ANNUAL,
            start_date="2026-05-01",
            end_date="2026-05-05",
            hours_requested=40.0,
            reason="Vacation",
        )

        # Approve
        store.approve_leave(request.request_id, "mgr_001")
        self.assertEqual(balance.used_hours, 40.0)

        # Cancel
        cancelled = store.cancel_leave(request.request_id)
        self.assertEqual(cancelled.status, LeaveStatus.CANCELLED)

        # Verify hours restored
        self.assertEqual(balance.used_hours, 0.0)
        self.assertEqual(balance.pending_hours, 0.0)

    def test_cancel_pending_leave(self):
        """Test cancelling a pending leave request releases pending hours."""
        store = get_leave_store()
        # Setup balance
        store._get_or_create_balance("emp_001", "Alice", "venue_001", LeaveType.ANNUAL)
        balance = store._balances["emp_001:venue_001:annual"]
        balance.accrued_hours = 152.0

        request = store.submit_leave_request(
            employee_id="emp_001",
            employee_name="Alice",
            venue_id="venue_001",
            leave_type=LeaveType.ANNUAL,
            start_date="2026-05-01",
            end_date="2026-05-05",
            hours_requested=40.0,
            reason="Vacation",
        )

        self.assertEqual(balance.pending_hours, 40.0)

        # Cancel without approval
        cancelled = store.cancel_leave(request.request_id)
        self.assertEqual(cancelled.status, LeaveStatus.CANCELLED)

        # Verify pending released
        self.assertEqual(balance.pending_hours, 0.0)

    def test_get_balances(self):
        """Test retrieving all balances for an employee."""
        store = get_leave_store()

        # Create balances for multiple leave types
        for leave_type in [LeaveType.ANNUAL, LeaveType.PERSONAL_CARER, LeaveType.LONG_SERVICE]:
            balance = store._get_or_create_balance("emp_001", "Alice", "venue_001", leave_type)
            balance.accrued_hours = 100.0

        balances = store.get_balances("emp_001", "venue_001")
        self.assertEqual(len(balances), 3)
        self.assertEqual(balances[0].employee_id, "emp_001")
        self.assertEqual(balances[0].accrued_hours, 100.0)

    def test_leave_calendar_approved_requests(self):
        """Test calendar generation includes only approved requests."""
        store = get_leave_store()
        store._get_or_create_balance("emp_001", "Alice", "venue_001", LeaveType.ANNUAL)
        balance = store._balances["emp_001:venue_001:annual"]
        balance.accrued_hours = 152.0

        # Submit and approve request for emp_001
        request = store.submit_leave_request(
            employee_id="emp_001",
            employee_name="Alice",
            venue_id="venue_001",
            leave_type=LeaveType.ANNUAL,
            start_date="2026-05-01",
            end_date="2026-05-03",
            hours_requested=24.0,
            reason="Vacation",
        )
        store.approve_leave(request.request_id, "mgr_001")

        # Setup balance and submit for emp_002
        store._get_or_create_balance("emp_002", "Bob", "venue_001", LeaveType.ANNUAL)
        balance2 = store._balances["emp_002:venue_001:annual"]
        balance2.accrued_hours = 152.0

        # Submit but don't approve second request for emp_002
        store.submit_leave_request(
            employee_id="emp_002",
            employee_name="Bob",
            venue_id="venue_001",
            leave_type=LeaveType.ANNUAL,
            start_date="2026-05-05",
            end_date="2026-05-07",
            hours_requested=24.0,
            reason="Vacation",
        )

        # Get calendar
        calendar = store.get_leave_calendar("venue_001", "2026-05-01", "2026-05-10")

        # Should only include approved request (3 days)
        self.assertEqual(len(calendar), 3)
        self.assertTrue(all(e["employee_id"] == "emp_001" for e in calendar))

    def test_calendar_date_filtering(self):
        """Test that calendar respects date ranges."""
        store = get_leave_store()
        store._get_or_create_balance("emp_001", "Alice", "venue_001", LeaveType.ANNUAL)
        balance = store._balances["emp_001:venue_001:annual"]
        balance.accrued_hours = 152.0

        request = store.submit_leave_request(
            employee_id="emp_001",
            employee_name="Alice",
            venue_id="venue_001",
            leave_type=LeaveType.ANNUAL,
            start_date="2026-05-10",
            end_date="2026-05-15",
            hours_requested=48.0,
            reason="Vacation",
        )
        store.approve_leave(request.request_id, "mgr_001")

        # Query before leave period
        calendar = store.get_leave_calendar("venue_001", "2026-05-01", "2026-05-09")
        self.assertEqual(len(calendar), 0)

        # Query overlapping - should return days from May 10 to May 12 (3 days)
        calendar = store.get_leave_calendar("venue_001", "2026-05-08", "2026-05-12")
        # This is May 10, 11, 12 = 3 days
        self.assertEqual(len(calendar), 3)

        # Query overlapping end - should return May 10 to May 15 (6 days)
        calendar = store.get_leave_calendar("venue_001", "2026-05-08", "2026-05-15")
        self.assertEqual(len(calendar), 6)

        # Query after
        calendar = store.get_leave_calendar("venue_001", "2026-05-16", "2026-05-30")
        self.assertEqual(len(calendar), 0)

    def test_conflict_detection_with_shifts(self):
        """Test detecting conflicts between leave and shifts."""
        store = get_leave_store()

        request = LeaveRequest(
            request_id="leave_001",
            employee_id="emp_001",
            employee_name="Alice",
            venue_id="venue_001",
            leave_type=LeaveType.ANNUAL,
            start_date="2026-05-01",
            end_date="2026-05-05",
            hours_requested=40.0,
            reason="Vacation",
        )

        shifts = [
            {"date": "2026-04-30", "start_time": "09:00", "end_time": "17:00"},
            {"date": "2026-05-01", "start_time": "09:00", "end_time": "17:00"},
            {"date": "2026-05-02", "start_time": "09:00", "end_time": "17:00"},
            {"date": "2026-05-04", "start_time": "14:00", "end_time": "22:00"},
            {"date": "2026-05-10", "start_time": "09:00", "end_time": "17:00"},
        ]

        conflict = store.check_conflicts(request, shifts)
        self.assertIsNotNone(conflict)
        # Should find 3 shifts: May 1, 2, and 4
        self.assertEqual(len(conflict.conflicting_shifts), 3)
        self.assertTrue(conflict.minimum_staff_warning)

    def test_no_conflict_before_leave(self):
        """Test that shifts before leave period don't conflict."""
        store = get_leave_store()

        request = LeaveRequest(
            request_id="leave_001",
            employee_id="emp_001",
            employee_name="Alice",
            venue_id="venue_001",
            leave_type=LeaveType.ANNUAL,
            start_date="2026-05-10",
            end_date="2026-05-15",
            hours_requested=40.0,
            reason="Vacation",
        )

        shifts = [
            {"date": "2026-05-01", "start_time": "09:00", "end_time": "17:00"},
            {"date": "2026-05-05", "start_time": "09:00", "end_time": "17:00"},
        ]

        conflict = store.check_conflicts(request, shifts)
        self.assertIsNone(conflict)

    def test_list_requests_by_status(self):
        """Test filtering requests by status."""
        store = get_leave_store()
        store._get_or_create_balance("emp_001", "Alice", "venue_001", LeaveType.ANNUAL)
        balance = store._balances["emp_001:venue_001:annual"]
        balance.accrued_hours = 152.0

        req1 = store.submit_leave_request(
            employee_id="emp_001",
            employee_name="Alice",
            venue_id="venue_001",
            leave_type=LeaveType.ANNUAL,
            start_date="2026-05-01",
            end_date="2026-05-05",
            hours_requested=40.0,
            reason="Vacation",
        )

        req2 = store.submit_leave_request(
            employee_id="emp_001",
            employee_name="Alice",
            venue_id="venue_001",
            leave_type=LeaveType.ANNUAL,
            start_date="2026-05-10",
            end_date="2026-05-15",
            hours_requested=48.0,
            reason="Vacation",
        )

        store.approve_leave(req1.request_id, "mgr_001")

        pending = store._list_requests(status=LeaveStatus.PENDING)
        self.assertEqual(len(pending), 1)
        self.assertEqual(pending[0].request_id, req2.request_id)

        approved = store._list_requests(status=LeaveStatus.APPROVED)
        self.assertEqual(len(approved), 1)
        self.assertEqual(approved[0].request_id, req1.request_id)

    def test_list_requests_by_date_range(self):
        """Test filtering requests by date range."""
        store = get_leave_store()
        store._get_or_create_balance("emp_001", "Alice", "venue_001", LeaveType.ANNUAL)
        balance = store._balances["emp_001:venue_001:annual"]
        balance.accrued_hours = 300.0

        req1 = store.submit_leave_request(
            employee_id="emp_001",
            employee_name="Alice",
            venue_id="venue_001",
            leave_type=LeaveType.ANNUAL,
            start_date="2026-05-01",
            end_date="2026-05-05",
            hours_requested=40.0,
            reason="May vacation",
        )

        req2 = store.submit_leave_request(
            employee_id="emp_001",
            employee_name="Alice",
            venue_id="venue_001",
            leave_type=LeaveType.ANNUAL,
            start_date="2026-06-10",
            end_date="2026-06-15",
            hours_requested=48.0,
            reason="June vacation",
        )

        # Query May range
        results = store._list_requests(date_from="2026-05-01", date_to="2026-05-31")
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].request_id, req1.request_id)

        # Query June range
        results = store._list_requests(date_from="2026-06-01", date_to="2026-06-30")
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].request_id, req2.request_id)

    def test_balance_available_hours_calculation(self):
        """Test that available_hours is computed correctly."""
        balance = LeaveBalance(
            employee_id="emp_001",
            employee_name="Alice",
            venue_id="venue_001",
            leave_type=LeaveType.ANNUAL,
            accrued_hours=152.0,
            used_hours=40.0,
            pending_hours=20.0,
        )

        expected = 152.0 - 40.0 - 20.0
        self.assertEqual(balance.available_hours, expected)

    def test_persistence_roundtrip(self):
        """Test that requests and balances persist and rehydrate."""
        store = get_leave_store()
        store._get_or_create_balance("emp_001", "Alice", "venue_001", LeaveType.ANNUAL)
        balance = store._balances["emp_001:venue_001:annual"]
        balance.accrued_hours = 152.0

        request = store.submit_leave_request(
            employee_id="emp_001",
            employee_name="Alice",
            venue_id="venue_001",
            leave_type=LeaveType.ANNUAL,
            start_date="2026-05-01",
            end_date="2026-05-05",
            hours_requested=40.0,
            reason="Vacation",
        )
        request_id = request.request_id

        store.approve_leave(request_id, "mgr_001")

        # Reset and rehydrate
        _reset_for_tests()
        _p.reset_for_tests()
        _p.reset_rehydrate_for_tests()
        _p.init_db()

        new_store = get_leave_store()
        retrieved = new_store._requests.get(request_id)

        self.assertIsNotNone(retrieved)
        self.assertEqual(retrieved.employee_id, "emp_001")
        self.assertEqual(retrieved.status, LeaveStatus.APPROVED)
        self.assertEqual(retrieved.decided_by, "mgr_001")

    def test_multiple_venues_isolation(self):
        """Test that leave requests are isolated by venue."""
        store = get_leave_store()

        for venue in ["venue_001", "venue_002"]:
            store._get_or_create_balance("emp_001", "Alice", venue, LeaveType.ANNUAL)
            balance = store._balances[f"emp_001:{venue}:annual"]
            balance.accrued_hours = 152.0

            store.submit_leave_request(
                employee_id="emp_001",
                employee_name="Alice",
                venue_id=venue,
                leave_type=LeaveType.ANNUAL,
                start_date="2026-05-01",
                end_date="2026-05-05",
                hours_requested=40.0,
                reason="Vacation",
            )

        requests_v1 = store._list_requests(venue_id="venue_001")
        requests_v2 = store._list_requests(venue_id="venue_002")

        self.assertEqual(len(requests_v1), 1)
        self.assertEqual(len(requests_v2), 1)
        self.assertEqual(requests_v1[0].venue_id, "venue_001")
        self.assertEqual(requests_v2[0].venue_id, "venue_002")

    def test_error_approve_non_pending(self):
        """Test that approving non-pending request raises error."""
        store = get_leave_store()
        store._get_or_create_balance("emp_001", "Alice", "venue_001", LeaveType.ANNUAL)
        balance = store._balances["emp_001:venue_001:annual"]
        balance.accrued_hours = 152.0

        request = store.submit_leave_request(
            employee_id="emp_001",
            employee_name="Alice",
            venue_id="venue_001",
            leave_type=LeaveType.ANNUAL,
            start_date="2026-05-01",
            end_date="2026-05-05",
            hours_requested=40.0,
            reason="Vacation",
        )

        store.approve_leave(request.request_id, "mgr_001")

        with self.assertRaises(ValueError):
            store.approve_leave(request.request_id, "mgr_001")

    def test_error_reject_non_pending(self):
        """Test that rejecting non-pending request raises error."""
        store = get_leave_store()
        store._get_or_create_balance("emp_001", "Alice", "venue_001", LeaveType.ANNUAL)
        balance = store._balances["emp_001:venue_001:annual"]
        balance.accrued_hours = 152.0

        request = store.submit_leave_request(
            employee_id="emp_001",
            employee_name="Alice",
            venue_id="venue_001",
            leave_type=LeaveType.ANNUAL,
            start_date="2026-05-01",
            end_date="2026-05-05",
            hours_requested=40.0,
            reason="Vacation",
        )

        store.reject_leave(request.request_id, "mgr_001", "Not approved")

        with self.assertRaises(ValueError):
            store.reject_leave(request.request_id, "mgr_001", "Already rejected")

    def test_request_to_dict(self):
        """Test serialization of LeaveRequest to dict."""
        request = LeaveRequest(
            request_id="leave_001",
            employee_id="emp_001",
            employee_name="Alice",
            venue_id="venue_001",
            leave_type=LeaveType.ANNUAL,
            start_date="2026-05-01",
            end_date="2026-05-05",
            hours_requested=40.0,
            reason="Vacation",
            status=LeaveStatus.PENDING,
        )

        d = request.to_dict()
        self.assertEqual(d["request_id"], "leave_001")
        self.assertEqual(d["leave_type"], "annual")
        self.assertEqual(d["status"], "pending")

    def test_balance_to_dict(self):
        """Test serialization of LeaveBalance to dict."""
        balance = LeaveBalance(
            employee_id="emp_001",
            employee_name="Alice",
            venue_id="venue_001",
            leave_type=LeaveType.ANNUAL,
            accrued_hours=152.0,
            used_hours=40.0,
            pending_hours=20.0,
        )

        d = balance.to_dict()
        self.assertEqual(d["accrued_hours"], 152.0)
        self.assertEqual(d["available_hours"], 92.0)
        self.assertEqual(d["leave_type"], "annual")


if __name__ == "__main__":
    unittest.main()
