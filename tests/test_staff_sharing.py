"""Test suite for staff_sharing.py module.

Tests the StaffSharingStore with 25+ comprehensive test cases covering:
- Transfer request lifecycle (request → approve → activate → complete)
- Transfer request rejection and cancellation
- Invalid state transitions
- Home venue registration
- Cross-venue availability
- Staff borrowing and lending
- Sharing statistics
- Persistence and thread safety
- Edge cases and error handling
"""

import sys
import os
import unittest
from datetime import date, datetime, timezone, timedelta

# Add parent to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from rosteriq.staff_sharing import (
    get_staff_sharing_store,
    _reset_for_tests,
    request_transfer,
    approve_transfer,
    reject_transfer,
    cancel_transfer,
    activate_transfer,
    complete_transfer,
    register_home_venue,
    set_cross_venue_availability,
    get_available_for_sharing,
    get_borrowable_staff,
    get_sharing_stats,
    get_transfer_request,
    list_transfer_requests,
    TransferStatus,
    TransferRequest,
    StaffHomeVenue,
    CrossVenueAvailability,
    SharingStats,
)


class TestTransferRequestLifecycle(unittest.TestCase):
    """Test the full transfer request lifecycle."""

    def setUp(self):
        """Reset store before each test."""
        _reset_for_tests()

    def test_request_transfer_creates_request(self):
        """Test that request_transfer creates a REQUESTED transfer."""
        req = request_transfer(
            employee_id="emp1",
            employee_name="John Doe",
            from_venue_id="v1",
            from_venue_name="Sydney Bar",
            to_venue_id="v2",
            to_venue_name="Melbourne Lounge",
            requested_by="mgr1",
            start_date=date(2026, 5, 1),
            end_date=date(2026, 5, 7),
            reason="Cross-venue cover",
            notes="Peak season coverage",
        )
        self.assertIsNotNone(req.request_id)
        self.assertEqual(req.employee_id, "emp1")
        self.assertEqual(req.employee_name, "John Doe")
        self.assertEqual(req.from_venue_id, "v1")
        self.assertEqual(req.to_venue_id, "v2")
        self.assertEqual(req.status, TransferStatus.REQUESTED)
        self.assertIsNone(req.approved_by)

    def test_approve_transfer_changes_status(self):
        """Test that approve_transfer moves to APPROVED state."""
        req1 = request_transfer(
            employee_id="emp1", employee_name="John Doe",
            from_venue_id="v1", from_venue_name="Sydney Bar",
            to_venue_id="v2", to_venue_name="Melbourne Lounge",
            requested_by="mgr1",
            start_date=date(2026, 5, 1), end_date=date(2026, 5, 7),
            reason="Cover",
        )
        req2 = approve_transfer(req1.request_id, "mgr_approver")
        self.assertEqual(req2.status, TransferStatus.APPROVED)
        self.assertEqual(req2.approved_by, "mgr_approver")
        self.assertIsNotNone(req2.approved_at)

    def test_full_lifecycle_request_to_complete(self):
        """Test full lifecycle: request → approve → activate → complete."""
        req = request_transfer(
            employee_id="emp1", employee_name="John Doe",
            from_venue_id="v1", from_venue_name="Sydney Bar",
            to_venue_id="v2", to_venue_name="Melbourne Lounge",
            requested_by="mgr1",
            start_date=date(2026, 5, 1), end_date=date(2026, 5, 7),
            reason="Cover",
        )
        self.assertEqual(req.status, TransferStatus.REQUESTED)

        req = approve_transfer(req.request_id, "approver")
        self.assertEqual(req.status, TransferStatus.APPROVED)

        req = activate_transfer(req.request_id)
        self.assertEqual(req.status, TransferStatus.ACTIVE)

        req = complete_transfer(req.request_id)
        self.assertEqual(req.status, TransferStatus.COMPLETED)

    def test_reject_transfer_from_requested(self):
        """Test that reject_transfer moves REQUESTED -> REJECTED."""
        req1 = request_transfer(
            employee_id="emp1", employee_name="John Doe",
            from_venue_id="v1", from_venue_name="Sydney Bar",
            to_venue_id="v2", to_venue_name="Melbourne Lounge",
            requested_by="mgr1",
            start_date=date(2026, 5, 1), end_date=date(2026, 5, 7),
            reason="Cover",
        )
        req2 = reject_transfer(req1.request_id)
        self.assertEqual(req2.status, TransferStatus.REJECTED)

    def test_cancel_transfer_from_requested(self):
        """Test that cancel_transfer works from REQUESTED state."""
        req1 = request_transfer(
            employee_id="emp1", employee_name="John Doe",
            from_venue_id="v1", from_venue_name="Sydney Bar",
            to_venue_id="v2", to_venue_name="Melbourne Lounge",
            requested_by="mgr1",
            start_date=date(2026, 5, 1), end_date=date(2026, 5, 7),
            reason="Cover",
        )
        req2 = cancel_transfer(req1.request_id)
        self.assertEqual(req2.status, TransferStatus.CANCELLED)

    def test_cancel_transfer_from_approved(self):
        """Test that cancel_transfer works from APPROVED state."""
        req1 = request_transfer(
            employee_id="emp1", employee_name="John Doe",
            from_venue_id="v1", from_venue_name="Sydney Bar",
            to_venue_id="v2", to_venue_name="Melbourne Lounge",
            requested_by="mgr1",
            start_date=date(2026, 5, 1), end_date=date(2026, 5, 7),
            reason="Cover",
        )
        req = approve_transfer(req1.request_id, "approver")
        req = cancel_transfer(req.request_id)
        self.assertEqual(req.status, TransferStatus.CANCELLED)


class TestInvalidTransitions(unittest.TestCase):
    """Test that invalid state transitions raise errors."""

    def setUp(self):
        """Reset store before each test."""
        _reset_for_tests()

    def test_cannot_approve_non_requested_transfer(self):
        """Test that approving a non-REQUESTED transfer raises error."""
        req = request_transfer(
            employee_id="emp1", employee_name="John Doe",
            from_venue_id="v1", from_venue_name="Sydney Bar",
            to_venue_id="v2", to_venue_name="Melbourne Lounge",
            requested_by="mgr1",
            start_date=date(2026, 5, 1), end_date=date(2026, 5, 7),
            reason="Cover",
        )
        approve_transfer(req.request_id, "approver")
        with self.assertRaises(ValueError):
            approve_transfer(req.request_id, "approver")

    def test_cannot_reject_approved_transfer(self):
        """Test that rejecting an APPROVED transfer raises error."""
        req = request_transfer(
            employee_id="emp1", employee_name="John Doe",
            from_venue_id="v1", from_venue_name="Sydney Bar",
            to_venue_id="v2", to_venue_name="Melbourne Lounge",
            requested_by="mgr1",
            start_date=date(2026, 5, 1), end_date=date(2026, 5, 7),
            reason="Cover",
        )
        req = approve_transfer(req.request_id, "approver")
        with self.assertRaises(ValueError):
            reject_transfer(req.request_id)

    def test_cannot_activate_non_approved_transfer(self):
        """Test that activating a non-APPROVED transfer raises error."""
        req = request_transfer(
            employee_id="emp1", employee_name="John Doe",
            from_venue_id="v1", from_venue_name="Sydney Bar",
            to_venue_id="v2", to_venue_name="Melbourne Lounge",
            requested_by="mgr1",
            start_date=date(2026, 5, 1), end_date=date(2026, 5, 7),
            reason="Cover",
        )
        with self.assertRaises(ValueError):
            activate_transfer(req.request_id)

    def test_cannot_complete_non_active_transfer(self):
        """Test that completing a non-ACTIVE transfer raises error."""
        req = request_transfer(
            employee_id="emp1", employee_name="John Doe",
            from_venue_id="v1", from_venue_name="Sydney Bar",
            to_venue_id="v2", to_venue_name="Melbourne Lounge",
            requested_by="mgr1",
            start_date=date(2026, 5, 1), end_date=date(2026, 5, 7),
            reason="Cover",
        )
        with self.assertRaises(ValueError):
            complete_transfer(req.request_id)

    def test_cannot_cancel_completed_transfer(self):
        """Test that cancelling a COMPLETED transfer raises error."""
        req = request_transfer(
            employee_id="emp1", employee_name="John Doe",
            from_venue_id="v1", from_venue_name="Sydney Bar",
            to_venue_id="v2", to_venue_name="Melbourne Lounge",
            requested_by="mgr1",
            start_date=date(2026, 5, 1), end_date=date(2026, 5, 7),
            reason="Cover",
        )
        req = approve_transfer(req.request_id, "approver")
        req = activate_transfer(req.request_id)
        req = complete_transfer(req.request_id)
        with self.assertRaises(ValueError):
            cancel_transfer(req.request_id)

    def test_nonexistent_request_raises_error(self):
        """Test that operations on nonexistent request raise error."""
        with self.assertRaises(ValueError):
            approve_transfer("nonexistent", "approver")


class TestHomeVenueRegistration(unittest.TestCase):
    """Test home venue registration and retrieval."""

    def setUp(self):
        """Reset store before each test."""
        _reset_for_tests()

    def test_register_home_venue(self):
        """Test registering a staff member's home venue."""
        home = register_home_venue(
            employee_id="emp1",
            employee_name="John Doe",
            home_venue_id="v1",
            home_venue_name="Sydney Bar",
        )
        self.assertEqual(home.employee_id, "emp1")
        self.assertEqual(home.employee_name, "John Doe")
        self.assertEqual(home.home_venue_id, "v1")
        self.assertEqual(home.home_venue_name, "Sydney Bar")
        self.assertEqual(len(home.secondary_venues), 0)

    def test_register_home_venue_with_secondaries(self):
        """Test registering with secondary venues."""
        home = register_home_venue(
            employee_id="emp1",
            employee_name="John Doe",
            home_venue_id="v1",
            home_venue_name="Sydney Bar",
            secondary_venues=["v2", "v3"],
        )
        self.assertEqual(home.secondary_venues, ["v2", "v3"])

    def test_register_home_venue_to_dict(self):
        """Test to_dict serialization of home venue."""
        home = register_home_venue(
            employee_id="emp1",
            employee_name="John Doe",
            home_venue_id="v1",
            home_venue_name="Sydney Bar",
        )
        d = home.to_dict()
        self.assertEqual(d["employee_id"], "emp1")
        self.assertEqual(d["home_venue_id"], "v1")
        self.assertIsInstance(d["secondary_venues"], list)


class TestCrossVenueAvailability(unittest.TestCase):
    """Test cross-venue availability management."""

    def setUp(self):
        """Reset store before each test."""
        _reset_for_tests()

    def test_set_cross_venue_availability(self):
        """Test setting staff cross-venue availability."""
        avail = set_cross_venue_availability(
            employee_id="emp1",
            employee_name="John Doe",
            home_venue_id="v1",
            available_venues=["v1", "v2", "v3"],
            roles=["bartender", "floor"],
            certs=["responsible_service", "food_handling"],
            max_hours=15.0,
        )
        self.assertEqual(avail.employee_id, "emp1")
        self.assertEqual(avail.home_venue_id, "v1")
        self.assertEqual(len(avail.available_venues), 3)
        self.assertEqual(len(avail.roles), 2)
        self.assertEqual(avail.max_hours_cross_venue, 15.0)

    def test_set_cross_venue_availability_defaults(self):
        """Test default max_hours is 10.0."""
        avail = set_cross_venue_availability(
            employee_id="emp1",
            employee_name="John Doe",
            home_venue_id="v1",
            available_venues=["v1"],
            roles=["bartender"],
            certs=[],
        )
        self.assertEqual(avail.max_hours_cross_venue, 10.0)

    def test_availability_to_dict(self):
        """Test to_dict serialization of availability."""
        avail = set_cross_venue_availability(
            employee_id="emp1",
            employee_name="John Doe",
            home_venue_id="v1",
            available_venues=["v1", "v2"],
            roles=["bartender"],
            certs=["responsible_service"],
        )
        d = avail.to_dict()
        self.assertEqual(d["employee_id"], "emp1")
        self.assertEqual(d["available_venues"], ["v1", "v2"])
        self.assertEqual(d["roles"], ["bartender"])


class TestGetAvailableForSharing(unittest.TestCase):
    """Test retrieval of staff available for sharing."""

    def setUp(self):
        """Reset store before each test."""
        _reset_for_tests()

    def test_get_available_for_sharing_empty(self):
        """Test getting available staff when none are registered."""
        avail = get_available_for_sharing("v1")
        self.assertEqual(len(avail), 0)

    def test_get_available_for_sharing_filters_by_venue(self):
        """Test that only staff available from the venue are returned."""
        set_cross_venue_availability(
            employee_id="emp1",
            employee_name="John Doe",
            home_venue_id="v1",
            available_venues=["v1", "v2"],
            roles=["bartender"],
            certs=[],
        )
        set_cross_venue_availability(
            employee_id="emp2",
            employee_name="Jane Smith",
            home_venue_id="v2",
            available_venues=["v2", "v3"],
            roles=["manager"],
            certs=[],
        )
        # Only emp1 is available from v1
        avail_v1 = get_available_for_sharing("v1")
        self.assertEqual(len(avail_v1), 1)
        self.assertEqual(avail_v1[0].employee_id, "emp1")

        # emp2 is available from v2 but not emp1
        avail_v2 = get_available_for_sharing("v2")
        self.assertEqual(len(avail_v2), 1)
        self.assertEqual(avail_v2[0].employee_id, "emp2")

    def test_get_available_for_sharing_excludes_home_only(self):
        """Test that staff with only home venue are excluded."""
        set_cross_venue_availability(
            employee_id="emp1",
            employee_name="John Doe",
            home_venue_id="v1",
            available_venues=["v1"],
            roles=["bartender"],
            certs=[],
        )
        avail = get_available_for_sharing("v1")
        # emp1 is registered but not available for sharing (only home venue)
        self.assertEqual(len(avail), 0)


class TestGetBorrowableStaff(unittest.TestCase):
    """Test retrieval of staff available to borrow."""

    def setUp(self):
        """Reset store before each test."""
        _reset_for_tests()

    def test_get_borrowable_staff_empty(self):
        """Test getting borrowable staff when none are available."""
        staff = get_borrowable_staff("v1")
        self.assertEqual(len(staff), 0)

    def test_get_borrowable_staff_by_venue(self):
        """Test filtering borrowable staff by target venue."""
        set_cross_venue_availability(
            employee_id="emp1",
            employee_name="John Doe",
            home_venue_id="v1",
            available_venues=["v1", "v2"],
            roles=["bartender"],
            certs=[],
        )
        set_cross_venue_availability(
            employee_id="emp2",
            employee_name="Jane Smith",
            home_venue_id="v1",
            available_venues=["v1"],
            roles=["bartender"],
            certs=[],
        )
        # emp1 can work at v2, emp2 cannot
        staff_v2 = get_borrowable_staff("v2")
        self.assertEqual(len(staff_v2), 1)
        self.assertEqual(staff_v2[0]["employee_id"], "emp1")

    def test_get_borrowable_staff_by_role(self):
        """Test filtering borrowable staff by role."""
        set_cross_venue_availability(
            employee_id="emp1",
            employee_name="John Doe",
            home_venue_id="v1",
            available_venues=["v1", "v2"],
            roles=["bartender"],
            certs=[],
        )
        set_cross_venue_availability(
            employee_id="emp2",
            employee_name="Jane Smith",
            home_venue_id="v1",
            available_venues=["v1", "v2"],
            roles=["manager"],
            certs=[],
        )
        # Only emp1 can work as bartender at v2
        staff = get_borrowable_staff("v2", role="bartender")
        self.assertEqual(len(staff), 1)
        self.assertEqual(staff[0]["employee_id"], "emp1")

        # Only emp2 can work as manager
        staff_mgr = get_borrowable_staff("v2", role="manager")
        self.assertEqual(len(staff_mgr), 1)
        self.assertEqual(staff_mgr[0]["employee_id"], "emp2")

    def test_borrowable_staff_includes_active_transfers_count(self):
        """Test that borrowable staff includes count of active transfers."""
        set_cross_venue_availability(
            employee_id="emp1",
            employee_name="John Doe",
            home_venue_id="v1",
            available_venues=["v1", "v2"],
            roles=["bartender"],
            certs=[],
        )
        staff = get_borrowable_staff("v2")
        self.assertEqual(staff[0]["active_transfers"], 0)

        # Create an active transfer
        req = request_transfer(
            employee_id="emp1", employee_name="John Doe",
            from_venue_id="v1", from_venue_name="Sydney",
            to_venue_id="v2", to_venue_name="Melbourne",
            requested_by="mgr1",
            start_date=date(2026, 5, 1), end_date=date(2026, 5, 7),
            reason="Cover",
        )
        activate_transfer(approve_transfer(req.request_id, "approver").request_id)

        staff = get_borrowable_staff("v2")
        self.assertEqual(staff[0]["active_transfers"], 1)


class TestSharingStats(unittest.TestCase):
    """Test sharing statistics calculation."""

    def setUp(self):
        """Reset store before each test."""
        _reset_for_tests()

    def test_sharing_stats_empty(self):
        """Test stats when no transfers exist."""
        stats = get_sharing_stats("v1", date(2026, 5, 1), date(2026, 5, 31))
        self.assertEqual(stats.venue_id, "v1")
        self.assertEqual(stats.staff_lent_out, 0)
        self.assertEqual(stats.staff_borrowed, 0)
        self.assertEqual(stats.total_transfer_hours, 0)

    def test_sharing_stats_lent_out(self):
        """Test stats correctly counts staff lent out."""
        request_transfer(
            employee_id="emp1", employee_name="John Doe",
            from_venue_id="v1", from_venue_name="Sydney",
            to_venue_id="v2", to_venue_name="Melbourne",
            requested_by="mgr1",
            start_date=date(2026, 5, 1), end_date=date(2026, 5, 7),
            reason="Cover",
        )
        stats = get_sharing_stats("v1", date(2026, 5, 1), date(2026, 5, 31))
        self.assertEqual(stats.staff_lent_out, 1)
        self.assertEqual(stats.staff_borrowed, 0)

    def test_sharing_stats_borrowed(self):
        """Test stats correctly counts staff borrowed."""
        request_transfer(
            employee_id="emp1", employee_name="John Doe",
            from_venue_id="v1", from_venue_name="Sydney",
            to_venue_id="v2", to_venue_name="Melbourne",
            requested_by="mgr1",
            start_date=date(2026, 5, 1), end_date=date(2026, 5, 7),
            reason="Cover",
        )
        stats = get_sharing_stats("v2", date(2026, 5, 1), date(2026, 5, 31))
        self.assertEqual(stats.staff_borrowed, 1)
        self.assertEqual(stats.staff_lent_out, 0)

    def test_sharing_stats_counts_active_transfers(self):
        """Test that stats counts active transfers correctly."""
        req = request_transfer(
            employee_id="emp1", employee_name="John Doe",
            from_venue_id="v1", from_venue_name="Sydney",
            to_venue_id="v2", to_venue_name="Melbourne",
            requested_by="mgr1",
            start_date=date(2026, 5, 1), end_date=date(2026, 5, 7),
            reason="Cover",
        )
        stats = get_sharing_stats("v1", date(2026, 5, 1), date(2026, 5, 31))
        self.assertEqual(stats.active_transfers, 0)
        self.assertEqual(stats.pending_requests, 1)

        activate_transfer(approve_transfer(req.request_id, "approver").request_id)
        stats = get_sharing_stats("v1", date(2026, 5, 1), date(2026, 5, 31))
        self.assertEqual(stats.active_transfers, 1)
        self.assertEqual(stats.pending_requests, 0)

    def test_sharing_stats_calculates_hours(self):
        """Test that stats calculates transfer hours."""
        req = request_transfer(
            employee_id="emp1", employee_name="John Doe",
            from_venue_id="v1", from_venue_name="Sydney",
            to_venue_id="v2", to_venue_name="Melbourne",
            requested_by="mgr1",
            start_date=date(2026, 5, 1), end_date=date(2026, 5, 3),
            reason="Cover",
        )
        approve_transfer(req.request_id, "approver")
        activate_transfer(req.request_id)

        stats = get_sharing_stats("v1", date(2026, 5, 1), date(2026, 5, 31))
        # 3 days = 3 * 8 = 24 hours
        self.assertEqual(stats.total_transfer_hours, 24.0)


class TestListTransferRequests(unittest.TestCase):
    """Test listing and filtering transfer requests."""

    def setUp(self):
        """Reset store before each test."""
        _reset_for_tests()

    def test_list_transfer_requests_empty(self):
        """Test listing when no requests exist."""
        reqs = list_transfer_requests()
        self.assertEqual(len(reqs), 0)

    def test_list_transfer_requests_all(self):
        """Test listing all requests."""
        request_transfer(
            employee_id="emp1", employee_name="John Doe",
            from_venue_id="v1", from_venue_name="Sydney",
            to_venue_id="v2", to_venue_name="Melbourne",
            requested_by="mgr1",
            start_date=date(2026, 5, 1), end_date=date(2026, 5, 7),
            reason="Cover",
        )
        request_transfer(
            employee_id="emp2", employee_name="Jane Smith",
            from_venue_id="v2", from_venue_name="Melbourne",
            to_venue_id="v3", to_venue_name="Brisbane",
            requested_by="mgr1",
            start_date=date(2026, 5, 1), end_date=date(2026, 5, 7),
            reason="Cover",
        )
        reqs = list_transfer_requests()
        self.assertEqual(len(reqs), 2)

    def test_list_transfer_requests_filter_by_venue(self):
        """Test filtering requests by venue."""
        request_transfer(
            employee_id="emp1", employee_name="John Doe",
            from_venue_id="v1", from_venue_name="Sydney",
            to_venue_id="v2", to_venue_name="Melbourne",
            requested_by="mgr1",
            start_date=date(2026, 5, 1), end_date=date(2026, 5, 7),
            reason="Cover",
        )
        request_transfer(
            employee_id="emp2", employee_name="Jane Smith",
            from_venue_id="v2", from_venue_name="Melbourne",
            to_venue_id="v3", to_venue_name="Brisbane",
            requested_by="mgr1",
            start_date=date(2026, 5, 1), end_date=date(2026, 5, 7),
            reason="Cover",
        )
        # v1 has one transfer (from_venue_id)
        reqs_v1 = list_transfer_requests(venue_id="v1")
        self.assertEqual(len(reqs_v1), 1)

        # v2 has two transfers (one as to_venue, one as from_venue)
        reqs_v2 = list_transfer_requests(venue_id="v2")
        self.assertEqual(len(reqs_v2), 2)

    def test_list_transfer_requests_filter_by_status(self):
        """Test filtering requests by status."""
        req1 = request_transfer(
            employee_id="emp1", employee_name="John Doe",
            from_venue_id="v1", from_venue_name="Sydney",
            to_venue_id="v2", to_venue_name="Melbourne",
            requested_by="mgr1",
            start_date=date(2026, 5, 1), end_date=date(2026, 5, 7),
            reason="Cover",
        )
        req2 = request_transfer(
            employee_id="emp2", employee_name="Jane Smith",
            from_venue_id="v2", from_venue_name="Melbourne",
            to_venue_id="v3", to_venue_name="Brisbane",
            requested_by="mgr1",
            start_date=date(2026, 5, 1), end_date=date(2026, 5, 7),
            reason="Cover",
        )
        approve_transfer(req1.request_id, "approver")

        requested = list_transfer_requests(status=TransferStatus.REQUESTED)
        self.assertEqual(len(requested), 1)

        approved = list_transfer_requests(status=TransferStatus.APPROVED)
        self.assertEqual(len(approved), 1)

    def test_list_transfer_requests_filter_by_employee(self):
        """Test filtering requests by employee."""
        request_transfer(
            employee_id="emp1", employee_name="John Doe",
            from_venue_id="v1", from_venue_name="Sydney",
            to_venue_id="v2", to_venue_name="Melbourne",
            requested_by="mgr1",
            start_date=date(2026, 5, 1), end_date=date(2026, 5, 7),
            reason="Cover",
        )
        request_transfer(
            employee_id="emp2", employee_name="Jane Smith",
            from_venue_id="v2", from_venue_name="Melbourne",
            to_venue_id="v3", to_venue_name="Brisbane",
            requested_by="mgr1",
            start_date=date(2026, 5, 1), end_date=date(2026, 5, 7),
            reason="Cover",
        )
        emp1_reqs = list_transfer_requests(employee_id="emp1")
        self.assertEqual(len(emp1_reqs), 1)
        self.assertEqual(emp1_reqs[0].employee_id, "emp1")


class TestGetTransferRequest(unittest.TestCase):
    """Test retrieving individual transfer requests."""

    def setUp(self):
        """Reset store before each test."""
        _reset_for_tests()

    def test_get_transfer_request_by_id(self):
        """Test retrieving a specific request by ID."""
        req1 = request_transfer(
            employee_id="emp1", employee_name="John Doe",
            from_venue_id="v1", from_venue_name="Sydney",
            to_venue_id="v2", to_venue_name="Melbourne",
            requested_by="mgr1",
            start_date=date(2026, 5, 1), end_date=date(2026, 5, 7),
            reason="Cover",
        )
        req2 = get_transfer_request(req1.request_id)
        self.assertIsNotNone(req2)
        self.assertEqual(req2.request_id, req1.request_id)
        self.assertEqual(req2.employee_id, "emp1")

    def test_get_nonexistent_request_returns_none(self):
        """Test that getting nonexistent request returns None."""
        req = get_transfer_request("nonexistent")
        self.assertIsNone(req)


class TestTransferRequestSerialization(unittest.TestCase):
    """Test serialization of transfer requests."""

    def setUp(self):
        """Reset store before each test."""
        _reset_for_tests()

    def test_transfer_request_to_dict(self):
        """Test TransferRequest.to_dict() serialization."""
        req = request_transfer(
            employee_id="emp1", employee_name="John Doe",
            from_venue_id="v1", from_venue_name="Sydney Bar",
            to_venue_id="v2", to_venue_name="Melbourne Lounge",
            requested_by="mgr1",
            start_date=date(2026, 5, 1), end_date=date(2026, 5, 7),
            reason="Cross-venue cover",
            notes="Peak season",
        )
        d = req.to_dict()
        self.assertEqual(d["employee_id"], "emp1")
        self.assertEqual(d["from_venue_id"], "v1")
        self.assertEqual(d["to_venue_id"], "v2")
        self.assertEqual(d["status"], "requested")
        self.assertIsNone(d["approved_by"])
        self.assertEqual(d["reason"], "Cross-venue cover")

    def test_sharing_stats_to_dict(self):
        """Test SharingStats.to_dict() serialization."""
        stats = get_sharing_stats("v1", date(2026, 5, 1), date(2026, 5, 31))
        d = stats.to_dict()
        self.assertEqual(d["venue_id"], "v1")
        self.assertEqual(d["period_start"], "2026-05-01")
        self.assertEqual(d["period_end"], "2026-05-31")
        self.assertEqual(d["staff_lent_out"], 0)
        self.assertEqual(d["total_transfer_hours"], 0.0)


class TestStoreThreadSafety(unittest.TestCase):
    """Test thread safety of the store."""

    def setUp(self):
        """Reset store before each test."""
        _reset_for_tests()

    def test_concurrent_requests_do_not_corrupt(self):
        """Test that concurrent requests maintain integrity."""
        import threading
        results = []

        def create_request(emp_id, v_from, v_to):
            req = request_transfer(
                employee_id=emp_id,
                employee_name=f"Employee {emp_id}",
                from_venue_id=v_from,
                from_venue_name=f"Venue {v_from}",
                to_venue_id=v_to,
                to_venue_name=f"Venue {v_to}",
                requested_by="mgr",
                start_date=date(2026, 5, 1),
                end_date=date(2026, 5, 7),
                reason="Cover",
            )
            results.append(req.request_id)

        threads = []
        for i in range(5):
            t = threading.Thread(target=create_request, args=(f"emp{i}", "v1", "v2"))
            threads.append(t)
            t.start()

        for t in threads:
            t.join()

        # All requests should be stored
        all_reqs = list_transfer_requests()
        self.assertEqual(len(all_reqs), 5)
        self.assertEqual(len(results), 5)
        self.assertEqual(len(set(results)), 5)  # All IDs unique


if __name__ == "__main__":
    unittest.main()
