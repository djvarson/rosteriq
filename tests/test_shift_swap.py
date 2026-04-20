"""Test suite for shift_swap.py module.

Tests the ShiftSwapStore with 10 comprehensive test cases covering:
- Offer creation
- Claiming
- Approval/rejection
- Cancellation
- Listing and filtering
- Persistence roundtrip
"""

import sys
import os
import unittest
import tempfile
from datetime import datetime, timezone

# Add parent to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from rosteriq.shift_swap import (
    get_swap_store,
    _reset_for_tests,
    ShiftSwap,
    SwapStatus,
)
from rosteriq import persistence as _p


class TestShiftSwap(unittest.TestCase):
    """Test suite for shift swap functionality."""

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
        # Clear the DB file between tests by deleting and recreating
        if os.path.exists(self.db_path):
            os.unlink(self.db_path)
        _p.init_db()

    def tearDown(self):
        """Clean up after each test."""
        _p.reset_for_tests()

    def test_offer_shift(self):
        """Test offering a shift with all fields."""
        store = get_swap_store()
        swap = store.offer(
            venue_id="venue_001",
            shift_id="shift_123",
            shift_date="2026-04-20",
            shift_start="09:00",
            shift_end="17:00",
            role="bartender",
            offered_by="emp_001",
            offered_by_name="Alice",
            reason="Got sick",
        )

        # Verify all fields
        self.assertEqual(swap.swap_id[:5], "swap_")
        self.assertEqual(swap.venue_id, "venue_001")
        self.assertEqual(swap.shift_id, "shift_123")
        self.assertEqual(swap.shift_date, "2026-04-20")
        self.assertEqual(swap.shift_start, "09:00")
        self.assertEqual(swap.shift_end, "17:00")
        self.assertEqual(swap.role, "bartender")
        self.assertEqual(swap.offered_by, "emp_001")
        self.assertEqual(swap.offered_by_name, "Alice")
        self.assertEqual(swap.reason, "Got sick")
        self.assertEqual(swap.status, SwapStatus.OFFERED)
        self.assertIsNone(swap.claimed_by)
        self.assertIsNotNone(swap.created_at)
        self.assertIsNotNone(swap.updated_at)

    def test_claim_shift(self):
        """Test claiming an offered shift."""
        store = get_swap_store()
        swap = store.offer(
            venue_id="venue_001",
            shift_id="shift_123",
            shift_date="2026-04-20",
            shift_start="09:00",
            shift_end="17:00",
            role="bartender",
            offered_by="emp_001",
            offered_by_name="Alice",
            reason="Got sick",
        )

        # Claim it
        claimed = store.claim(swap.swap_id, "emp_002", "Bob")
        self.assertEqual(claimed.status, SwapStatus.CLAIMED)
        self.assertEqual(claimed.claimed_by, "emp_002")
        self.assertEqual(claimed.claimed_by_name, "Bob")
        self.assertIsNotNone(claimed.claimed_at)

    def test_claim_non_offered_raises(self):
        """Test claiming a non-OFFERED swap raises ValueError."""
        store = get_swap_store()
        swap = store.offer(
            venue_id="venue_001",
            shift_id="shift_123",
            shift_date="2026-04-20",
            shift_start="09:00",
            shift_end="17:00",
            role="bartender",
            offered_by="emp_001",
            offered_by_name="Alice",
            reason="Got sick",
        )

        # Claim once
        store.claim(swap.swap_id, "emp_002", "Bob")

        # Try to claim again - should raise
        with self.assertRaises(ValueError) as cm:
            store.claim(swap.swap_id, "emp_003", "Charlie")
        self.assertIn("Cannot claim", str(cm.exception))

    def test_approve_swap(self):
        """Test approving a claimed swap."""
        store = get_swap_store()
        swap = store.offer(
            venue_id="venue_001",
            shift_id="shift_123",
            shift_date="2026-04-20",
            shift_start="09:00",
            shift_end="17:00",
            role="bartender",
            offered_by="emp_001",
            offered_by_name="Alice",
            reason="Got sick",
        )

        store.claim(swap.swap_id, "emp_002", "Bob")
        approved = store.approve(swap.swap_id, "mgr_001", note="Looks good")

        self.assertEqual(approved.status, SwapStatus.APPROVED)
        self.assertEqual(approved.reviewed_by, "mgr_001")
        self.assertEqual(approved.review_note, "Looks good")
        self.assertIsNotNone(approved.reviewed_at)

    def test_reject_swap(self):
        """Test rejecting a claimed swap."""
        store = get_swap_store()
        swap = store.offer(
            venue_id="venue_001",
            shift_id="shift_123",
            shift_date="2026-04-20",
            shift_start="09:00",
            shift_end="17:00",
            role="bartender",
            offered_by="emp_001",
            offered_by_name="Alice",
            reason="Got sick",
        )

        store.claim(swap.swap_id, "emp_002", "Bob")
        rejected = store.reject(swap.swap_id, "mgr_001", note="Need coverage")

        self.assertEqual(rejected.status, SwapStatus.REJECTED)
        self.assertEqual(rejected.reviewed_by, "mgr_001")
        self.assertEqual(rejected.review_note, "Need coverage")
        self.assertIsNotNone(rejected.reviewed_at)

    def test_cancel_by_offerer(self):
        """Test cancelling an offered swap by the offerer."""
        store = get_swap_store()
        swap = store.offer(
            venue_id="venue_001",
            shift_id="shift_123",
            shift_date="2026-04-20",
            shift_start="09:00",
            shift_end="17:00",
            role="bartender",
            offered_by="emp_001",
            offered_by_name="Alice",
            reason="Got sick",
        )

        cancelled = store.cancel(swap.swap_id, "emp_001")
        self.assertEqual(cancelled.status, SwapStatus.CANCELLED)

    def test_cancel_claimed_by_offerer(self):
        """Test cancelling a claimed swap by the offerer."""
        store = get_swap_store()
        swap = store.offer(
            venue_id="venue_001",
            shift_id="shift_123",
            shift_date="2026-04-20",
            shift_start="09:00",
            shift_end="17:00",
            role="bartender",
            offered_by="emp_001",
            offered_by_name="Alice",
            reason="Got sick",
        )

        store.claim(swap.swap_id, "emp_002", "Bob")
        cancelled = store.cancel(swap.swap_id, "emp_001")
        self.assertEqual(cancelled.status, SwapStatus.CANCELLED)

    def test_list_available(self):
        """Test listing available (OFFERED) swaps."""
        store = get_swap_store()

        # Create 3 swaps
        swap1 = store.offer(
            venue_id="venue_001",
            shift_id="shift_1",
            shift_date="2026-04-20",
            shift_start="09:00",
            shift_end="17:00",
            role="bartender",
            offered_by="emp_001",
            offered_by_name="Alice",
            reason="Reason 1",
        )

        swap2 = store.offer(
            venue_id="venue_001",
            shift_id="shift_2",
            shift_date="2026-04-21",
            shift_start="10:00",
            shift_end="18:00",
            role="floor",
            offered_by="emp_002",
            offered_by_name="Bob",
            reason="Reason 2",
        )

        swap3 = store.offer(
            venue_id="venue_001",
            shift_id="shift_3",
            shift_date="2026-04-22",
            shift_start="11:00",
            shift_end="19:00",
            role="kitchen",
            offered_by="emp_003",
            offered_by_name="Charlie",
            reason="Reason 3",
        )

        # Claim one
        store.claim(swap1.swap_id, "emp_099", "Claimer")

        # List available - should return swap2 and swap3
        available = store.list_available("venue_001")
        self.assertEqual(len(available), 2)
        swap_ids = {s.swap_id for s in available}
        self.assertIn(swap2.swap_id, swap_ids)
        self.assertIn(swap3.swap_id, swap_ids)
        self.assertNotIn(swap1.swap_id, swap_ids)

    def test_list_pending_review(self):
        """Test listing pending review (CLAIMED) swaps."""
        store = get_swap_store()

        # Create 3 swaps
        swap1 = store.offer(
            venue_id="venue_001",
            shift_id="shift_1",
            shift_date="2026-04-20",
            shift_start="09:00",
            shift_end="17:00",
            role="bartender",
            offered_by="emp_001",
            offered_by_name="Alice",
            reason="Reason 1",
        )

        swap2 = store.offer(
            venue_id="venue_001",
            shift_id="shift_2",
            shift_date="2026-04-21",
            shift_start="10:00",
            shift_end="18:00",
            role="floor",
            offered_by="emp_002",
            offered_by_name="Bob",
            reason="Reason 2",
        )

        swap3 = store.offer(
            venue_id="venue_001",
            shift_id="shift_3",
            shift_date="2026-04-22",
            shift_start="11:00",
            shift_end="19:00",
            role="kitchen",
            offered_by="emp_003",
            offered_by_name="Charlie",
            reason="Reason 3",
        )

        # Claim two
        store.claim(swap1.swap_id, "emp_099", "Claimer")
        store.claim(swap2.swap_id, "emp_099", "Claimer")

        # List pending - should return swap1 and swap2
        pending = store.list_pending_review("venue_001")
        self.assertEqual(len(pending), 2)
        swap_ids = {s.swap_id for s in pending}
        self.assertIn(swap1.swap_id, swap_ids)
        self.assertIn(swap2.swap_id, swap_ids)
        self.assertNotIn(swap3.swap_id, swap_ids)

    def test_persistence_roundtrip(self):
        """Test that swaps persist and rehydrate correctly."""
        # Create a swap in the first store instance
        store1 = get_swap_store()
        swap = store1.offer(
            venue_id="venue_001",
            shift_id="shift_123",
            shift_date="2026-04-20",
            shift_start="09:00",
            shift_end="17:00",
            role="bartender",
            offered_by="emp_001",
            offered_by_name="Alice",
            reason="Got sick",
        )
        swap_id = swap.swap_id

        # Claim it
        store1.claim(swap_id, "emp_002", "Bob")

        # Reset the singleton and persistence
        _reset_for_tests()
        _p.reset_for_tests()
        _p.reset_rehydrate_for_tests()

        # Rehydrate
        _p.force_enable_for_tests(True)
        _p.init_db()

        # Get a new store instance (which rehydrates from DB)
        store2 = get_swap_store()

        # Verify the swap was restored
        restored = store2.get(swap_id)
        self.assertIsNotNone(restored)
        self.assertEqual(restored.swap_id, swap_id)
        self.assertEqual(restored.venue_id, "venue_001")
        self.assertEqual(restored.offered_by, "emp_001")
        self.assertEqual(restored.claimed_by, "emp_002")
        self.assertEqual(restored.status, SwapStatus.CLAIMED)


if __name__ == "__main__":
    unittest.main()
