"""Tests for rosteriq.tip_pool — stdlib unittest only."""
from __future__ import annotations
import sys, unittest
from pathlib import Path
from datetime import date, timedelta

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from rosteriq.tip_pool import (
    TipEntry, TipPool, TipAllocation, TipSummary,
    DistributionMethod, DEFAULT_POINT_WEIGHTS,
    add_tip_entry, create_pool, distribute_pool, undo_distribution,
    get_employee_tips, build_tip_summary, get_tip_pool_store, _reset_for_tests,
)


class TestTipEntry(unittest.TestCase):
    def setUp(self):
        _reset_for_tests()

    def test_add_entry(self):
        e = add_tip_entry("v1", date(2026, 4, 20), 50.0, "cash", "emp1")
        self.assertEqual(e.venue_id, "v1")
        self.assertEqual(e.amount, 50.0)
        self.assertEqual(e.source, "cash")

    def test_entry_to_dict(self):
        e = add_tip_entry("v1", date(2026, 4, 20), 25.0, "card", "emp1")
        d = e.to_dict()
        self.assertEqual(d["amount"], 25.0)
        self.assertIn("entry_id", d)

    def test_multiple_entries(self):
        add_tip_entry("v1", date(2026, 4, 20), 50.0, "cash", "emp1")
        add_tip_entry("v1", date(2026, 4, 20), 30.0, "card", "emp1")
        entries = get_tip_pool_store().list_entries("v1")
        self.assertEqual(len(entries), 2)


class TestCreatePool(unittest.TestCase):
    def setUp(self):
        _reset_for_tests()

    def test_create_pool_sums_entries(self):
        e1 = add_tip_entry("v1", date(2026, 4, 20), 50.0, "cash", "emp1")
        e2 = add_tip_entry("v1", date(2026, 4, 20), 30.0, "card", "emp1")
        pool = create_pool("v1", date(2026, 4, 20), [e1, e2])
        self.assertEqual(pool.total_amount, 80.0)
        self.assertFalse(pool.is_distributed)
        self.assertEqual(len(pool.entries), 2)

    def test_pool_to_dict(self):
        e1 = add_tip_entry("v1", date(2026, 4, 20), 100.0, "cash", "emp1")
        pool = create_pool("v1", date(2026, 4, 20), [e1])
        d = pool.to_dict()
        self.assertEqual(d["total_amount"], 100.0)
        self.assertFalse(d["is_distributed"])


class TestDistributeHoursBased(unittest.TestCase):
    def setUp(self):
        _reset_for_tests()
        e = add_tip_entry("v1", date(2026, 4, 20), 100.0, "cash", "emp1")
        self.pool = create_pool("v1", date(2026, 4, 20), [e])
        self.staff = [
            {"employee_id": "e1", "employee_name": "Alice", "hours_worked": 8.0, "role": "bar"},
            {"employee_id": "e2", "employee_name": "Bob", "hours_worked": 4.0, "role": "floor"},
        ]

    def test_hours_based_proportional(self):
        allocs = distribute_pool(self.pool.pool_id, self.staff)
        alice = next(a for a in allocs if a.employee_id == "e1")
        bob = next(a for a in allocs if a.employee_id == "e2")
        self.assertAlmostEqual(alice.share_amount, 66.67, places=2)
        self.assertAlmostEqual(bob.share_amount, 33.33, places=2)

    def test_shares_sum_to_total(self):
        allocs = distribute_pool(self.pool.pool_id, self.staff)
        total = sum(a.share_amount for a in allocs)
        self.assertAlmostEqual(total, 100.0, places=0)

    def test_pool_marked_distributed(self):
        distribute_pool(self.pool.pool_id, self.staff)
        pool = get_tip_pool_store().get_pool(self.pool.pool_id)
        self.assertTrue(pool.is_distributed)
        self.assertIsNotNone(pool.distributed_at)


class TestDistributeEqualSplit(unittest.TestCase):
    def setUp(self):
        _reset_for_tests()
        e = add_tip_entry("v1", date(2026, 4, 20), 90.0, "cash", "emp1")
        self.pool = create_pool("v1", date(2026, 4, 20), [e])
        self.staff = [
            {"employee_id": "e1", "employee_name": "Alice", "hours_worked": 8.0, "role": "bar"},
            {"employee_id": "e2", "employee_name": "Bob", "hours_worked": 4.0, "role": "floor"},
            {"employee_id": "e3", "employee_name": "Charlie", "hours_worked": 6.0, "role": "kitchen"},
        ]

    def test_equal_split(self):
        allocs = distribute_pool(self.pool.pool_id, self.staff,
                                  method=DistributionMethod.EQUAL_SPLIT)
        for a in allocs:
            self.assertAlmostEqual(a.share_amount, 30.0, places=2)

    def test_equal_pct(self):
        allocs = distribute_pool(self.pool.pool_id, self.staff,
                                  method=DistributionMethod.EQUAL_SPLIT)
        for a in allocs:
            self.assertAlmostEqual(a.share_pct, 33.33, places=2)


class TestDistributePointsBased(unittest.TestCase):
    def setUp(self):
        _reset_for_tests()
        e = add_tip_entry("v1", date(2026, 4, 20), 100.0, "cash", "emp1")
        self.pool = create_pool("v1", date(2026, 4, 20), [e])

    def test_points_based_weights(self):
        staff = [
            {"employee_id": "e1", "employee_name": "Alice", "hours_worked": 8.0, "role": "manager"},
            {"employee_id": "e2", "employee_name": "Bob", "hours_worked": 8.0, "role": "kitchen"},
        ]
        # manager: 8*1.5=12 points, kitchen: 8*0.8=6.4 points, total=18.4
        allocs = distribute_pool(self.pool.pool_id, staff,
                                  method=DistributionMethod.POINTS_BASED)
        mgr = next(a for a in allocs if a.employee_id == "e1")
        kit = next(a for a in allocs if a.employee_id == "e2")
        self.assertGreater(mgr.share_amount, kit.share_amount)
        self.assertAlmostEqual(mgr.points, 12.0, places=1)
        self.assertAlmostEqual(kit.points, 6.4, places=1)

    def test_points_custom_weights(self):
        staff = [
            {"employee_id": "e1", "employee_name": "Alice", "hours_worked": 5.0, "role": "bar"},
            {"employee_id": "e2", "employee_name": "Bob", "hours_worked": 5.0, "role": "floor"},
        ]
        custom = {"bar": 2.0, "floor": 1.0}
        allocs = distribute_pool(self.pool.pool_id, staff,
                                  method=DistributionMethod.POINTS_BASED,
                                  point_weights=custom)
        bar = next(a for a in allocs if a.employee_id == "e1")
        floor = next(a for a in allocs if a.employee_id == "e2")
        self.assertAlmostEqual(bar.share_amount, 66.67, places=2)
        self.assertAlmostEqual(floor.share_amount, 33.33, places=2)


class TestUndoDistribution(unittest.TestCase):
    def setUp(self):
        _reset_for_tests()
        e = add_tip_entry("v1", date(2026, 4, 20), 100.0, "cash", "emp1")
        self.pool = create_pool("v1", date(2026, 4, 20), [e])
        self.staff = [
            {"employee_id": "e1", "employee_name": "Alice", "hours_worked": 5.0, "role": "bar"},
        ]

    def test_undo_marks_undistributed(self):
        distribute_pool(self.pool.pool_id, self.staff)
        pool = undo_distribution(self.pool.pool_id)
        self.assertFalse(pool.is_distributed)
        self.assertIsNone(pool.distributed_at)

    def test_undo_clears_allocations(self):
        distribute_pool(self.pool.pool_id, self.staff)
        undo_distribution(self.pool.pool_id)
        allocs = get_employee_tips("e1", "v1")
        self.assertEqual(len(allocs), 0)

    def test_undo_not_distributed_raises(self):
        with self.assertRaises(ValueError):
            undo_distribution(self.pool.pool_id)


class TestEmployeeTips(unittest.TestCase):
    def setUp(self):
        _reset_for_tests()

    def test_get_employee_tips(self):
        e = add_tip_entry("v1", date(2026, 4, 20), 100.0, "cash", "emp1")
        pool = create_pool("v1", date(2026, 4, 20), [e])
        staff = [{"employee_id": "e1", "employee_name": "Alice",
                  "hours_worked": 5.0, "role": "bar"}]
        distribute_pool(pool.pool_id, staff)
        tips = get_employee_tips("e1", "v1")
        self.assertEqual(len(tips), 1)
        self.assertAlmostEqual(tips[0].share_amount, 100.0, places=2)


class TestTipSummary(unittest.TestCase):
    def setUp(self):
        _reset_for_tests()

    def test_summary_totals(self):
        e1 = add_tip_entry("v1", date(2026, 4, 20), 50.0, "cash", "emp1")
        e2 = add_tip_entry("v1", date(2026, 4, 20), 30.0, "card", "emp1")
        pool = create_pool("v1", date(2026, 4, 20), [e1, e2])
        staff = [{"employee_id": "e1", "employee_name": "Alice",
                  "hours_worked": 5.0, "role": "bar"}]
        distribute_pool(pool.pool_id, staff)
        summary = build_tip_summary("v1", date(2026, 4, 1), date(2026, 4, 30))
        self.assertEqual(summary.total_tips, 80.0)
        self.assertEqual(summary.total_distributed, 80.0)
        self.assertEqual(summary.pools_count, 1)
        self.assertEqual(summary.by_source["cash"], 50.0)
        self.assertEqual(summary.by_source["card"], 30.0)

    def test_summary_to_dict(self):
        summary = build_tip_summary("v1", date(2026, 4, 1), date(2026, 4, 30))
        d = summary.to_dict()
        self.assertIn("total_tips", d)
        self.assertIn("by_source", d)

    def test_summary_empty(self):
        summary = build_tip_summary("v1", date(2026, 4, 1), date(2026, 4, 30))
        self.assertEqual(summary.total_tips, 0)
        self.assertEqual(summary.pools_count, 0)


class TestEdgeCases(unittest.TestCase):
    def setUp(self):
        _reset_for_tests()

    def test_distribute_empty_staff_raises(self):
        e = add_tip_entry("v1", date(2026, 4, 20), 100.0, "cash", "emp1")
        pool = create_pool("v1", date(2026, 4, 20), [e])
        with self.assertRaises(ValueError):
            distribute_pool(pool.pool_id, [])

    def test_double_distribute_raises(self):
        e = add_tip_entry("v1", date(2026, 4, 20), 100.0, "cash", "emp1")
        pool = create_pool("v1", date(2026, 4, 20), [e])
        staff = [{"employee_id": "e1", "employee_name": "Alice",
                  "hours_worked": 5.0, "role": "bar"}]
        distribute_pool(pool.pool_id, staff)
        with self.assertRaises(ValueError):
            distribute_pool(pool.pool_id, staff)

    def test_nonexistent_pool_raises(self):
        with self.assertRaises(ValueError):
            distribute_pool("fake_id", [{"employee_id": "e1", "employee_name": "A",
                                         "hours_worked": 1, "role": "bar"}])


if __name__ == "__main__":
    unittest.main()
